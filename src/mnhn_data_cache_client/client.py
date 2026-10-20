import copy
from dataclasses import dataclass

from elasticsearch import Elasticsearch

from .geo import buffer_geojson


class TaxonFilter:
    """A single taxon criterion built from a previously resolved taxon ID.

    Instances are typically constructed from hits returned by
    :meth:`DataCacheClient.find_taxon` or :meth:`DataCacheClient.search_taxa`
    rather than from raw names, keeping the taxon-index lookup and the
    observation query as two explicit steps.

    Args:
        taxon_id: Key of any name in the taxon index, synonyms included:
            :meth:`DataCacheClient.search_observations` matches observations
            of its accepted name.
        taxon_rank: Optional rank string (e.g. ``"genus"``, ``"species"``).
            Stored as metadata only; not used in the query.
        include_subtaxa: When ``True`` the observation filter matches any
            observation whose ``taxon_branch_key`` array contains *taxon_id*,
            covering all subtaxa at any rank.  When ``False`` (default) only
            observations whose ``taxon_preferred.keyword`` equals *taxon_id*
            are returned.
        exclude: When ``True`` matching observations are *excluded* from the
            result set rather than included.
    """

    def __init__(self, taxon_id, taxon_rank=None, include_subtaxa=False, exclude=False):
        self.taxon_id = taxon_id
        self.taxon_rank = taxon_rank
        self.include_subtaxa = include_subtaxa
        self.exclude = exclude

    @classmethod
    def from_taxon_source(cls, source, include_subtaxa=False, exclude=False):
        """Construct a :class:`TaxonFilter` from a taxon index ``_source`` dict.

        Reads ``taxon_key`` and ``taxon_rank`` from *source*, keeping field-name
        knowledge in one place rather than scattered across every call site.

        Args:
            source: The ``_source`` dict from a taxon index hit.
            include_subtaxa: Forwarded to :class:`TaxonFilter`.
            exclude: Forwarded to :class:`TaxonFilter`.
        """
        return cls(
            source["taxon_key"],
            taxon_rank=source.get("taxon_rank"),
            include_subtaxa=include_subtaxa,
            exclude=exclude,
        )

    def __repr__(self):
        return (f"TaxonFilter({self.taxon_id!r}"
                + (f", taxon_rank={self.taxon_rank!r}" if self.taxon_rank else "")
                + (", include_subtaxa=True" if self.include_subtaxa else "")
                + (", exclude=True" if self.exclude else "")
                + ")")


@dataclass(frozen=True)
class Taxon:
    """A name from the taxon index.

    Attributes:
        id: The name's own key.
        name: Scientific name.
        authority: Authorship string.
        rank: Rank label, e.g. ``"species"``.
        accepted_id: Key of the currently accepted name; equal to *id* unless
            this name is a synonym.
    """

    id: str
    name: str
    authority: str
    rank: str
    accepted_id: str

    @property
    def is_synonym(self):
        return self.accepted_id != self.id

    @classmethod
    def _from_source(cls, source):
        return cls(
            id=source["taxon_key"],
            name=source.get("taxon_item_name") or "",
            authority=source.get("taxon_authority") or "",
            rank=source.get("taxon_rank") or "",
            accepted_id=source.get("taxon_preferred") or source["taxon_key"],
        )


class DataCacheClient():

    def __init__(self,
                 es_hosts=["serv-gis.vm.mnhn.etat.lu"],
                 chunk_size=35000,
                 obs_indices=["data_cache_obs_recorder",
                              "data_cache_obs_ornitho",
                              "data_cache_obs_gbif",
                              "data_cache_obs_inaturalist",
                              "data_cache_obs_observation_org",
                              ],
                 taxon_indices=["data_cache_taxon"],
                 **kwargs):
        """
        Args:
            es_hosts: Elasticsearch host(s).
            chunk_size: Number of records fetched per ES request.
            obs_indices: Observation index name(s).
            taxon_indices: Taxon index name(s).
            **kwargs: Passed through to :class:`elasticsearch.Elasticsearch`.
        """
        if type(es_hosts) not in (list, tuple):
            es_hosts = [es_hosts]
        self._es = Elasticsearch(es_hosts, **kwargs)
        self._chunk_size = chunk_size
        self.obs_indices = obs_indices
        self.taxon_indices = taxon_indices

        self._observation_mapping = self._es.indices.get_mapping(index=obs_indices[0])
        self._taxon_mapping = self._es.indices.get_mapping(index=taxon_indices[0])

    # ------------------------------------------------------------------
    # Taxon index — single-item lookup
    # ------------------------------------------------------------------

    def find_taxon(self, name, rank=None):
        """Return the ``_source`` dict for the first taxon matching *name* exactly.

        Unlike :meth:`search_taxa`, which uses prefix matching and returns a
        page generator, this method uses an exact phrase match and is intended
        for the common case of resolving one taxon name to its ID before
        building a :class:`TaxonFilter`.

        Args:
            name: Exact taxon name to look up (matched against
                ``taxon_item_name``).
            rank: Optional rank string (matched against ``taxon_rank``).
                Useful when the same name exists at multiple ranks.

        Returns:
            The ``_source`` dict of the first matching document.

        Raises:
            ValueError: When no matching taxon is found.
        """
        query = {"bool": {"must": [{"match_phrase": {"taxon_item_name": name}}]}}
        if rank:
            query["bool"]["must"].append({"term": {"taxon_rank": rank}})

        result = self._es.search(index=self.taxon_indices, query=query, size=1)
        hits = result["hits"]["hits"]
        if not hits:
            rank_msg = f" with rank {rank!r}" if rank else ""
            raise ValueError(f"Taxon not found: {name!r}{rank_msg}")
        return hits[0]["_source"]

    def get_taxa(self, taxon_ids):
        """Return ``{taxon_id: Taxon}`` for *taxon_ids*; unknown keys are left out."""
        ids = list(dict.fromkeys(taxon_ids))
        if not ids:
            return {}
        result = self._es.search(
            index=self.taxon_indices,
            query={"terms": {"taxon_key.keyword": ids}},
            size=len(ids),
        )
        taxa = (Taxon._from_source(hit["_source"]) for hit in result["hits"]["hits"])
        return {taxon.id: taxon for taxon in taxa}

    def get_taxon(self, taxon_id):
        """Return the :class:`Taxon` for *taxon_id*, or ``None`` if unknown."""
        return self.get_taxa([taxon_id]).get(taxon_id)

    def suggest_taxa(self, text, limit=20):
        """Return up to *limit* :class:`Taxon` suggestions for a search box.

        Matches names starting with *text* (case-insensitive) or authorships
        matching it. Exact name matches come first, then shorter names.
        """
        result = self._es.search(
            index=self.taxon_indices,
            query={
                "bool": {
                    "should": [
                        # taxon_item_name is a keyword field; case_insensitive
                        # prefix scans the term dictionary.
                        {"prefix": {"taxon_item_name": {"value": text, "case_insensitive": True}}},
                        {"match": {"taxon_authority": {"query": text}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            sort=[
                {
                    "_script": {
                        "type": "number",
                        "order": "asc",
                        "script": {
                            "lang": "painless",
                            "source": "doc['taxon_item_name'].value.toLowerCase() == params.q ? 0 : 1",
                            "params": {"q": text.lower()},
                        },
                    }
                },
                {
                    "_script": {
                        "type": "number",
                        "order": "asc",
                        "script": {
                            "lang": "painless",
                            "source": "doc['taxon_item_name'].value.length()",
                        },
                    }
                },
            ],
            size=limit,
        )
        return [Taxon._from_source(hit["_source"]) for hit in result["hits"]["hits"]]

    # ------------------------------------------------------------------
    # Observation query helpers
    # ------------------------------------------------------------------

    def _resolve_accepted(self, taxon_filters):
        """Return copies of *taxon_filters* pointing at their accepted names.

        Observations are tagged with accepted keys only (taxon_preferred,
        taxon_branch_key), so a filter on a synonym's own key would silently
        match nothing. Keys missing from the taxon index are kept as they are.
        """
        if isinstance(taxon_filters, TaxonFilter):
            taxon_filters = [taxon_filters]
        taxa = self.get_taxa(tf.taxon_id for tf in taxon_filters)
        resolved = []
        for tf in taxon_filters:
            tf = copy.copy(tf)
            if tf.taxon_id in taxa:
                tf.taxon_id = taxa[tf.taxon_id].accepted_id
            resolved.append(tf)
        return resolved

    def _make_taxon_obs_clause(self, taxon_filter):
        """Return the ES query clause for one :class:`TaxonFilter`.

        * ``include_subtaxa=False``: ``term`` on ``taxon_preferred.keyword``.
        * ``include_subtaxa=True``: ``term`` on ``taxon_branch_key`` (an array
          of all ancestor keys); matches observations of any subtaxon at any rank.
        """
        if taxon_filter.include_subtaxa:
            return {"term": {"taxon_branch_key": taxon_filter.taxon_id}}
        return {"term": {"taxon_preferred.keyword": taxon_filter.taxon_id}}

    def _make_observations_query(self,
                                 taxon_filters=None,
                                 date_range=None,
                                 recorder_names=None,
                                 polygon=None,
                                 areas=None,
                                 within_m=None):
        """Build an Elasticsearch bool query for observations.

        Args:
            taxon_filters: A :class:`TaxonFilter` or list of
                :class:`TaxonFilter`.  Multiple *include* filters are combined
                with OR; *exclude* filters generate ``must_not`` clauses.
            date_range: ``(from, to)`` date strings, both inclusive; either
                bound may be ``None`` for an open interval.
            recorder_names: Recorder name filter (full-text match).
            polygon: List of ``[lon, lat]`` coordinate pairs forming a closed
                ring.
            areas: List of GeoJSON geometry dicts (WGS84). Observations
                intersecting any of them match.
            within_m: Optional distance in metres: with *areas*, observations
                up to this far outside an area match too.
        """
        must     = []
        must_not = []
        filter_  = []

        if taxon_filters is not None:
            if isinstance(taxon_filters, TaxonFilter):
                taxon_filters = [taxon_filters]

            include_clauses = []
            exclude_clauses = []
            for tf in taxon_filters:
                clause = self._make_taxon_obs_clause(tf)
                if tf.exclude:
                    exclude_clauses.append(clause)
                else:
                    include_clauses.append(clause)

            if len(include_clauses) == 1:
                must.append(include_clauses[0])
            elif len(include_clauses) > 1:
                must.append({
                    "bool": {
                        "should": include_clauses,
                        "minimum_should_match": 1,
                    }
                })

            must_not.extend(exclude_clauses)

        if date_range:
            _from, _to = date_range
            if _from and _to:
                filter_.append({"range": {"date_start": {"gte": _from, "lte": _to}}})
            elif not _to:
                filter_.append({"range": {"date_start": {"gte": _from}}})
            elif not _from:
                filter_.append({"range": {"date_start": {"lte": _to}}})
            else:
                raise ValueError("date_range must have at least one bound")

        if recorder_names:
            must.append({"match": {"recorder_names": recorder_names}})

        if polygon:
            filter_.append({
                "geo_shape": {
                    "location": {
                        "shape": {
                            "type": "polygon",
                            "coordinates": [polygon],
                        },
                        "relation": "within",
                    }
                }
            })

        if areas:
            if within_m:
                areas = [buffer_geojson(a, within_m) for a in areas]
            filter_.append({
                "bool": {
                    "should": [
                        {"geo_shape": {"location": {"shape": a, "relation": "intersects"}}}
                        for a in areas
                    ],
                    "minimum_should_match": 1,
                }
            })

        bool_q = {}
        if must:
            bool_q["must"]     = must
        if must_not:
            bool_q["must_not"] = must_not
        if filter_:
            bool_q["filter"]   = filter_

        return {"bool": bool_q} if bool_q else {"match_all": {}}

    def search_observations(self, taxon_filters=None, **kwargs):
        """Yield pages of observation hits matching the given filters.

        Takes the arguments of :meth:`_make_observations_query`. Taxon filters
        are resolved to their accepted names first, in one taxon index lookup;
        doing it on every search follows the taxonomy if the accepted name of
        a taxon changes. The caller's filters are not modified.
        """
        if taxon_filters is not None:
            taxon_filters = self._resolve_accepted(taxon_filters)
        query = self._make_observations_query(taxon_filters=taxon_filters, **kwargs)
        count = self._es.count(index=self.obs_indices, query=query)["count"]
        sort  = [{"date_start": {"order": "desc"}}]
        for i in range(0, count, self._chunk_size):
            yield self._es.search(
                index=self.obs_indices,
                query=query,
                sort=sort,
                size=self._chunk_size,
                from_=i,
            )

    # ------------------------------------------------------------------
    # Taxon index queries
    # ------------------------------------------------------------------

    def _make_taxa_query(self, taxon_name=None, rank=None):
        must = []

        if taxon_name:
            must.append({"prefix": {"taxon_item_name": taxon_name}})
        if rank:
            must.append({"term": {"taxon_rank": rank}})

        return {"bool": {"must": must}} if must else {"match_all": {}}

    def search_taxa(self, **kwargs):
        query = self._make_taxa_query(**kwargs)
        count = self._es.count(index=self.taxon_indices, query=query)["count"]
        for i in range(0, count, self._chunk_size):
            yield self._es.search(
                index=self.taxon_indices,
                query=query,
                size=self._chunk_size,
                from_=i,
            )

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def print_results(self, query, cols):
        count = 0
        for c in cols:
            print(repr(c), end="\t")
        print()
        for obs in query:
            hits = obs["hits"]["hits"]
            count += len(hits)
            for o in hits:
                src = o["_source"]
                for col in cols:
                    print(repr(src.get(col, "n/a")), end="\t")
                print()
        print(f"Found {count} items")
