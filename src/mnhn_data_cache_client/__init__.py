from elasticsearch6 import Elasticsearch


class DataCacheClient():

    def __init__(self,
                 es_hosts=["serv-gis.vm.mnhn.etat.lu"],
                 chunk_size=35000,
                 obs_indices=["data_cache_obs_recorder"],
                 taxon_indices=["data_cache_taxon"],
                 **kwargs):
        if type(es_hosts) not in (list, tuple):
            es_hosts = [es_hosts]
        self._es = Elasticsearch(es_hosts, **kwargs)
        self._chunk_size = chunk_size
        self.obs_indices = obs_indices
        self.taxon_indices = taxon_indices

        self._observation_mapping = self._es.indices.get_mapping(obs_indices[0])
        self._taxon_mapping = self._es.indices.get_mapping(taxon_indices[0])

    def observation_mapping(self):
        if 'mnhn' in self._observation_mapping:
            ks = self._observation_mapping['mnhn']['mappings']['observations']['properties']
        elif 'mnhn2' in self._observation_mapping:
            ks = self._observation_mapping['mnhn2']['mappings']['observations']['properties']
        return ks

    def taxon_mapping(self):
        ks = self._taxon_mapping[self.taxon_indices]['mappings']['taxa']['properties']
        return ks

    def _make_observations_query(self,
                                 taxon_name=None,
                                 date_range=None,
                                 recorder_names=None,
                                 polygon=None):
        query = {
            "bool": {
                "filter": [],
                "must": [],
            }
        }

        if taxon_name:
            query["bool"]["must"].append({"match_phrase": {"Taxon_Name": taxon_name}})

        if date_range:
            _from, _to = date_range
            if _from and _to:
                query["bool"]["filter"].append({"range": {"date_start": {"gt": _from, "lt": _to}}})
            elif not _to:
                query["bool"]["filter"].append({"range": {"date_start": {"gt": _from}}})
            elif not _from:
                query["bool"]["filter"].append({"range": {"date_start": {"lt": _to}}})
            else:
                raise ValueError("date_range must have at least one bound")

        if recorder_names:
            query["bool"]["must"].append({"match": {"recorder_names": recorder_names}})

        if polygon:
            query["bool"]["filter"].append({
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

        return query

    def _make_observations_body(self, query):
        return {
            "query": query,
            "sort": [
                {"date_start": {"order": "desc"}},
            ],
        }

    def search_observations(self, **kwargs):
        query = self._make_observations_query(**kwargs)
        count = self._es.count(index=self.obs_indices,
                               body={"query": query})["count"]
        body = self._make_observations_body(query)
        for i in range(0, count, self._chunk_size):
            body["size"] = self._chunk_size
            body["from"] = i
            yield self._es.search(index=self.obs_indices,
                                  size=self._chunk_size,
                                  body=body)

    def _make_taxa_query(self, taxon_name=None, rank=None):
        query = {
            "bool": {
                "filter": [],
                "must": [],
            }
        }

        if taxon_name:
            query["bool"]["must"].append({"prefix": {"taxon_item_name": taxon_name}})

        if rank:
            query["bool"]["must"].append({"term": {"taxon_rank": rank}})

        return query

    def search_taxa(self, **kwargs):
        query = self._make_taxa_query(**kwargs)
        search_body = {"query": query}
        count = self._es.count(index=self.taxon_indices,
                               body=search_body)["count"]
        body = {"query": query}
        for i in range(0, count, self._chunk_size):
            body["size"] = self._chunk_size
            body["from"] = i
            yield self._es.search(index=self.taxon_indices,
                                  size=self._chunk_size,
                                  body=body)

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
