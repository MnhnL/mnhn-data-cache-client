"""Unit tests for taxon lookups and synonym resolution — no Elasticsearch needed."""
from mnhn_data_cache_client import DataCacheClient, Taxon, TaxonFilter

TAXON_INDEX = {
    "ACC": {"taxon_key": "ACC", "taxon_item_name": "Vulpes vulpes",
            "taxon_authority": "(Linnaeus, 1758)", "taxon_rank": "species",
            "taxon_preferred": "ACC"},
    "SYN": {"taxon_key": "SYN", "taxon_item_name": "Canis vulpes",
            "taxon_authority": "Linnaeus, 1758", "taxon_rank": "species",
            "taxon_preferred": "ACC"},
    "NOPREF": {"taxon_key": "NOPREF", "taxon_item_name": "Meles meles"},
}


class _StubEs:
    def __init__(self):
        self.searches = []

    def search(self, *, index, query, size, **kwargs):
        self.searches.append(query)
        keys = query["terms"]["taxon_key.keyword"]
        hits = [{"_source": TAXON_INDEX[k]} for k in keys if k in TAXON_INDEX]
        return {"hits": {"hits": hits[:size]}}


def _client():
    # Skip __init__, which connects to Elasticsearch.
    client = DataCacheClient.__new__(DataCacheClient)
    client._es = _StubEs()
    client.taxon_indices = ["taxa"]
    return client


def test_get_taxa_returns_known_keys_only():
    taxa = _client().get_taxa(["ACC", "SYN", "MISSING"])
    assert set(taxa) == {"ACC", "SYN"}
    assert taxa["SYN"] == Taxon(
        id="SYN", name="Canis vulpes", authority="Linnaeus, 1758",
        rank="species", accepted_id="ACC",
    )
    assert taxa["SYN"].is_synonym
    assert not taxa["ACC"].is_synonym


def test_get_taxa_without_ids_does_not_query():
    client = _client()
    assert client.get_taxa([]) == {}
    assert client._es.searches == []


def test_taxon_without_preferred_key_is_its_own_accepted_name():
    taxon = _client().get_taxon("NOPREF")
    assert taxon.accepted_id == "NOPREF"
    assert taxon.authority == "" and taxon.rank == ""


def test_get_taxon_unknown_is_none():
    assert _client().get_taxon("MISSING") is None


def test_resolve_accepted_maps_synonyms_in_one_lookup():
    client = _client()
    filters = [
        TaxonFilter("SYN", include_subtaxa=True),
        TaxonFilter("ACC", exclude=True),
        TaxonFilter("MISSING"),
    ]
    resolved = client._resolve_accepted(filters)

    assert [tf.taxon_id for tf in resolved] == ["ACC", "ACC", "MISSING"]
    assert [(tf.include_subtaxa, tf.exclude) for tf in resolved] == [
        (True, False), (False, True), (False, False),
    ]
    assert len(client._es.searches) == 1


def test_resolve_accepted_leaves_callers_filters_alone():
    tf = TaxonFilter("SYN")
    _client()._resolve_accepted(tf)
    assert tf.taxon_id == "SYN"
