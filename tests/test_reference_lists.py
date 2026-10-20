"""Unit tests for reference lists — no Elasticsearch needed."""
from mnhn_data_cache_client import DataCacheClient, ReferenceList


class _StubEs:
    """Answers the terms aggregation and pages the composite one over *rows*
    of (accepted, item, taxon, count)."""

    def __init__(self, rows=()):
        self.rows = sorted(rows)
        self.requests = []

    def search(self, *, index, size, aggs, query=None):
        self.requests.append({"query": query, "aggs": aggs})
        if "lists" in aggs:
            return {"aggregations": {"lists": {"buckets": [
                {"key": "RED LIST", "doc_count": 30}, {"key": "CHECKLIST", "doc_count": 10},
            ]}}}
        composite = aggs["taxa"]["composite"]
        after = composite.get("after")
        after = after and (after["accepted"], after["item"], after["taxon"])
        page = [r for r in self.rows if after is None or r[:3] > after][:composite["size"]]
        agg = {"buckets": [
            {"key": {"accepted": a, "item": i, "taxon": t}, "doc_count": n} for a, i, t, n in page
        ]}
        if page:
            a, i, t, _ = page[-1]
            agg["after_key"] = {"accepted": a, "item": i, "taxon": t}
        return {"aggregations": {"taxa": agg}}


def _client(es):
    # Skip __init__, which connects to Elasticsearch.
    client = DataCacheClient.__new__(DataCacheClient)
    client._es = es
    client.obs_indices = ["obs"]
    return client


def test_get_reference_lists():
    assert _client(_StubEs()).get_reference_lists() == [
        ReferenceList("RED LIST", 30), ReferenceList("CHECKLIST", 10),
    ]


def test_reference_list_taxa_prefer_records_identified_to_the_accepted_name():
    rows = [
        # Lacerta agilis: mostly recorded under a synonym, but the accepted
        # name's own records decide.
        ("GS7", "GS7", "DI6", 5), ("GS7", "SYN", "SYNKEY", 50),
        # Natrix helvetica: only ever recorded under a synonym.
        ("AOKG", "985", "NATRIX_SYN", 7), ("AOKG", "986", "NATRIX_OTHER", 3),
    ]
    es = _StubEs(rows)
    assert _client(es).get_reference_list_taxa("RED LIST") == {"DI6", "NATRIX_SYN"}
    assert es.requests[0]["query"] == {"term": {"List": "RED LIST"}}


def test_reference_list_taxa_page_through_all_buckets():
    rows = [(f"A{i:03}", f"A{i:03}", f"T{i:03}", 1) for i in range(25)]
    es = _StubEs(rows)
    assert _client(es).get_reference_list_taxa("RED LIST", page_size=10) == {f"T{i:03}" for i in range(25)}
    assert len(es.requests) == 3


def test_reference_list_taxa_of_unknown_list_is_empty():
    assert _client(_StubEs()).get_reference_list_taxa("NOPE") == set()
