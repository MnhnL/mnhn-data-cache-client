from elasticsearch6 import Elasticsearch
from pprint import pp

class DataCacheClient():
    obs_indices="recorder,inaturalist_current,mnhn_gbif,ornitho"
    taxon_indices="mnhn-taxa"

    def __init__(self, es_host="serv-gis.vm.mnhn.etat.lu", chunk_size=35000):
        self._es_host = es_host
        self._es = Elasticsearch(["serv-gis.vm.mnhn.etat.lu"])
        self._chunk_size = chunk_size
        
        self._observation_mapping = self._es.indices.get_mapping('recorder')
        self._taxon_mapping = self._es.indices.get_mapping(DataCacheClient.taxon_indices)

    def observation_mapping(self):
        if 'mnhn' in self._observation_mapping:
            ks = self._observation_mapping['mnhn']['mappings']['observations']['properties']
        elif 'mnhn2' in self._observation_mapping:
            ks = self._observation_mapping['mnhn2']['mappings']['observations']['properties']
        return ks

    def taxon_mapping(self):
        ks = self._taxon_mapping[DataCacheClient.taxon_indices]['mappings']['taxa']['properties']
        return ks

    def _make_observations_query(self,
                                 taxon_name=None,
                                 date_range=None,
                                 recorder_names=None,
                                 polygon=None):
        query ={
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
                raise Error()

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
        count = self._es.count(index=DataCacheClient.obs_indices,
                               body={"query": query})["count"]
        body = self._make_observations_body(query)
        for i in range(0, count, self._chunk_size):
            body["size"] = self._chunk_size
            body["from"] = i
            obs = self._es.search(index=DataCacheClient.obs_indices,
                                  size=self._chunk_size,
                                  body=body)
            yield(obs)

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

    def _make_taxa_body(self, query):
        return {
            "query": query,
        }
            
    def search_taxa(self, **kwargs):
        query = self._make_taxa_query(**kwargs)
        search_body = {"query": query}
        count = self._es.count(index=DataCacheClient.taxon_indices,
                               body=search_body)["count"]
        body = self._make_taxa_body(query)
        for i in range(0, count, self._chunk_size):
            body["size"] = self._chunk_size
            body["from"] = i
            obs = self._es.search(index=DataCacheClient.taxon_indices,
                                  size=self._chunk_size,
                                  body=body)
            yield(obs)

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
                    c = src.get(col, "n/a")
                    print(repr(c), end="\t")
                print()
        print(f"Found {count} items")
