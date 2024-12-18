from mnhn_data_cache_client import DataCacheClient

dcc = DataCacheClient("serv-data.vm.mnhn.etat.lu")

points = [[6.178704087531173,49.68863533449158],[6.178704087531173,49.682717804951466],[6.182024794656826,49.682717804951466],[6.182024794656826,49.68863533449158],[6.178704087531173,49.68863533449158]]


dcc.print_results(dcc.search_observations(# taxon_name="Galium odoratum",
                                          date_range=["2024-01-01", "2024-12-01"],
                                          polygon=points),
                  ["date_start", "Taxon_Name", "Autority", "Taxon_Common_Names", "Determiner", "Lat", "Long", "Recorders"])
# dcc.print_results(dcc.search_taxa(taxon_name="Galium", rank="species"),
#                   ["taxon_item_name", "taxon_authority", "taxon_rank", "taxon_preferred"])






