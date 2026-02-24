"""Integration tests against a live Elasticsearch instance.

Set MNHN_ES_HOST and run:

    export MNHN_ES_HOST=serv-data.vm.mnhn.etat.lu
    pytest tests/test_integration.py -v

All tests are skipped automatically when MNHN_ES_HOST is not set.

Customisation
-------------
Fill in the placeholders below with taxa that actually exist in your data.
"""

import pytest

from mnhn_data_cache_client import TaxonFilter

# ---------------------------------------------------------------------------
# Placeholders — replace with taxa present in your data
# ---------------------------------------------------------------------------

A_GENUS       = "Galium"           # a genus with several observed species
A_SPECIES     = "Galium odoratum"  # a species that belongs to A_GENUS
ANOTHER_GENUS = "Rubia"            # an unrelated genus, also observed
ABSENT_TAXON  = "Zznonexistent"    # something guaranteed absent from the data

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _obs_count(client, **kwargs):
    """Return the total number of observation hits for the given query kwargs."""
    return sum(
        len(page["hits"]["hits"])
        for page in client.search_observations(**kwargs)
    )


# ---------------------------------------------------------------------------
# Session-scoped fixtures — resolved once, reused across all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def genus_source(live_client):
    return live_client.find_taxon(A_GENUS, rank="genus")


@pytest.fixture(scope="session")
def species_source(live_client):
    return live_client.find_taxon(A_SPECIES, rank=None)


@pytest.fixture(scope="session")
def another_genus_source(live_client):
    return live_client.find_taxon(ANOTHER_GENUS, rank="genus")


# ---------------------------------------------------------------------------
# Taxon index
# ---------------------------------------------------------------------------

class TestTaxonIndex:

    def test_known_genus_is_found(self, live_client):
        source = live_client.find_taxon(A_GENUS, rank="genus")
        assert source["taxon_item_name"] == A_GENUS
        assert source["taxon_rank"].lower() == "genus"

    def test_taxon_source_has_required_fields(self, genus_source):
        assert "taxon_key"       in genus_source
        assert "taxon_rank"      in genus_source
        assert "taxon_item_name" in genus_source

    def test_absent_taxon_raises(self, live_client):
        with pytest.raises(ValueError):
            live_client.find_taxon(ABSENT_TAXON)

    def test_prefix_search_returns_multiple_taxa(self, live_client):
        pages = list(live_client.search_taxa(taxon_name=A_GENUS))
        hits  = [h for page in pages for h in page["hits"]["hits"]]
        assert len(hits) > 1


# ---------------------------------------------------------------------------
# Observation results — basic sanity
# ---------------------------------------------------------------------------

class TestObservationResults:

    def test_exact_species_returns_results(self, live_client, species_source):
        tf = TaxonFilter.from_taxon_source(species_source)
        assert _obs_count(live_client, taxon_filters=[tf]) > 0

    def test_genus_with_subtaxa_returns_results(self, live_client, genus_source):
        tf = TaxonFilter.from_taxon_source(genus_source, include_subtaxa=True)
        assert _obs_count(live_client, taxon_filters=[tf]) > 0

    def test_complex_query_runs_without_error(
            self, live_client, genus_source, species_source):
        """A multi-filter query involving include, exclude and date range
        must execute without raising an exception."""
        filters = [
            TaxonFilter.from_taxon_source(genus_source,   include_subtaxa=True),
            TaxonFilter.from_taxon_source(species_source, exclude=True),
        ]
        list(live_client.search_observations(
            taxon_filters=filters,
            date_range=["2020-01-01", "2024-12-31"],
        ))

    def test_observation_hit_has_expected_fields(self, live_client, species_source):
        tf    = TaxonFilter.from_taxon_source(species_source)
        pages = list(live_client.search_observations(taxon_filters=[tf]))
        hit   = pages[0]["hits"]["hits"][0]["_source"]
        assert "Taxon_Name" in hit
        assert "date_start" in hit


# ---------------------------------------------------------------------------
# Observation results — relational properties
# These check ordering/comparison rather than specific counts, so they hold
# regardless of how the data changes over time.
# ---------------------------------------------------------------------------

class TestObservationRelativeProperties:

    def test_subtaxa_returns_at_least_as_many_as_exact_match(
            self, live_client, genus_source):
        tf_exact   = TaxonFilter.from_taxon_source(genus_source)
        tf_subtaxa = TaxonFilter.from_taxon_source(genus_source, include_subtaxa=True)
        assert (
            _obs_count(live_client, taxon_filters=[tf_subtaxa]) >=
            _obs_count(live_client, taxon_filters=[tf_exact])
        )

    def test_or_of_two_genera_returns_at_least_as_many_as_either_alone(
            self, live_client, genus_source, another_genus_source):
        tf_a = TaxonFilter.from_taxon_source(genus_source,         include_subtaxa=True)
        tf_b = TaxonFilter.from_taxon_source(another_genus_source, include_subtaxa=True)
        count_or = _obs_count(live_client, taxon_filters=[tf_a, tf_b])
        assert count_or >= _obs_count(live_client, taxon_filters=[tf_a])
        assert count_or >= _obs_count(live_client, taxon_filters=[tf_b])

    def test_excluding_species_does_not_increase_results(
            self, live_client, genus_source, species_source):
        tf_genus   = TaxonFilter.from_taxon_source(genus_source,   include_subtaxa=True)
        tf_exclude = TaxonFilter.from_taxon_source(species_source, exclude=True)
        assert (
            _obs_count(live_client, taxon_filters=[tf_genus, tf_exclude]) <=
            _obs_count(live_client, taxon_filters=[tf_genus])
        )

    def test_date_range_does_not_increase_results(self, live_client, genus_source):
        tf = TaxonFilter.from_taxon_source(genus_source, include_subtaxa=True)
        assert (
            _obs_count(live_client, taxon_filters=[tf],
                       date_range=["2024-01-01", "2024-12-31"]) <=
            _obs_count(live_client, taxon_filters=[tf])
        )
