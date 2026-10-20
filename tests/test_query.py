"""Unit tests for observation query building — no Elasticsearch needed."""
import pytest

shapely = pytest.importorskip("shapely")
pytest.importorskip("pyproj")

from pyproj import Geod
from shapely.geometry import Point, shape

from mnhn_data_cache_client import DataCacheClient
from mnhn_data_cache_client.geo import buffer_geojson

# ~1 km square in Luxembourg City.
SQUARE = {
    "type": "Polygon",
    "coordinates": [[
        [6.12, 49.60], [6.134, 49.60], [6.134, 49.609], [6.12, 49.609], [6.12, 49.60],
    ]],
}

GEOD = Geod(ellps="WGS84")


def _query(**kwargs):
    # Skip __init__, which connects to Elasticsearch.
    return DataCacheClient.__new__(DataCacheClient)._make_observations_query(**kwargs)


def _geo_shapes(query):
    (geo,) = query["bool"]["filter"]
    return [c["geo_shape"]["location"] for c in geo["bool"]["should"]]


def _east_of(lon, lat, metres):
    lon2, lat2, _ = GEOD.fwd(lon, lat, 90, metres)
    return Point(lon2, lat2)


def test_areas_are_ored_intersects_clauses():
    other = {"type": "Point", "coordinates": [6.0, 49.7]}
    shapes = _geo_shapes(_query(areas=[SQUARE, other]))
    assert [s["shape"] for s in shapes] == [SQUARE, other]
    assert all(s["relation"] == "intersects" for s in shapes)


def test_areas_without_within_m_are_passed_unchanged():
    (s,) = _geo_shapes(_query(areas=[SQUARE], within_m=0))
    assert s["shape"] == SQUARE


def test_within_m_buffers_each_area():
    (s,) = _geo_shapes(_query(areas=[SQUARE], within_m=200))
    buffered = shape(s["shape"])
    assert buffered.contains(shape(SQUARE))


@pytest.mark.parametrize("distance_m", [50, 200, 1000])
def test_buffer_distance_is_accurate_in_metres(distance_m):
    buffered = shape(buffer_geojson(SQUARE, distance_m))
    # East edge sits at lon 6.134, where a degree of longitude is only ~72 km:
    # a buffer done in degrees would be visibly off here.
    assert buffered.contains(_east_of(6.134, 49.6045, distance_m * 0.95))
    assert not buffered.contains(_east_of(6.134, 49.6045, distance_m * 1.05))


def test_buffer_stays_accurate_across_a_large_area():
    # ~55 km tall, roughly Luxembourg's extent; check at its northern edge,
    # furthest from the centre of the local projection.
    tall = {
        "type": "Polygon",
        "coordinates": [[[5.9, 49.45], [6.4, 49.45], [6.4, 49.95], [5.9, 49.95], [5.9, 49.45]]],
    }
    buffered = shape(buffer_geojson(tall, 500))
    assert buffered.contains(_east_of(6.4, 49.94, 500 * 0.97))
    assert not buffered.contains(_east_of(6.4, 49.94, 500 * 1.03))


def test_buffer_handles_multipolygons():
    multi = {
        "type": "MultiPolygon",
        "coordinates": [SQUARE["coordinates"], [[
            [6.20, 49.60], [6.21, 49.60], [6.21, 49.61], [6.20, 49.61], [6.20, 49.60],
        ]]],
    }
    buffered = shape(buffer_geojson(multi, 100))
    assert buffered.contains(shape(multi))


def test_date_range_bounds_are_inclusive():
    def date_filter(date_range):
        (clause,) = _query(date_range=date_range)["bool"]["filter"]
        return clause["range"]["date_start"]

    assert date_filter(("2024-01-01", "2024-12-31")) == {"gte": "2024-01-01", "lte": "2024-12-31"}
    assert date_filter(("2024-01-01", None)) == {"gte": "2024-01-01"}
    assert date_filter((None, "2024-12-31")) == {"lte": "2024-12-31"}
