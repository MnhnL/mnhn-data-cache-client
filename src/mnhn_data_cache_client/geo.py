"""Geometry helpers for area-based observation queries.

Why a hand-rolled projection instead of a library
-------------------------------------------------
Buffering in metres needs a metric projection. The obvious libraries all cause
trouble here:

* pyproj: its wheels bundle their own copy of the PROJ library. A process that
  also loads the system PROJ (e.g. Django with a SpatiaLite database) ends up
  with two PROJ copies and corrupts its heap ("corrupted size vs. prev_size").
* pyproj built from source against the system PROJ avoids the clash, but then
  the build machine and every deploy target must have the same PROJ version,
  which breaks when wheels are built in one place and installed in another.
* GDAL (osgeo, or GeoDjango) uses the system PROJ, but would tie this
  backend-agnostic client to Django or to a GDAL build matching the system.

A local equirectangular projection on the WGS84 ellipsoid is a few lines,
needs nothing beyond shapely, and is accurate to well under 1 % of the buffer
distance for areas tens of kilometres across (tests check it against geodesic
distances). That is plenty for "observations within X m of an area".

This is only needed by the Elasticsearch backend: a PostGIS backend can do the
same exactly with ST_DWithin on geography.
"""
import math

import shapely
from shapely.geometry import mapping, shape

# WGS84 ellipsoid
_A = 6378137.0
_E2 = 6.69437999014e-3


def _local_scale(lat_deg):
    """Metres per degree of (longitude, latitude) at *lat_deg*."""
    phi = math.radians(lat_deg)
    w = 1 - _E2 * math.sin(phi) ** 2
    n = _A / math.sqrt(w)               # prime vertical radius of curvature
    m = _A * (1 - _E2) / w ** 1.5       # meridional radius of curvature
    return math.radians(1) * n * math.cos(phi), math.radians(1) * m


def buffer_geojson(geometry, distance_m):
    """Return *geometry* grown by *distance_m* metres, as a GeoJSON dict.

    Args:
        geometry: A GeoJSON geometry dict in WGS84 (lon/lat).
        distance_m: Buffer distance in metres.

    The geometry is projected into a local equirectangular plane centred on
    its own centroid, buffered there and projected back. Distances are exact
    along the centre's parallel and meridian; the error grows with the area's
    north-south extent (about 0.5 % of *distance_m* for a 50 km tall area at
    mid latitudes). The result is simplified with a tolerance of 2 % of
    *distance_m* to keep the query payload small.
    """
    geom = shape(geometry)
    centre = geom.centroid
    lon0, lat0 = centre.x, centre.y
    kx, ky = _local_scale(lat0)

    def to_local(coords):
        return (coords - (lon0, lat0)) * (kx, ky)

    def to_wgs84(coords):
        return coords / (kx, ky) + (lon0, lat0)

    buffered = shapely.transform(geom, to_local).buffer(distance_m)
    # Buffering rounds every vertex into an arc of many short segments, so a
    # detailed area comes out several times larger, and the whole shape is
    # sent with every query. Simplifying in the local plane keeps the tolerance
    # in metres; preserve_topology keeps rings from collapsing or crossing.
    buffered = buffered.simplify(distance_m * 0.02, preserve_topology=True)
    return mapping(shapely.transform(buffered, to_wgs84))
