import os

import pytest

from mnhn_data_cache_client import DataCacheClient


@pytest.fixture(scope="session")
def live_client():
    host = os.environ.get("MNHN_ES_HOST")
    if not host:
        pytest.skip("MNHN_ES_HOST not set — skipping integration tests")
    return DataCacheClient(host, verify_certs=False, ssl_show_warn=False)
