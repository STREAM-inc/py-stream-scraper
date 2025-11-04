import re
import fakeredis
import pytest


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def url_filter():
    return [re.compile(r"^/(blog|news)/")]
