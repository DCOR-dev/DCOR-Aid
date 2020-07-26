import time
from dcoraid.dbmodel import util


def test_cache():
    count = 0

    @util.ttl_cache(seconds=.1)
    def test(arg1):
        nonlocal count
        count += 1
        return count

    assert test(1) == 1, "called the first time we invoke it"
    assert test(1) == 1, "not called because it is already cached"

    time.sleep(.01)
    assert test(1) == 1, "not called because it is already cached"

    # Let's now wait for the cache to expire
    time.sleep(.1)
    assert test(1) == 2, "called because the cache expired"


if __name__ == "__main__":
    # Run all tests
    loc = locals()
    for key in list(loc.keys()):
        if key.startswith("test_") and hasattr(loc[key], "__call__"):
            loc[key]()
