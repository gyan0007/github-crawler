from crawler.ingest import build_star_ranges, build_shards

def test_build_star_ranges():
    ranges = build_star_ranges()
    assert isinstance(ranges, list)
    # Ensure all values are valid range strings (like "0..10" or ">10000")
    assert all(".." in r or r.startswith(">") for r in ranges)

def test_build_shards():
    shards = build_shards(months_back=1)
    assert isinstance(shards, list)
    assert len(shards) > 0

