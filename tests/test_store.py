import pytest
from crawler import store

def test_insert_repositories(monkeypatch):
    """Test insert_repositories with fake DB connection."""

    class FakeCursor:
        def executemany(self, sql, rows):
            self.data = rows
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class FakeConn:
        def cursor(self): return FakeCursor()
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def commit(self): pass
        def rollback(self): pass

    monkeypatch.setattr(store, "get_conn", lambda: FakeConn())

    repos = [{"id": "1", "name": "repo", "description": "desc", "stargazerCount": 10, "url": "http://x"}]
    count = store.insert_repositories(repos)

    assert count == 1

