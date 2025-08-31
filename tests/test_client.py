import pytest
from crawler import client

@pytest.mark.asyncio
async def test_fetch_repos_mocked(monkeypatch):
    """Test fetch_repos returns empty list when mocked."""

    async def fake_fetch_repos(query: str, retries=5):
        return []

    monkeypatch.setattr(client, "fetch_repos", fake_fetch_repos)

    result = await client.fetch_repos("stars:>100")
    assert result == []

