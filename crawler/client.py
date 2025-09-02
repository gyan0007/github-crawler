import os
import aiohttp
import asyncio
import random

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise RuntimeError("❌ GITHUB_TOKEN not set")

GITHUB_API = "https://api.github.com/graphql"

QUERY = """
query ($query: String!, $cursor: String) {
  search(query: $query, type: REPOSITORY, first: 100, after: $cursor) {
    nodes {
      ... on Repository {
        id
        name
        description
        stargazerCount
        forkCount   # used only for sharding
        url
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}


async def fetch_repos(query: str, retries=5):
    """Fetch repositories from GitHub GraphQL search API with rate-limit awareness.
       Returns (repos, remaining_quota, reset_epoch)."""
    repos = []
    cursor = None
    remaining = None
    reset = None

    async with aiohttp.ClientSession() as session:
        while True:
            variables = {"query": query, "cursor": cursor}
            try:
                async with session.post(
                    GITHUB_API,
                    json={"query": QUERY, "variables": variables},
                    headers=HEADERS,
                ) as resp:
                    data = await resp.json()
                    remaining = int(resp.headers.get("X-RateLimit-Remaining", "1"))
                    reset = int(resp.headers.get("X-RateLimit-Reset", "0"))

                    if resp.status == 403 or "RATE_LIMITED" in str(data):
                        if retries > 0:
                            backoff = random.uniform(10, 20)
                            print(
                                f"⚠️ Hard rate limit (query={query}). "
                                f"Retrying in {backoff:.1f}s..."
                            )
                            await asyncio.sleep(backoff)
                            return await fetch_repos(query, retries=retries - 1)
                        else:
                            raise RuntimeError(
                                f"❌ Rate limited after retries (query={query})"
                            )

                    if "errors" in data:
                        raise RuntimeError(f"❌ GitHub API error: {data['errors']}")

                    nodes = data["data"]["search"]["nodes"]

                    # Drop forkCount since we don't store it
                    for n in nodes:
                        n.pop("forkCount", None)

                    repos.extend(nodes)

                    page_info = data["data"]["search"]["pageInfo"]
                    if not page_info["hasNextPage"]:
                        break
                    cursor = page_info["endCursor"]

            except Exception as e:
                if retries > 0:
                    backoff = random.uniform(5, 15)
                    print(f"⚠️ Error: {e}. Retrying in {backoff:.2f}s...")
                    await asyncio.sleep(backoff)
                    return await fetch_repos(query, retries=retries - 1)
                else:
                    print(f"❌ Failed permanently on query={query}: {e}")
                    break

    return repos, remaining, reset

