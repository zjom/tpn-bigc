# CLAUDE.md

This repo contains `bigcommerce_client`, an async BigCommerce REST client. Use it for any bulk
read or write against a BigCommerce store. Don't write ad-hoc `requests`/`curl` loops: they
ignore the store's rate limit and collect 429s.

**Channel:** Unless otherwise specified, perform all actions on channel 1 (The Pharmacy Network)
in BigCommerce. The store also has a separate BrushGo storefront channel; don't touch it unless
asked.

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/pytest -q          # offline tests, no store needed
```

Credentials come from the environment, never from the repo:

- `BIGCOMMERCE_STORE_HASH`: the `{store_hash}` in `https://api.bigcommerce.com/stores/{store_hash}`.
- `BIGCOMMERCE_ACCESS_TOKEN`: an API account token, sent as `X-Auth-Token`.

In a Claude Code cloud session where `api.bigcommerce.com` is listed as a proxy-injected host,
the proxy adds the real token. Pass any placeholder string as the token, and don't ask the user
for one. Never commit a store hash, token or store data.

## What the client does for you

- Caps requests in flight at `max_concurrency` (default 8).
- Uses one shared gate for every task. A 429 pauses all workers for `X-Rate-Limit-Time-Reset-Ms`,
  then retries. When `X-Rate-Limit-Requests-Left` drops to `max_concurrency` or lower, it pauses
  before the store has to send a 429.
- Retries 429s always. Retries 5xx and network errors only for GET/PUT/DELETE, never POST, so a
  retry can't create duplicates.
- Raises `BigCommerceError` (`.status_code`, `.body`) on any other 4xx.

Don't add your own sleeps, retry loops or semaphores around it. Change `max_concurrency` instead,
and keep it at or below the default. The store's quota is shared with every other app on it.

## Bulk request pattern

Write a script in your scratchpad (not the repo) and run it with `.venv/bin/python`.

```python
import asyncio, json, logging, os
from bigcommerce_client import BigCommerceClient, BigCommerceError

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)   # one line per request is noise at scale

def chunks(items, n):
    return [items[i:i + n] for i in range(0, len(items), n)]

async def main(dry_run=True):
    async with BigCommerceClient(
        os.environ["BIGCOMMERCE_STORE_HASH"],
        os.environ.get("BIGCOMMERCE_ACCESS_TOKEN", "injected-by-proxy"),
    ) as bc:
        # 1. Read: get_all fetches every page in parallel. Ask only for the fields you need.
        products = await bc.get_all(
            "/v3/catalog/products",
            params={"include_fields": "name,sku,is_visible", "is_visible": "false"},
        )
        print(f"{len(products)} products match")

        # 2. Build the change list, and show it before writing anything.
        updates = [{"id": p["id"], "is_visible": True} for p in products]
        if dry_run:
            print(json.dumps(updates[:5], indent=2), "...")
            return

        # 3. Write, using the endpoint's batch form (10 products per PUT here), in parallel.
        results = await bc.map(
            lambda batch: bc.put("/v3/catalog/products", json=batch),
            chunks(updates, 10),
            return_exceptions=True,   # one bad batch must not cancel the others
        )

        # 4. Report failures with enough detail to retry just those.
        failed = [(b, r) for b, r in zip(chunks(updates, 10), results) if isinstance(r, Exception)]
        for batch, err in failed:
            detail = err.body if isinstance(err, BigCommerceError) else repr(err)
            print("FAILED ids", [u["id"] for u in batch], detail)
        print(f"{len(results) - len(failed)}/{len(results)} batches ok; stats={bc.stats}")

asyncio.run(main(dry_run=True))
```

### Rules for bulk work

1. **Read before you write.** Run with `dry_run=True` first. Show the user the count and a sample
   of the changes, and get confirmation before any POST/PUT/DELETE against a live store.
2. **Use batch endpoints where they exist.** One request carrying N records spends one unit of
   quota instead of N. Check the BigCommerce docs for each endpoint's batch size. Common ones:
   `PUT /v3/catalog/products` (up to 10), `PUT /v3/catalog/variants` (up to 50),
   `POST/PUT /v3/customers` (up to 10). Many v3 DELETE and GET endpoints accept `id:in=1,2,3`.
   Use `bc.map` over single-record endpoints only when no batch form exists.
3. **Always pass `return_exceptions=True` to `bc.map` for writes**, then list exactly which
   records failed and why. Never report success from the request count alone.
4. **POSTs aren't retried on 5xx.** Before re-running failed creates, check whether the record
   already exists (look it up by SKU, email, etc.).
5. **Trim payloads.** Use `include_fields` / `exclude_fields` on v3 reads.
6. **Paginate with `get_all` / `iter_pages`.** They handle v3 (`meta.pagination`) and v2 (bare
   list, 204 at the end). Use `iter_pages` to stream very large collections instead of holding
   them all in memory.
7. Use `bc.request(...)` when you need headers or the status code, and
   `get/post/put/delete` when you only need the parsed JSON (`None` for 204).

## Changing the client

- Keep `tests/test_client.py` passing, and add a `FakeStore`/`MockTransport` test for new
  behavior. Tests must not hit a real store.
- Rate-limit headers: `X-Rate-Limit-Requests-Left`, `X-Rate-Limit-Requests-Quota`,
  `X-Rate-Limit-Time-Reset-Ms`, `X-Rate-Limit-Time-Window-Ms`.
