# bigcommerce-client

Async BigCommerce API client that runs requests in parallel without tripping the store's rate limit.

```python
import asyncio
from bigcommerce_client import BigCommerceClient

async def main():
    async with BigCommerceClient("STORE_HASH", access_token="TOKEN", max_concurrency=8) as bc:
        products = await bc.get_all("/v3/catalog/products", params={"include_fields": "name,sku"})

        # Fan out: results come back in input order, at most 8 requests in flight.
        await bc.map(
            lambda p: bc.put(f"/v3/catalog/products/{p['id']}", json={"is_visible": True}),
            products,
            return_exceptions=True,   # one failure doesn't cancel the rest
        )
        print(bc.stats)

asyncio.run(main())
```

## How rate limiting works

* **Shared gate.** Every request waits on one `RateLimitGate`. A 429 pauses *all* workers for
  `X-Rate-Limit-Time-Reset-Ms` (falls back to `Retry-After`, then exponential backoff), and the
  request is retried. Overlapping 429s merge into one pause.
* **Proactive throttle.** If a response shows `X-Rate-Limit-Requests-Left <= throttle_below`
  (default: `max_concurrency`), the gate closes until the window resets, so 429s are rare.
* **Retries.** 429s are always retried (BigCommerce didn't process the request). 5xx responses and
  network errors are retried only for idempotent methods (GET/PUT/DELETE…), never POST.
  Other 4xx responses raise `BigCommerceError` (`.status_code`, `.body`).

## API

| Method | |
|---|---|
| `get / post / put / delete(path, ...)` | Parsed JSON (None for 204) |
| `request(method, path, **httpx_kwargs)` | Raw `httpx.Response` |
| `map(fn, items, return_exceptions=False)` | Run coroutines concurrently, ordered results |
| `get_all(path, params, limit=250)` / `iter_pages(...)` | Pagination, parallel. V3 uses `meta.pagination.total_pages`; V2 fetches pages in windows until an empty page or a 204 |

Options: `max_concurrency`, `max_retries`, `throttle_below`, `timeout`, `backoff_base`, `backoff_max`.

## Tests

```bash
pip install -e '.[test]' && pytest
```
