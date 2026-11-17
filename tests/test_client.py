import asyncio
import time

import httpx
import pytest

from bigcommerce_client import BigCommerceClient, BigCommerceError

pytestmark = pytest.mark.asyncio


class FakeStore:
    """Fake BigCommerce: fixed quota per window, 429 when exceeded, tracks concurrency."""

    def __init__(self, quota=5, window_ms=200, latency=0.01):
        self.quota, self.window_ms, self.latency = quota, window_ms, latency
        self.window_start = time.monotonic()
        self.used = 0
        self.in_flight = self.max_in_flight = 0
        self.count_429 = self.count_ok = 0

    def _headers(self):
        reset = max(0, self.window_ms - (time.monotonic() - self.window_start) * 1000)
        return {
            "X-Rate-Limit-Requests-Quota": str(self.quota),
            "X-Rate-Limit-Requests-Left": str(max(0, self.quota - self.used)),
            "X-Rate-Limit-Time-Window-Ms": str(self.window_ms),
            "X-Rate-Limit-Time-Reset-Ms": str(int(reset)),
        }

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.latency)
            if (time.monotonic() - self.window_start) * 1000 >= self.window_ms:
                self.window_start, self.used = time.monotonic(), 0
            if self.used >= self.quota:
                self.count_429 += 1
                return httpx.Response(429, headers=self._headers(), json={"title": "Too many"})
            self.used += 1
            self.count_ok += 1
            return httpx.Response(200, headers=self._headers(), json={"path": request.url.path})
        finally:
            self.in_flight -= 1


def make_client(handler, **kw):
    return BigCommerceClient("store", "token", base_url="https://bc.test",
                             transport=httpx.MockTransport(handler), **kw)


async def test_parallel_requests_survive_rate_limit_and_respect_concurrency():
    store = FakeStore(quota=5, window_ms=150)
    async with make_client(store.handler, max_concurrency=4, max_retries=10) as bc:
        results = await bc.map(lambda i: bc.get(f"/v3/items/{i}"), range(30))
    assert [r["path"] for r in results] == [f"/v3/items/{i}" for i in range(30)]
    assert store.count_ok == 30
    assert store.max_in_flight <= 4
    # Proactive throttling + shared gate should keep 429s rare.
    assert store.count_429 <= 4, store.count_429


async def test_429_pauses_every_worker_for_reset_window():
    calls = []

    async def handler(request):
        calls.append(time.monotonic())
        if len(calls) == 1:
            return httpx.Response(429, headers={"X-Rate-Limit-Time-Reset-Ms": "300"})
        return httpx.Response(200, json={})

    async with make_client(handler, max_concurrency=1, throttle_below=0) as bc:
        start = time.monotonic()
        await asyncio.gather(bc.get("/a"), bc.get("/b"), bc.get("/c"))
    # Every call after the 429 waited for the reset window.
    assert all(t - start >= 0.3 for t in calls[1:])
    assert bc.stats["rate_limited"] == 1


async def test_retry_after_fallback_and_retry_exhaustion():
    async def handler(request):
        return httpx.Response(429, headers={"Retry-After": "0.01"})

    async with make_client(handler, max_retries=2) as bc:
        with pytest.raises(BigCommerceError) as exc:
            await bc.get("/x")
    assert exc.value.status_code == 429
    assert bc.stats["requests"] == 3


async def test_5xx_retried_for_get_but_not_post():
    hits = {"GET": 0, "POST": 0}

    async def handler(request):
        hits[request.method] += 1
        if hits[request.method] == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"ok": True})

    async with make_client(handler, backoff_base=0.01) as bc:
        assert await bc.get("/x") == {"ok": True}
        with pytest.raises(BigCommerceError):
            await bc.post("/x", json={})
    assert hits == {"GET": 2, "POST": 1}


async def test_4xx_raises_with_body():
    async def handler(request):
        return httpx.Response(422, json={"title": "bad sku"})

    async with make_client(handler) as bc:
        with pytest.raises(BigCommerceError) as exc:
            await bc.post("/v3/catalog/products", json={})
    assert exc.value.status_code == 422 and exc.value.body == {"title": "bad sku"}


async def test_v3_pagination_parallel_and_ordered():
    total_items, limit = 23, 5
    seen_pages = []

    async def handler(request):
        page = int(request.url.params["page"])
        seen_pages.append(page)
        await asyncio.sleep(0.05 if page % 2 else 0.0)  # finish out of order
        start = (page - 1) * limit
        data = list(range(start, min(start + limit, total_items)))
        return httpx.Response(200, json={"data": data, "meta": {"pagination": {
            "total": total_items, "total_pages": 5, "current_page": page}}})

    async with make_client(handler) as bc:
        items = await bc.get_all("/v3/catalog/products", limit=limit)
    assert items == list(range(23))
    assert sorted(seen_pages) == [1, 2, 3, 4, 5]


async def test_v2_pagination_stops_on_empty_page():
    total_items, limit = 12, 5

    async def handler(request):
        page = int(request.url.params["page"])
        start = (page - 1) * limit
        data = list(range(start, min(start + limit, total_items)))
        return httpx.Response(200, json=data) if data else httpx.Response(204)

    async with make_client(handler, max_concurrency=2) as bc:
        assert await bc.get_all("/v2/orders", limit=limit) == list(range(12))
