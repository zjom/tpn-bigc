"""Async BigCommerce REST client with bounded parallelism and 429 handling.

BigCommerce enforces a per-store request quota over a rolling window and
reports it on every response:

    X-Rate-Limit-Requests-Quota   requests allowed per window
    X-Rate-Limit-Requests-Left    requests left in the current window
    X-Rate-Limit-Time-Window-Ms   window length
    X-Rate-Limit-Time-Reset-Ms    ms until the window resets

When the quota is exhausted it answers 429 and the client must wait
X-Rate-Limit-Time-Reset-Ms before trying again. This client shares one
RateLimitGate across every concurrent task, so a single 429 (or a response
showing the quota is nearly spent) pauses *all* workers until the window
resets instead of letting each of them hammer the API and collect its own 429.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Mapping, TypeVar

import httpx

log = logging.getLogger("bigcommerce_client")

T = TypeVar("T")
R = TypeVar("R")

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
RETRYABLE_STATUS = frozenset({500, 502, 503, 504})


class BigCommerceError(Exception):
    """Non-success response from BigCommerce (after any retries)."""

    def __init__(self, response: httpx.Response):
        self.response = response
        self.status_code = response.status_code
        try:
            self.body: Any = response.json()
        except ValueError:
            self.body = response.text
        req = response.request
        super().__init__(f"{req.method} {req.url} -> {response.status_code}: {self.body}")


class RateLimitGate:
    """Shared pause point for every request made through one client.

    Workers call ``wait()`` before sending. ``pause(seconds)`` pushes the
    resume time forward (never backward), so overlapping 429s from many
    in-flight requests collapse into one pause.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._resume_at = 0.0

    @property
    def paused_for(self) -> float:
        return max(0.0, self._resume_at - self._clock())

    def pause(self, seconds: float) -> None:
        self._resume_at = max(self._resume_at, self._clock() + seconds)

    async def wait(self) -> None:
        # Loop because another task may extend the pause while we sleep.
        while (delay := self.paused_for) > 0:
            await asyncio.sleep(delay)


def _header_float(headers: httpx.Headers, name: str) -> float | None:
    value = headers.get(name)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


class BigCommerceClient:
    """Async client for ``https://api.bigcommerce.com/stores/{store_hash}``.

    Use as an async context manager::

        async with BigCommerceClient("abc123", access_token="...") as bc:
            products = await bc.get_all("/v3/catalog/products")
            results = await bc.map(lambda p: bc.put(f"/v3/catalog/products/{p['id']}",
                                                    json={"is_visible": True}),
                                   products)

    Args:
        store_hash: The store hash from the API path.
        access_token: API account token, sent as ``X-Auth-Token``.
        max_concurrency: Max requests in flight at once.
        max_retries: Retries for 429s, 5xx and transport errors (5xx and
            transport errors are retried only for idempotent methods).
        throttle_below: If a response reports this many or fewer requests
            left in the window, pause everyone until the window resets
            instead of waiting to be told with a 429. Defaults to
            ``max_concurrency`` (requests already in flight still spend
            quota). 0 disables it.
        timeout: Per-request timeout in seconds.
        base_url: Override for testing or other API hosts.
        transport: Optional httpx transport (useful for tests).
    """

    def __init__(
        self,
        store_hash: str,
        access_token: str,
        *,
        max_concurrency: int = 8,
        max_retries: int = 5,
        throttle_below: int | None = None,
        timeout: float = 30.0,
        backoff_base: float = 0.5,
        backoff_max: float = 30.0,
        base_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.throttle_below = max_concurrency if throttle_below is None else throttle_below
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.gate = RateLimitGate()
        self._sem = asyncio.Semaphore(max_concurrency)
        self._http = httpx.AsyncClient(
            base_url=base_url or f"https://api.bigcommerce.com/stores/{store_hash}",
            headers={
                "X-Auth-Token": access_token,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=timeout,
            transport=transport,
            limits=httpx.Limits(max_connections=max_concurrency,
                                max_keepalive_connections=max_concurrency),
        )
        # Simple counters, handy for logging/monitoring.
        self.stats = {"requests": 0, "rate_limited": 0, "retries": 0}

    async def __aenter__(self) -> "BigCommerceClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------ core

    async def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Send one request, honoring the shared rate-limit gate and retrying.

        Returns the raw ``httpx.Response`` for any 2xx/3xx status; raises
        ``BigCommerceError`` otherwise.
        """
        method = method.upper()
        attempt = 0
        while True:
            await self.gate.wait()
            async with self._sem:
                # The gate may have closed while we queued for a slot.
                await self.gate.wait()
                self.stats["requests"] += 1
                try:
                    resp = await self._http.request(method, path, **kwargs)
                except httpx.TransportError as exc:
                    if method not in IDEMPOTENT_METHODS or attempt >= self.max_retries:
                        raise
                    delay = self._backoff(attempt)
                    log.warning("%s %s failed (%r); retrying in %.2fs", method, path, exc, delay)
                    resp = None

            if resp is not None:
                self._observe_quota(resp)

                if resp.status_code == 429:
                    self.stats["rate_limited"] += 1
                    if attempt >= self.max_retries:
                        raise BigCommerceError(resp)
                    delay = self._reset_delay(resp, attempt)
                    self.gate.pause(delay)
                    log.info("429 on %s %s; pausing all requests %.2fs", method, path, delay)
                    attempt += 1
                    self.stats["retries"] += 1
                    continue  # the gate does the waiting

                if resp.status_code in RETRYABLE_STATUS and method in IDEMPOTENT_METHODS \
                        and attempt < self.max_retries:
                    delay = self._backoff(attempt)
                    log.warning("%s %s -> %s; retrying in %.2fs",
                                method, path, resp.status_code, delay)
                elif resp.is_error:
                    raise BigCommerceError(resp)
                else:
                    return resp

            attempt += 1
            self.stats["retries"] += 1
            await asyncio.sleep(delay)

    def _observe_quota(self, resp: httpx.Response) -> None:
        """Pause proactively when the window is almost used up."""
        if self.throttle_below <= 0 or resp.status_code == 429:
            return
        left = _header_float(resp.headers, "X-Rate-Limit-Requests-Left")
        reset_ms = _header_float(resp.headers, "X-Rate-Limit-Time-Reset-Ms")
        if left is not None and reset_ms is not None and left <= self.throttle_below:
            self.gate.pause(reset_ms / 1000.0)
            log.debug("Only %d requests left; pausing %.0fms", left, reset_ms)

    def _reset_delay(self, resp: httpx.Response, attempt: int) -> float:
        reset_ms = _header_float(resp.headers, "X-Rate-Limit-Time-Reset-Ms")
        if reset_ms is not None:
            # Small jitter so the resumed burst doesn't land in lockstep.
            return reset_ms / 1000.0 + random.uniform(0, 0.25)
        retry_after = _header_float(resp.headers, "Retry-After")
        if retry_after is not None:
            return retry_after
        return self._backoff(attempt)

    def _backoff(self, attempt: int) -> float:
        cap = min(self.backoff_max, self.backoff_base * (2 ** attempt))
        return random.uniform(cap / 2, cap)

    # ------------------------------------------------------------- shortcuts

    @staticmethod
    def _json(resp: httpx.Response) -> Any:
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    async def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return self._json(await self.request("GET", path, params=params))

    async def post(self, path: str, json: Any = None, **kw: Any) -> Any:
        return self._json(await self.request("POST", path, json=json, **kw))

    async def put(self, path: str, json: Any = None, **kw: Any) -> Any:
        return self._json(await self.request("PUT", path, json=json, **kw))

    async def delete(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return self._json(await self.request("DELETE", path, params=params))

    # --------------------------------------------------------- parallel work

    async def map(
        self,
        fn: Callable[[T], Awaitable[R]],
        items: Iterable[T],
        *,
        return_exceptions: bool = False,
    ) -> list[R | BaseException]:
        """Run ``fn(item)`` for every item concurrently; results keep input order.

        Concurrency is bounded by the client's semaphore, so it is safe to pass
        thousands of items. With ``return_exceptions=True`` a failed item yields
        its exception instead of cancelling the rest.
        """
        return await asyncio.gather(*(fn(i) for i in items),
                                    return_exceptions=return_exceptions)

    async def iter_pages(
        self, path: str, params: Mapping[str, Any] | None = None, *, limit: int = 250
    ) -> AsyncIterator[list[Any]]:
        """Yield pages of items in order, fetching pages in parallel.

        V3 endpoints (``{"data": [...], "meta": {"pagination": ...}}``): page 1
        reveals ``total_pages``, then the rest are fetched concurrently.
        V2 endpoints (bare list, 204 when past the end): pages are fetched in
        concurrent windows until a short or empty page appears.
        """
        base = {**(params or {}), "limit": limit}
        first = await self.get(path, {**base, "page": 1})

        if isinstance(first, dict) and "data" in first:
            yield first["data"]
            total = (first.get("meta") or {}).get("pagination", {}).get("total_pages", 1)
            pages = range(2, total + 1)
            for start in range(0, len(pages), self.max_concurrency * 4):
                chunk = pages[start:start + self.max_concurrency * 4]
                results = await self.map(
                    lambda p: self.get(path, {**base, "page": p}), chunk)
                for body in results:
                    yield body["data"]
            return

        first = first or []
        yield first
        if len(first) < limit:
            return
        page = 2
        while True:
            chunk = range(page, page + self.max_concurrency)
            results = await self.map(lambda p: self.get(path, {**base, "page": p}), chunk)
            for body in results:
                body = body or []
                if body:
                    yield body
                if len(body) < limit:
                    return
            page += self.max_concurrency

    async def get_all(
        self, path: str, params: Mapping[str, Any] | None = None, *, limit: int = 250
    ) -> list[Any]:
        """Fetch every item of a paginated collection (V2 or V3)."""
        out: list[Any] = []
        async for page in self.iter_pages(path, params, limit=limit):
            out.extend(page)
        return out
