"""WSGI application that dispatches console requests to the wired plant."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Callable

from . import handlers
from .http import (
    Handler,
    Request,
    RequestError,
    Response,
    Router,
    build_request,
    error_response,
)
from .middleware import guarded, status_for
from .routes import register_routes

if TYPE_CHECKING:  # pragma: no cover - typing only
    from bagsort.app import Plant

StartResponse = Callable[[str, list[tuple[str, str]]], object]


def _error_response(exc: Exception) -> Response:
    return error_response(status_for(exc), str(exc))


class Server:
    """The console: a router, a plant and the request lifecycle around them."""

    def __init__(self, plant: "Plant") -> None:
        self.plant = plant
        self.router = Router()
        register_routes(self.router, handlers)

    # -- dispatch ----------------------------------------------------------

    def _dispatch(self, handler: Handler, request: Request) -> Response:
        """Run a matched request, wrapping idempotent writes in the ledger."""

        layer = self.plant.idempotency

        def execute() -> Response:
            return guarded(lambda: handler(self, request))

        if not layer.eligible(request):
            return execute()
        try:
            response, verdict = layer.serve(request, execute)
        except Exception as exc:
            self.plant.metrics.observe_idempotent("failed")
            return _error_response(exc)
        self.plant.metrics.observe_idempotent(verdict)
        return response

    # -- in process entry point (tests, demo) ------------------------------

    def call(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | list[object] | None = None,
        query: dict[str, list[str]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Response:
        handler, params, exists = self.router.match(method, path)
        if handler is None:
            status = 405 if exists else 404
            return error_response(status, "method not allowed" if exists else "not found")
        fields: dict[str, object] = {}
        items: tuple[object, ...] = ()
        if isinstance(payload, list):
            items = tuple(payload)
        elif payload:
            fields = payload
        request = Request(
            method=method.upper(),
            path=path,
            query=query or {},
            payload=fields,
            items=items,
            params=params,
            headers={key.lower(): value for key, value in (headers or {}).items()},
        )
        return self._dispatch(handler, request)

    # -- WSGI entry point --------------------------------------------------

    def __call__(self, environ: dict[str, object], start_response: StartResponse):
        started = time.perf_counter()
        method = str(environ.get("REQUEST_METHOD", "GET")).upper()
        path = str(environ.get("PATH_INFO", "/") or "/")
        handler, params, exists = self.router.match(method, path)
        if handler is None:
            response = error_response(
                405 if exists else 404, "method not allowed" if exists else "not found"
            )
        else:
            try:
                request = build_request(environ, params)
            except RequestError as exc:
                response = error_response(exc.status, exc.message)
            else:
                response = self._dispatch(handler, request)
        self.plant.metrics.observe(path, response.status, time.perf_counter() - started)
        status, headers = response.wsgi()
        start_response(status, headers)
        return [response.body]
