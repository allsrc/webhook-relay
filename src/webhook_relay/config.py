import os
from dataclasses import dataclass, field

import yaml


@dataclass(frozen=True)
class Route:
    name: str
    url: str
    mode: str = "primary"  # primary | mirror (sent as X-Relay-Mode so receivers can stay observe-only)
    phone_number_ids: frozenset[str] = frozenset()  # empty = all events
    # Optional per-route tuning; None = default for the mode (see delivery.RouteSettings).
    connect_timeout: float | None = None  # seconds to establish the TCP connection
    timeout: float | None = None  # seconds for each write / read of the response headers
    workers: int | None = None  # concurrent deliveries to this route
    breaker_failures: int | None = None  # consecutive connect failures that open the circuit (0 = disabled)
    breaker_cooldown: float | None = None  # seconds before a half-open probe


@dataclass(frozen=True)
class Provider:
    name: str
    path: str
    verify_token: str
    app_secret: str  # empty = signatures not verified here
    routes: tuple[Route, ...] = ()


@dataclass(frozen=True)
class Config:
    db_path: str = "relay.db"
    retain_hours: int = 24
    providers: tuple[Provider, ...] = field(default_factory=tuple)
    lag_warn_seconds: float = 30.0


def _opt(r: dict, key: str, cast):
    return cast(r[key]) if r.get(key) is not None else None


def load(path: str) -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    providers = []
    for name, p in (raw.get("providers") or {}).items():
        routes = tuple(
            Route(
                name=r["name"],
                url=r["url"],
                mode=r.get("mode", "primary"),
                phone_number_ids=frozenset(str(x) for x in (r.get("match", {}) or {}).get("phone_number_id", [])),
                connect_timeout=_opt(r, "connect_timeout", float),
                timeout=_opt(r, "timeout", float),
                workers=_opt(r, "workers", int),
                breaker_failures=_opt(r, "breaker_failures", int),
                breaker_cooldown=_opt(r, "breaker_cooldown", float),
            )
            for r in p.get("routes", [])
        )
        providers.append(
            Provider(
                name=name,
                path=p.get("path", f"/{name}"),
                verify_token=os.environ.get(p.get("verify_token_env", ""), ""),
                app_secret=os.environ.get(p.get("app_secret_env", ""), ""),
                routes=routes,
            )
        )
    return Config(
        raw.get("db_path", "relay.db"),
        int(raw.get("retain_hours", 24)),
        tuple(providers),
        float(raw.get("lag_warn_seconds", 30)),
    )
