import os
from dataclasses import dataclass, field

import yaml


@dataclass(frozen=True)
class Route:
    name: str
    url: str
    mode: str = "primary"  # primary | mirror (sent as X-Relay-Mode so receivers can stay observe-only)
    phone_number_ids: frozenset[str] = frozenset()  # empty = all events


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


def load(path: str) -> Config:
    raw = yaml.safe_load(open(path)) or {}
    providers = []
    for name, p in (raw.get("providers") or {}).items():
        routes = tuple(
            Route(
                name=r["name"],
                url=r["url"],
                mode=r.get("mode", "primary"),
                phone_number_ids=frozenset(str(x) for x in (r.get("match", {}) or {}).get("phone_number_id", [])),
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
    return Config(raw.get("db_path", "relay.db"), int(raw.get("retain_hours", 24)), tuple(providers))
