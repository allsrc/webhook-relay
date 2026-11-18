import httpx
from fastapi.testclient import TestClient

from webhook_relay import cli
from webhook_relay.app import create_app
from webhook_relay.config import Config, Route, load
from webhook_relay.delivery import Deliverer
from webhook_relay.store import Store, open_store

P = "whatsapp"


def test_defaults_unchanged(monkeypatch, tmp_path):
    for k in ("RELAY_MAX_ATTEMPTS", "RELAY_BACKOFF", "RELAY_STORE_URL", "RELAY_DEAD_RETAIN_HOURS"):
        monkeypatch.delenv(k, raising=False)
    f = tmp_path / "c.yaml"
    f.write_text("providers: {}\n")
    c = load(str(f))
    assert (c.max_attempts, c.backoff, c.dead_retain_hours) == ((), (), None)
    d = Deliverer(Store(":memory:"))
    assert d.max_attempts == {"primary": 8, "mirror": 3} and d.backoff[:3] == [2, 10, 30]
    assert isinstance(open_store(":memory:"), Store)


def test_retry_budget_from_env(monkeypatch, tmp_path):
    f = tmp_path / "c.yaml"
    f.write_text("providers: {}\n")
    monkeypatch.setenv("RELAY_MAX_ATTEMPTS", "primary=2,mirror=1")
    monkeypatch.setenv("RELAY_BACKOFF", "1,5")
    c = load(str(f))
    store = Store(":memory:")
    d = Deliverer(store, httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500))),
                  max_attempts=dict(c.max_attempts), backoff=list(c.backoff))
    r = Route("prod", "http://p/h")
    i = store.enqueue(P, r.name, r.url, r.mode, b"x", {}, now=0.0)
    d.run_once(0.0)
    assert store.get(i)["status"] == "pending" and store.get(i)["next_at"] == 1.0
    d.run_once(1.0)
    assert store.get(i)["status"] == "dead"


def test_replay_cli(monkeypatch, tmp_path, capsys):
    db = tmp_path / "r.db"
    f = tmp_path / "c.yaml"
    f.write_text(f"db_path: {db.as_posix()}\nproviders: {{}}\n")
    monkeypatch.setenv("RELAY_CONFIG", str(f))
    monkeypatch.delenv("RELAY_STORE_URL", raising=False)
    s = Store(str(db))
    a = s.enqueue(P, "prod", "u", "primary", b"x", {}, now=1.0)
    b = s.enqueue(P, "laptop", "u", "mirror", b"y", {}, now=1.0)
    s.mark_dead(a, 8, 2.0, "e")
    s.mark_dead(b, 3, 2.0, "e")
    assert cli.main(["replay", "--dead", "--route", "prod"]) == 0
    assert "requeued 1" in capsys.readouterr().out
    assert s.get(a)["status"] == "pending" and s.get(a)["attempts"] == 0 and s.get(b)["status"] == "dead"


def test_readyz_and_livez():
    app = create_app(Config(), Store(":memory:"))
    c = TestClient(app)
    assert c.get("/readyz").status_code == 200 and c.get("/livez").status_code == 200
    app.state.store.ping = lambda: (_ for _ in ()).throw(RuntimeError("down"))
    assert c.get("/readyz").status_code == 503 and c.get("/livez").status_code == 200
