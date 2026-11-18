"""Operator commands: `webhook-relay replay --dead [--route NAME]`.

Uses the same configuration as the server (RELAY_CONFIG for db_path, RELAY_STORE_URL for Postgres), so run it
where the server runs, e.g. `kubectl exec deploy/webhook-relay -- webhook-relay replay --dead`.
"""
import argparse
import os
import sys

from webhook_relay.config import load
from webhook_relay.store import open_store


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="webhook-relay")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rp = sub.add_parser("replay", help="requeue dead deliveries with a fresh retry budget")
    rp.add_argument("--dead", action="store_true", required=True, help="replay rows in state 'dead'")
    rp.add_argument("--route", help="only this route (default: all routes)")
    args = ap.parse_args(argv)
    config = load(os.environ.get("RELAY_CONFIG", "/etc/relay/config.yaml"))
    store = open_store(config.db_path)
    n = store.requeue_dead(args.route)
    print(f"requeued {n} dead deliveries" + (f" for route {args.route}" if args.route else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
