"""Small CLI for one simulated OCPP 1.6J charge point."""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import shutil
import subprocess
import sys

from ocpp_simulator.connection import run_charge_point


def build_parser():
    parser = argparse.ArgumentParser(prog="ocpp-simulator", description="Use --backend {python,go} before or after run (default: python)")
    subs = parser.add_subparsers(dest="command")
    run = subs.add_parser("run", help="Connect a simulated charge point to a CSMS")
    run.add_argument("--cp", required=True, help="Charge-point identity")
    run.add_argument("--url", required=True, help="Full ws:// or wss:// URL")
    run.add_argument("--ca", help="Trusted CA bundle for WSS certificate validation")
    run.add_argument("--basic-user", help="HTTP Basic username (defaults to --cp)")
    run.add_argument("--basic-password", help="HTTP Basic password (username is --cp; visible in process arguments)")
    run.add_argument("--vendor", default="Arthexis")
    run.add_argument("--model", default="Simulator")
    run.add_argument("--connect-timeout", type=float, default=10.0)
    run.add_argument("--connector", type=int, help="Run one charge then exit")
    run.add_argument("--remote", action="store_true", help="Listen for remote CSMS commands")
    run.add_argument("--connectors", type=int, default=1, help="Number of simulated connectors")
    run.add_argument("--rfid", default="TEST001")
    run.add_argument("--power", type=float, default=7.2, help="Charging ceiling in kW")
    run.add_argument("--battery-kwh", type=float, default=60.0, help="Battery capacity in kWh (remote mode)")
    run.add_argument("--soc", type=float, default=30.0, help="Initial battery charge percentage (remote mode)")
    run.add_argument("--seed", type=int, default=1, help="Reproducible power fluctuation seed")
    run.add_argument("--duration", type=float, default=60.0, help="Charge duration in seconds")
    run.add_argument("--meter-interval", type=float, default=1.0, help="MeterValues interval in seconds")
    subs.add_parser("help", help="Show command help")
    return parser


def _select_backend(argv):
    """Extract backend from anywhere in the CLI, preserving Python's run flags."""
    backend = "python"
    remaining = []
    iterator = iter(argv)
    for token in iterator:
        if token == "--backend":
            backend = next(iterator, None)
            if backend is None:
                raise ValueError("--backend requires python or go")
        elif token.startswith("--backend="):
            backend = token.partition("=")[2]
        else:
            remaining.append(token)
    if backend not in {"python", "go"}:
        raise ValueError(f"Unsupported backend: {backend!r} (expected python or go)")
    return backend, remaining


def _go_executable():
    override = os.environ.get("OCPP_SIMULATOR_GO_BIN")
    if override:
        path = Path(override).expanduser()
    else:
        candidate = Path(sys.executable).resolve().parent / "ocpp-simulator-go"
        path = candidate if candidate.is_file() else None
        if path is None:
            located = shutil.which("ocpp-simulator-go")
            path = Path(located) if located else None
    if path is None or not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError(
            "Go backend unavailable: install the Go binary with ./deploy.sh --with-go "
            "or set OCPP_SIMULATOR_GO_BIN to a verified executable"
        )
    return str(path)


def main(argv=None):
    try:
        backend, argv = _select_backend(list(sys.argv[1:] if argv is None else argv))
    except ValueError as exc:
        print(f"ocpp-simulator: {exc}", file=sys.stderr)
        return 2
    if backend == "go":
        if not argv or argv[0] in {"help", "-h", "--help"}:
            build_parser().print_help()
            return 0
        try:
            completed = subprocess.run([_go_executable(), *argv], check=False)
        except (OSError, RuntimeError) as exc:
            print(f"ocpp-simulator: {exc}", file=sys.stderr)
            return 1
        return completed.returncode
    return main_python(argv)


def main_python(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command in (None, "help"):
        parser.print_help()
        return 0
    try:
        asyncio.run(run_charge_point(
            cp=args.cp, url=args.url, ca=args.ca, vendor=args.vendor,
            model=args.model, connect_timeout=args.connect_timeout,
            connector=args.connector, rfid=args.rfid, power_kw=args.power,
            duration=args.duration, meter_interval=args.meter_interval,
            remote=args.remote, connectors=args.connectors,
            battery_kwh=args.battery_kwh, initial_soc=args.soc, seed=args.seed,
            basic_password=args.basic_password, basic_user=args.basic_user,
        ))
    except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
        print(f"ocpp-simulator: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
