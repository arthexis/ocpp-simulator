"""Small CLI for one simulated OCPP 1.6J charge point."""
from __future__ import annotations

import argparse
import asyncio
import sys

from ocpp_simulator.connection import run_charge_point


def build_parser():
    parser = argparse.ArgumentParser(prog="ocpp-simulator")
    subs = parser.add_subparsers(dest="command")
    run = subs.add_parser("run", help="Connect a simulated charge point to a CSMS")
    run.add_argument("--cp", required=True, help="Charge-point identity")
    run.add_argument("--url", required=True, help="Full ws:// or wss:// URL")
    run.add_argument("--ca", help="Trusted CA bundle for WSS certificate validation")
    run.add_argument("--vendor", default="Arthexis")
    run.add_argument("--model", default="Simulator")
    run.add_argument("--connect-timeout", type=float, default=10.0)
    run.add_argument("--connector", type=int, help="Run one charge then exit")
    run.add_argument("--remote", action="store_true", help="Listen for remote CSMS commands")
    run.add_argument("--connectors", type=int, default=1, help="Number of simulated connectors")
    run.add_argument("--rfid", default="TEST001")
    run.add_argument("--power", type=float, default=7.2, help="Constant charging power in kW")
    run.add_argument("--duration", type=float, default=60.0, help="Charge duration in seconds")
    run.add_argument("--meter-interval", type=float, default=1.0, help="MeterValues interval in seconds")
    subs.add_parser("help", help="Show command help")
    return parser


def main(argv=None):
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
        ))
    except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
        print(f"ocpp-simulator: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
