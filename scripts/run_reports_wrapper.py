#!/usr/bin/env python3
import sys
import os
import importlib.util
from pathlib import Path

"""
Wrapper that runs your report logic against export.csv and writes:
  reports/reopens_by_user.csv
  reports/reopens_by_ticket.csv

It tries, in order:
  1) reports.process(input_csv, out_user_csv, out_ticket_csv)
  2) reports.main(input_csv, out_user_csv, out_ticket_csv)

Usage:
  python scripts/run_reports_wrapper.py export.csv
"""

def import_reports_module():
  repo_dir = Path(__file__).resolve().parent
  mod_path = repo_dir / "reports.py"
  if not mod_path.exists():
    print("ERROR: scripts/reports.py not found. Add your Python logic there.", file=sys.stderr)
    sys.exit(1)
  spec = importlib.util.spec_from_file_location("reports", mod_path)
  mod = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(mod)
  return mod

def main():
  if len(sys.argv) < 2:
    print("Usage: python scripts/run_reports_wrapper.py export.csv", file=sys.stderr)
    sys.exit(2)

  input_csv = sys.argv[1]
  if not os.path.exists(input_csv):
    print(f"Input not found: {input_csv}", file=sys.stderr)
    sys.exit(1)

  os.makedirs("reports", exist_ok=True)
  out_user = "reports/reopens_by_user.csv"
  out_ticket = "reports/reopens_by_ticket.csv"

  mod = import_reports_module()

  if hasattr(mod, "process"):
    mod.process(input_csv, out_user, out_ticket)
  elif hasattr(mod, "main"):
    mod.main(input_csv, out_user, out_ticket)
  else:
    print("ERROR: scripts/reports.py must define process(...) or main(...)", file=sys.stderr)
    sys.exit(1)

  # sanity check
  ok = True
  if not os.path.exists(out_user):
    print("WARN: missing reports/reopens_by_user.csv", file=sys.stderr); ok = False
  if not os.path.exists(out_ticket):
    print("WARN: missing reports/reopens_by_ticket.csv", file=sys.stderr); ok = False

  if not ok:
    sys.exit(1)

  print(f"✅ Generated:\n  {out_user}\n  {out_ticket}")

if __name__ == "__main__":
  main()
