#!/usr/bin/env python3
"""Are all runs of a scaling matrix valid (ended, right digest)? Exit code 1 if not. Used by scripts/run-final.sh.

Usage: python3 scripts/check_matrix.py bench/results/scaling_matrix/<time>-<name>
"""
import json
import sys
from pathlib import Path

d = Path(sys.argv[1])
info, summary = (json.loads((d / f).read_text()) for f in ("info.json", "summary.json"))
bad = [f"{c['spec']}: {c['valid']} of {info['repeats']} runs valid" for c in summary["configs"] if c["valid"] < info["repeats"]]
print("\n".join(bad) or f"all runs valid, digest {summary['digest']}")
sys.exit(bool(bad))
