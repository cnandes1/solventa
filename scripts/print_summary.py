#!/usr/bin/env python3
import csv
from pathlib import Path


path = Path(__file__).resolve().parents[1] / "results" / "acceptance_matrix.csv"
with path.open(newline="") as handle:
    rows = list(csv.DictReader(handle))

widths = {
    field: max(len(field), *(len(row[field]) for row in rows))
    for field in ("scenario", "metric", "result", "status")
}
header = "  ".join(field.upper().ljust(widths[field]) for field in widths)
print(header)
print("  ".join("-" * widths[field] for field in widths))
for row in rows:
    print("  ".join(row[field].ljust(widths[field]) for field in widths))
