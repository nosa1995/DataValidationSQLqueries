from pathlib import Path
import csv

SOURCE_DIR = Path("source")
TARGET_DIR = Path("target")
VALIDATION_DIR = Path("validation")

VALIDATION_DIR.mkdir(exist_ok=True)

FILENAME = "QA-DataValueQualityCheck.csv"  # compares this file in source vs target


def load_table_hashes(csv_path: Path):
    """
    Returns dict keyed by TableName with value:
      {
        "table_value_hash": <raw string>
      }
    """
    results = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return results

        # Normalize header names for safety (case/space tolerant)
        headers = {h.strip(): h for h in reader.fieldnames}

        # Required columns
        required = ["TableName", "table_value_hash"]
        missing_cols = [c for c in required if c not in headers]
        if missing_cols:
            raise ValueError(
                f"{csv_path.name} is missing columns: {missing_cols}. Found: {reader.fieldnames}"
            )

        for row in reader:
            table = (row.get(headers["TableName"]) or "").strip()
            if not table:
                continue

            results[table] = {
                "table_value_hash": (row.get(headers["table_value_hash"]) or "").strip()
            }
    return results


source_csv = SOURCE_DIR / FILENAME
target_csv = TARGET_DIR / FILENAME

if not source_csv.exists():
    raise FileNotFoundError(f"Missing source file: {source_csv}")
if not target_csv.exists():
    raise FileNotFoundError(f"Missing target file: {target_csv}")

source_map = load_table_hashes(source_csv)
target_map = load_table_hashes(target_csv)

all_tables = sorted(set(source_map) | set(target_map))

report_path = VALIDATION_DIR / "QA-DataValueQualityCheck_validation.csv"
summary_path = VALIDATION_DIR / "QA-DataValueQualityCheck_summary.csv"

mismatches = 0
missing_in_target = 0
extra_in_target = 0
matches = 0

with report_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "category",
        "TableName",
        "source_table_value_hash",
        "target_table_value_hash",
    ])

    for table in all_tables:
        src = source_map.get(table)
        tgt = target_map.get(table)

        if src is None:
            extra_in_target += 1
            writer.writerow([
                "extra_in_target",
                table,
                "",
                tgt["table_value_hash"],
            ])
            continue

        if tgt is None:
            missing_in_target += 1
            writer.writerow([
                "missing_in_target",
                table,
                src["table_value_hash"],
                "",
            ])
            continue

        hash_match = (src["table_value_hash"] == tgt["table_value_hash"])

        if hash_match:
            matches += 1
        else:
            mismatches += 1
            writer.writerow([
                "hash_mismatch",
                table,
                src["table_value_hash"],
                tgt["table_value_hash"],
            ])

# Write a small summary file for quick reporting
with summary_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["metric", "value"])
    writer.writerow(["file", FILENAME])
    writer.writerow(["tables_compared", len(all_tables)])
    writer.writerow(["matches", matches])
    writer.writerow(["hash_mismatches", mismatches])
    writer.writerow(["missing_in_target", missing_in_target])
    writer.writerow(["extra_in_target", extra_in_target])

status = "PASS" if mismatches == 0 and missing_in_target == 0 and extra_in_target == 0 else "FAIL"
print(f"{FILENAME}: {status}")
print(f"Report: {report_path}")
print(f"Summary: {summary_path}")
