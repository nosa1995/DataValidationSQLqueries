from pathlib import Path
import csv

SOURCE_DIR = Path("source")
TARGET_DIR = Path("target")
VALIDATION_DIR = Path("validation")

VALIDATION_DIR.mkdir(exist_ok=True)

def load_hashes(csv_path):
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        return {row[0] for row in reader if row}

for source_csv in SOURCE_DIR.glob("*.csv"):
    target_csv = TARGET_DIR / source_csv.name

    if not target_csv.exists():
        print(f"Missing target file for {source_csv.name}")
        continue

    source_hashes = load_hashes(source_csv)
    target_hashes = load_hashes(target_csv)

    missing_in_target = source_hashes - target_hashes
    extra_in_target   = target_hashes - source_hashes

    report_path = VALIDATION_DIR / f"{source_csv.stem}_validation.csv"

    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["category", "row_hash"])

        for h in missing_in_target:
            writer.writerow(["missing_in_target", h])

        for h in extra_in_target:
            writer.writerow(["extra_in_target", h])

    status = "PASS" if not missing_in_target and not extra_in_target else "FAIL"
    print(f"{source_csv.name}: {status}")
