import csv
from pathlib import Path


def read_sales(directory="/data"):
    files = sorted(Path(directory).glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files in {directory}")
    for path in files:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for number, row in enumerate(csv.DictReader(stream), start=1):
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"Invalid CSV record: {path.name}:{number}")
                yield {
                    **{key: value if value != "" else None for key, value in row.items()},
                    "source_file": path.name,
                    "source_row": number,
                    "event_id": f"{path.name}:{number}",
                }
