import json
from pathlib import Path
from tempfile import TemporaryDirectory

from lake import (
    correct_row, freeze, load_batch, mount_read, open_warehouse,
    read_feed, read_frozen, read_live, register_partition,
)


CASE_PATH = Path(__file__).with_name("samples") / "threshold-change.json"


def load_case():
    return json.loads(CASE_PATH.read_text(encoding="utf-8"))


def mount_image(view):
    return [view.partition_id, view.covered_through,
            [[row.row_id, row.liters] for row in view.rows]]


def delta_image(page):
    return [[entry.position, entry.fact_id, entry.partition_id, entry.delta.count,
             entry.delta.liters] for entry in page.entries]


def inspect_v1(root, case):
    with open_warehouse(root, case["old_threshold"]) as warehouse:
        for binding in case["partitions"]:
            register_partition(warehouse, *binding)
        for batch in case["batches"]:
            load_batch(warehouse, *batch)
        opening = read_feed(warehouse)
        freeze(warehouse, case["report_id"])
        first = mount_read(warehouse, case["first_mount"])

    with open_warehouse(root) as warehouse:
        receipts = [correct_row(warehouse, *edit) for edit in case["edits"]]
        second = mount_read(warehouse, case["second_mount"])
        resumed = read_feed(warehouse, opening.cursor)
        live = read_live(warehouse)
        frozen = read_frozen(warehouse, case["report_id"])

    return {
        "mounts": [mount_image(first), mount_image(second)],
        "edit_facts": [receipt.fact_id for receipt in receipts],
        "opening_deltas": delta_image(opening),
        "resumed_deltas": delta_image(resumed),
        "resumed_cursor": resumed.cursor.position,
        "live": [live.threshold, live.count, live.liters],
        "frozen": [frozen.threshold, frozen.fact_cut, frozen.count, frozen.liters],
    }


def main():
    case = load_case()
    with TemporaryDirectory(prefix="linti-threshold-fixture-") as directory:
        observed = inspect_v1(Path(directory) / "warehouse", case)
    if observed != case["v1"]:
        raise AssertionError(json.dumps({"expected": case["v1"], "actual": observed}, sort_keys=True))
    print(json.dumps(observed, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
