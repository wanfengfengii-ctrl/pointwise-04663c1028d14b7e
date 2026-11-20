"""End-to-end demonstration of the verified threshold change workflow.

Runs the narrative of samples/threshold-change.json: build the 20 L ledger, save
the reporting cursor and freeze the old daily report, request a change to 10 L,
verify volume A, reopen the warehouse, admit the two corrections while the
request is pending, verify volume B, then publish. The downstream cursor sees a
clear threshold boundary at one feed position; later deletions are reported with
the new threshold. Old daily reports keep their frozen values.
"""
import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from lake import (
    FeedThresholdChange, correct_row, collect_threshold_partition, freeze,
    load_batch, mount_read, open_warehouse, publish_threshold, read_feed,
    read_frozen, read_live, register_partition, request_threshold,
)
from lake.feed.cursors import START_CURSOR

CASE_PATH = Path(__file__).with_name("samples") / "threshold-change.json"


def _totals(totals):
    return [totals.threshold, totals.count, totals.liters]


def _entry_image(entry):
    if isinstance(entry, FeedThresholdChange):
        return {
            "kind": "threshold_change",
            "position": entry.position,
            "request_id": entry.request_id,
            "fact_cut": entry.fact_cut,
            "before": _totals(entry.before),
            "after": _totals(entry.after),
        }
    return {
        "kind": "feed",
        "position": entry.position,
        "fact_id": entry.fact_id,
        "partition_id": entry.partition_id,
        "delta": [entry.delta.count, entry.delta.liters],
    }


def run_demo(case, root):
    # Phase 1: the fixed 20 L ledger, reporting cursor and frozen daily report.
    with open_warehouse(root, case["old_threshold"]) as warehouse:
        for binding in case["partitions"]:
            register_partition(warehouse, *binding)
        for batch in case["batches"]:
            load_batch(warehouse, *batch)
        opening = read_feed(warehouse)
        saved_cursor = opening.cursor
        frozen_before = freeze(warehouse, case["report_id"])
        first_view = mount_read(warehouse, case["first_mount"])

        # Register the change request; its partition scope is fixed right now.
        assert request_threshold(warehouse, case["request_id"], case["new_threshold"]) is None
        # Re-registering with the same parameters while pending returns None.
        assert request_threshold(warehouse, case["request_id"], case["new_threshold"]) is None
        covered_a = collect_threshold_partition(
            warehouse, case["request_id"], case["first_mount"],
        )
        # Not every volume is verified yet: publishing stays on the todo list.
        assert publish_threshold(warehouse, case["request_id"]) is None

    # Reopen across a warehouse close; verifications survive and work continues.
    with open_warehouse(root) as warehouse:
        # Intake and in-place corrections continue normally between collections.
        receipts = [correct_row(warehouse, *edit) for edit in case["edits"]]
        second_view = mount_read(warehouse, case["second_mount"])
        # The old cursor still cannot see ordinary movement (zero old-basis delta).
        assert read_feed(warehouse, saved_cursor).entries == ()
        covered_b = collect_threshold_partition(
            warehouse, case["request_id"], case["second_mount"],
        )
        change = publish_threshold(warehouse, case["request_id"])
        # Re-claiming a published request returns the original result.
        assert publish_threshold(warehouse, case["request_id"]) == change
        # Same id and same threshold re-claim the original result as well.
        assert request_threshold(
            warehouse, case["request_id"], case["new_threshold"],
        ) == change
        live_after_publish = read_live(warehouse)

        # The old cursor now meets an explicit change boundary.
        boundary_page = read_feed(warehouse, saved_cursor)
        assert len(boundary_page.entries) == 1
        boundary = boundary_page.entries[0]
        assert isinstance(boundary, FeedThresholdChange)
        assert boundary.change == change

        # A later deletion is reported under the new threshold.
        deletion = correct_row(warehouse, *case["delete_after_publish"])
        tail_page = read_feed(warehouse, boundary_page.cursor)
        tail = tail_page.entries[0]
        live_after_delete = read_live(warehouse)
        frozen_after = read_frozen(warehouse, case["report_id"])
        full_page = read_feed(warehouse, START_CURSOR)

    observed = {
        "first_mount": [first_view.partition_id, first_view.covered_through,
                        [[row.row_id, row.liters] for row in first_view.rows]],
        "second_mount": [second_view.partition_id, second_view.covered_through,
                         [[row.row_id, row.liters] for row in second_view.rows]],
        "covered_through": [covered_a, covered_b],
        "edit_facts": [receipt.fact_id for receipt in receipts],
        "fact_cut": change.fact_cut,
        "position": change.position,
        "before": _totals(change.before),
        "after": _totals(change.after),
        "after_delete": _totals(live_after_delete),
        "delete_fact": deletion.fact_id,
        "delete_delta": [tail.position, tail.fact_id, tail.partition_id,
                         tail.delta.count, tail.delta.liters],
        "frozen_report": [frozen_after.threshold, frozen_after.fact_cut,
                          frozen_after.count, frozen_after.liters],
        "feed_order": [_entry_image(entry) for entry in full_page.entries],
    }

    expected = case["v2"]
    assert observed["fact_cut"] == expected["fact_cut"]
    assert observed["position"] == expected["position"]
    assert observed["before"] == expected["before"]
    assert observed["after"] == expected["after"]
    assert observed["after_delete"] == expected["after_delete"]
    assert observed["delete_delta"] == expected["delete_delta"]
    # The old daily report is still claimed by its original id with frozen values.
    assert observed["frozen_report"] == [
        frozen_before.threshold, frozen_before.fact_cut,
        frozen_before.count, frozen_before.liters,
    ]
    return observed


def main():
    parser = argparse.ArgumentParser(description="Demonstrate the verified threshold change")
    parser.add_argument("sample", nargs="?", type=Path, default=CASE_PATH)
    args = parser.parse_args()
    with args.sample.open(encoding="utf-8") as material:
        case = json.load(material)
    with TemporaryDirectory(prefix="linti-threshold-demo-") as directory:
        observed = run_demo(case, Path(directory) / "warehouse")
    print(json.dumps(observed, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
