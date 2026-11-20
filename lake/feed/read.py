from lake.corrections.unit_of_work import read_transaction
from lake.feed.append import FeedEntry, latest_feed_position
from lake.feed.cursors import resolve_cursor
from lake.feed.format import format_feed_page
from lake.partitions.source_rows import decode_liters
from lake.reports.deltas import ReportDelta
from lake.reports.rollup import LiveTotals
from lake.threshold.changes import FeedThresholdChange, ThresholdChange


def _load_change(db, position):
    record = db.execute('''
        SELECT r.request_id, c.fact_cut, c.old_threshold, c.old_count, c.old_liters,
               c.new_threshold, c.after_count, c.after_liters
        FROM threshold_changes AS c
        JOIN threshold_requests AS r ON r.change_position = c.position
        WHERE c.position = ?
    ''', (position,)).fetchone()
    (request_id, fact_cut, old_threshold, old_count, old_liters,
     new_threshold, after_count, after_liters) = record
    return ThresholdChange(
        request_id=request_id,
        fact_cut=fact_cut,
        position=position,
        before=LiveTotals(
            decode_liters(old_threshold), decode_liters(old_count), decode_liters(old_liters),
        ),
        after=LiveTotals(
            decode_liters(new_threshold), decode_liters(after_count), decode_liters(after_liters),
        ),
    )


def read_feed(warehouse, cursor=None):
    db = warehouse.db
    with read_transaction(db):
        cursor = resolve_cursor(cursor, latest_feed_position(db))
        records = db.execute('''
            SELECT kind, position, fact_id, partition_id, count_value, liters_value
            FROM (
                SELECT 'feed' AS kind, e.position AS position, e.fact_id AS fact_id,
                       f.partition_id AS partition_id, e.count_delta AS count_value,
                       e.liters_delta AS liters_value
                FROM feed_entries AS e JOIN fact_commits AS f USING (fact_id)
                UNION ALL
                SELECT 'change' AS kind, position, NULL, NULL, after_count, after_liters
                FROM threshold_changes
            )
            WHERE position > ? ORDER BY position
        ''', (cursor.position,))
        entries = []
        for kind, position, fact_id, partition_id, count_value, liters_value in records:
            if kind == 'feed':
                entries.append(FeedEntry(
                    position, fact_id, partition_id,
                    ReportDelta(decode_liters(count_value), decode_liters(liters_value)),
                ))
            else:
                entries.append(FeedThresholdChange(_load_change(db, position)))
        return format_feed_page(entries, cursor)
