from dataclasses import dataclass

from lake.partitions.source_rows import encode_liters
from lake.reports.deltas import ReportDelta


@dataclass(frozen=True, slots=True)
class FeedEntry:
    position: int
    fact_id: int
    partition_id: str
    delta: ReportDelta


def init_feed(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS feed_entries (
            position INTEGER PRIMARY KEY,
            fact_id INTEGER NOT NULL UNIQUE REFERENCES fact_commits (fact_id),
            count_delta TEXT NOT NULL,
            liters_delta TEXT NOT NULL
        )
    ''')
    # Normal feed deltas and threshold changes share one reporting position order.
    db.execute('''
        CREATE TABLE IF NOT EXISTS reporting_sequence (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            last_position INTEGER NOT NULL CHECK (last_position >= 0)
        )
    ''')
    db.execute('''
        INSERT OR IGNORE INTO reporting_sequence (singleton, last_position)
        SELECT 1, COALESCE(MAX(position), 0) FROM feed_entries
    ''')


def latest_feed_position(db):
    record = db.execute(
        'SELECT last_position FROM reporting_sequence WHERE singleton = 1'
    ).fetchone()
    if record is None:
        raise RuntimeError('reporting sequence is missing')
    return record[0]


def next_feed_position(db):
    result = db.execute(
        'UPDATE reporting_sequence SET last_position = last_position + 1 WHERE singleton = 1'
    )
    if result.rowcount != 1:
        raise RuntimeError('reporting sequence is missing')
    return latest_feed_position(db)


def append_delta(db, fact_id, delta):
    if delta.is_zero():
        return
    position = next_feed_position(db)
    db.execute('''
        INSERT INTO feed_entries (position, fact_id, count_delta, liters_delta)
        VALUES (?, ?, ?, ?)
    ''', (position, fact_id, encode_liters(delta.count), encode_liters(delta.liters)))
