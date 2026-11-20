"""Threshold change requests, volume-by-volume verification and publishing."""

from lake.cold.mounts import mount_read
from lake.corrections.commits import current_fact_cut
from lake.corrections.unit_of_work import read_transaction, write_transaction
from lake.feed.append import next_feed_position
from lake.partitions.catalog import UnknownPartition, lookup_partition
from lake.partitions.source_rows import decode_liters, encode_liters, require_liters, require_name
from lake.reports.qualification import read_threshold, write_threshold
from lake.reports.rollup import LiveTotals, replace_totals
from lake.threshold.changes import ThresholdChange

STATUS_PENDING = 'pending'
STATUS_PUBLISHED = 'published'


def init_threshold_workflow(db):
    db.execute('''
        CREATE TABLE IF NOT EXISTS threshold_changes (
            position INTEGER PRIMARY KEY,
            fact_cut INTEGER NOT NULL CHECK (fact_cut >= 0),
            old_threshold TEXT NOT NULL,
            old_count TEXT NOT NULL,
            old_liters TEXT NOT NULL,
            new_threshold TEXT NOT NULL,
            after_count TEXT NOT NULL,
            after_liters TEXT NOT NULL
        ) WITHOUT ROWID
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS threshold_requests (
            request_id TEXT PRIMARY KEY NOT NULL,
            new_threshold TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'published')),
            created_order INTEGER NOT NULL UNIQUE,
            change_position INTEGER UNIQUE REFERENCES threshold_changes (position),
            fact_cut INTEGER
        ) WITHOUT ROWID
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS threshold_request_partitions (
            request_id TEXT NOT NULL REFERENCES threshold_requests (request_id),
            partition_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
            verified INTEGER NOT NULL CHECK (verified IN (0, 1)),
            covered_through INTEGER,
            PRIMARY KEY (request_id, partition_id)
        ) WITHOUT ROWID
    ''')


def _load_request(db, request_id):
    record = db.execute('''
        SELECT request_id, new_threshold, status, created_order, change_position, fact_cut
        FROM threshold_requests WHERE request_id = ?
    ''', (request_id,)).fetchone()
    if record is None:
        return None
    return {
        'request_id': record[0],
        'new_threshold': decode_liters(record[1]),
        'status': record[2],
        'created_order': record[3],
        'change_position': record[4],
        'fact_cut': record[5],
    }


def _load_pending(db):
    records = db.execute('''
        SELECT request_id, new_threshold FROM threshold_requests WHERE status = ?
        ORDER BY created_order
    ''', (STATUS_PENDING,)).fetchall()
    return [(request_id, decode_liters(threshold)) for request_id, threshold in records]


def reject_new_partition_while_pending(db):
    if db.execute(
        "SELECT 1 FROM threshold_requests WHERE status = ? LIMIT 1", (STATUS_PENDING,)
    ).fetchone() is not None:
        raise ValueError('cannot register a partition while a threshold request is pending')


def _load_change(db, request):
    record = db.execute('''
        SELECT position, fact_cut, old_threshold, old_count, old_liters,
               new_threshold, after_count, after_liters
        FROM threshold_changes WHERE position = ?
    ''', (request['change_position'],)).fetchone()
    if record is None:
        raise RuntimeError(f'published threshold change missing for {request["request_id"]}')
    position, fact_cut, old_threshold, old_count, old_liters, new_threshold, after_count, after_liters = record
    return ThresholdChange(
        request_id=request['request_id'],
        fact_cut=fact_cut,
        position=position,
        before=LiveTotals(
            decode_liters(old_threshold), decode_liters(old_count), decode_liters(old_liters),
        ),
        after=LiveTotals(
            decode_liters(new_threshold), decode_liters(after_count), decode_liters(after_liters),
        ),
    )


def request_threshold(warehouse, request_id, new_threshold):
    require_name(request_id, 'request_id')
    require_liters(new_threshold)
    db = warehouse.db
    with write_transaction(db):
        existing = _load_request(db, request_id)
        if existing is not None:
            if existing['new_threshold'] != new_threshold:
                raise ValueError(
                    f'threshold request {request_id} already registered with another threshold'
                )
            if existing['status'] == STATUS_PUBLISHED:
                return _load_change(db, existing)
            return None

        pending = _load_pending(db)
        if pending:
            raise ValueError(f'another threshold request is pending: {pending[0][0]}')

        order = db.execute(
            'SELECT COALESCE(MAX(created_order), 0) + 1 FROM threshold_requests'
        ).fetchone()[0]
        db.execute('''
            INSERT INTO threshold_requests
                (request_id, new_threshold, status, created_order, change_position, fact_cut)
            VALUES (?, ?, ?, ?, NULL, NULL)
        ''', (request_id, encode_liters(new_threshold), STATUS_PENDING, order))
        partitions = db.execute(
            'SELECT partition_id FROM partitions ORDER BY partition_id'
        ).fetchall()
        for ordinal, (partition_id,) in enumerate(partitions):
            db.execute('''
                INSERT INTO threshold_request_partitions
                    (request_id, partition_id, ordinal, verified, covered_through)
                VALUES (?, ?, ?, 0, NULL)
            ''', (request_id, partition_id, ordinal))
        return None


def _request_partition(db, request_id, partition_id):
    record = db.execute('''
        SELECT verified, covered_through FROM threshold_request_partitions
        WHERE request_id = ? AND partition_id = ?
    ''', (request_id, partition_id)).fetchone()
    if record is None:
        raise UnknownPartition(f'partition {partition_id} is not part of request {request_id}')
    verified, covered_through = record
    return bool(verified), covered_through


def collect_threshold_partition(warehouse, request_id, partition_id):
    require_name(request_id, 'request_id')
    require_name(partition_id, 'partition_id')
    db = warehouse.db
    with read_transaction(db):
        request = _load_request(db, request_id)
        if request is None:
            raise LookupError(f'unknown threshold request: {request_id}')
        verified, covered_through = _request_partition(db, request_id, partition_id)
        if verified:
            return covered_through

    # The exclusive mount reads the actual volume rows and facts. Mount failures keep
    # their original exceptions and leave no partial verification behind.
    view = mount_read(warehouse, partition_id)
    with write_transaction(db):
        request = _load_request(db, request_id)
        if request is None:
            raise LookupError(f'unknown threshold request: {request_id}')
        verified, covered_through = _request_partition(db, request_id, partition_id)
        if verified:
            return covered_through
        db.execute('''
            UPDATE threshold_request_partitions
            SET verified = 1, covered_through = ?
            WHERE request_id = ? AND partition_id = ? AND verified = 0
        ''', (view.covered_through, request_id, partition_id))
        return view.covered_through


def _all_verified(db, request_id):
    record = db.execute('''
                SELECT COUNT(*), COALESCE(SUM(verified), 0)
                FROM threshold_request_partitions WHERE request_id = ?
            ''', (request_id,)).fetchone()
    return record[0] == record[1]


def publish_threshold(warehouse, request_id):
    require_name(request_id, 'request_id')
    db = warehouse.db
    with read_transaction(db):
        request = _load_request(db, request_id)
        if request is None:
            raise LookupError(f'unknown threshold request: {request_id}')
        if request['status'] == STATUS_PUBLISHED:
            return _load_change(db, request)
        if not _all_verified(db, request_id):
            return None
        partition_ids = [
            row[0] for row in db.execute('''
                SELECT partition_id FROM threshold_request_partitions
                WHERE request_id = ? ORDER BY ordinal
            ''', (request_id,))
        ]

    old_threshold = read_threshold(db)
    new_threshold = request['new_threshold']

    with write_transaction(db, 'publish_threshold', warehouse.before_commit):
        request = _load_request(db, request_id)
        if request['status'] == STATUS_PUBLISHED:
            return _load_change(db, request)
        if not _all_verified(db, request_id):
            return None

        fact_cut = current_fact_cut(db)
        old_count_total = old_liters_total = 0
        new_count_total = new_liters_total = 0
        for partition_id in partition_ids:
            binding = lookup_partition(db, partition_id)
            old_count, old_liters, new_count, new_liters = (
                warehouse.volumes.summarize_volume(
                    binding, old_threshold, new_threshold,
                )
            )
            replace_totals(db, partition_id, new_count, new_liters)
            old_count_total += old_count
            old_liters_total += old_liters
            new_count_total += new_count
            new_liters_total += new_liters

        before = LiveTotals(old_threshold, old_count_total, old_liters_total)
        after = LiveTotals(new_threshold, new_count_total, new_liters_total)
        position = next_feed_position(db)
        db.execute('''
            INSERT INTO threshold_changes
                (position, fact_cut, old_threshold, old_count, old_liters,
                 new_threshold, after_count, after_liters)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            position, fact_cut,
            encode_liters(before.threshold), encode_liters(before.count), encode_liters(before.liters),
            encode_liters(after.threshold), encode_liters(after.count), encode_liters(after.liters),
        ))
        write_threshold(db, new_threshold)
        db.execute('''
            UPDATE threshold_requests
            SET status = ?, change_position = ?, fact_cut = ?
            WHERE request_id = ? AND status = ?
        ''', (STATUS_PUBLISHED, position, fact_cut, request_id, STATUS_PENDING))
        return ThresholdChange(
            request_id=request_id,
            fact_cut=fact_cut,
            position=position,
            before=before,
            after=after,
        )
