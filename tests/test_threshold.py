from lake import (
    FeedCursor, FeedThresholdChange, MountBusy, MountReadError, ThresholdChange,
    UnknownPartition, collect_threshold_partition, correct_row, freeze, load_batch,
    mount_read, open_warehouse, publish_threshold, read_feed, read_frozen, read_live,
    register_partition, request_threshold,
)
from tests import InjectedFailure, ShiftCase, deltas, readings, totals


def change_from_page(page):
    entries = page.entries
    assert len(entries) == 1
    assert isinstance(entries[0], FeedThresholdChange)
    return entries[0].change


class TestThresholdRequests(ShiftCase):
    def test_request_fixes_partition_scope_and_replays(self):
        w = self.warehouse
        load_batch(w, 'A', 'b-a', [('x', 25)])
        self.assertIsNone(request_threshold(w, 'req', 10))
        # Same id and same threshold while pending re-claims None.
        self.assertIsNone(request_threshold(w, 'req', 10))
        # New partitions cannot join while a request is pending.
        with self.assertRaises(ValueError):
            register_partition(w, 'C', 'volume-C')
        # Re-registering an already bound partition is still allowed.
        self.assertEqual(register_partition(w, 'B', 'volume-B'), self.b)
        # A different request id is refused while one is pending.
        with self.assertRaises(ValueError):
            request_threshold(w, 'other', 10)
        # Same id with a different threshold is refused.
        with self.assertRaises(ValueError):
            request_threshold(w, 'req', 11)
        with self.assertRaises(ValueError):
            request_threshold(w, 'req', -1)
        with self.assertRaises(ValueError):
            request_threshold(w, ' req', 10)

    def test_invalid_threshold_values(self):
        w = self.warehouse
        for invalid in (-1, True, 1.5, None, '10'):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    request_threshold(w, 'req', invalid)

    def test_unknown_request_and_partition_lookup(self):
        w = self.warehouse
        with self.assertRaises(LookupError):
            collect_threshold_partition(w, 'missing', 'A')
        with self.assertRaises(LookupError):
            publish_threshold(w, 'missing')
        request_threshold(w, 'req', 10)
        with self.assertRaises(UnknownPartition):
            collect_threshold_partition(w, 'req', 'C')

    def test_empty_request_publishes_immediately(self):
        w = open_warehouse(self.root / 'empty', 20)
        self.addCleanup(w.close)
        self.assertIsNone(request_threshold(w, 'req', 10))
        change = publish_threshold(w, 'req')
        self.assertEqual(
            (change.request_id, change.fact_cut, change.position), ('req', 0, 1),
        )
        self.assertEqual(_totals(change.before), (20, 0, 0))
        self.assertEqual(_totals(change.after), (10, 0, 0))
        self.assertEqual(read_live(w).threshold, 10)


def _totals(values):
    return values.threshold, values.count, values.liters


class TestThresholdCollect(ShiftCase):
    def test_collect_uses_mount_and_records_coverage(self):
        w = self.warehouse
        batch = load_batch(w, 'A', 'b-a', [('x', 25)])
        request_threshold(w, 'req', 10)
        covered = collect_threshold_partition(w, 'req', 'A')
        self.assertEqual(covered, batch.fact_id)
        # Re-collecting returns the stored coverage without a fresh mount.
        self.assertEqual(collect_threshold_partition(w, 'req', 'A'), batch.fact_id)
        # Empty volumes must be verified too.
        self.assertEqual(collect_threshold_partition(w, 'req', 'B'), 0)
        change = publish_threshold(w, 'req')
        self.assertIsInstance(change, ThresholdChange)
        # After publish the verification can still be re-claimed.
        self.assertEqual(collect_threshold_partition(w, 'req', 'A'), batch.fact_id)

    def test_mount_busy_and_read_failures_are_retryable(self):
        w = self.warehouse
        load_batch(w, 'A', 'b-a', [('x', 25), ('y', 30)])
        request_threshold(w, 'req', 10)

        def fail(partition, row):
            if partition == 'A' and row.row_id == 'y':
                raise InjectedFailure('cold read')

        self.on_row = fail
        with self.assertRaises(MountReadError):
            collect_threshold_partition(w, 'req', 'A')
        self.on_row = None
        # The failed verification left nothing behind and can be retried.
        self.assertEqual(collect_threshold_partition(w, 'req', 'A'), 1)

        # While one session holds the exclusive mount, a second session on the
        # same warehouse is blocked by the shared mount gate.
        other = open_warehouse(self.root)
        self.addCleanup(other.close)

        def expect_busy(partition, row):
            with self.assertRaises(MountBusy):
                collect_threshold_partition(other, 'req', 'B')

        self.on_row = expect_busy
        collect_threshold_partition(w, 'req', 'B')
        self.on_row = None

    def test_verification_survives_reopen_and_publish_returns_none_until_complete(self):
        w = self.warehouse
        load_batch(w, 'A', 'b-a', [('x', 25)])
        load_batch(w, 'B', 'b-b', [('y', 8)])
        request_threshold(w, 'req', 10)
        collect_threshold_partition(w, 'req', 'A')
        self.assertIsNone(publish_threshold(w, 'req'))
        w = self.reopen()
        self.assertIsNone(publish_threshold(w, 'req'))
        self.assertEqual(collect_threshold_partition(w, 'req', 'B'), 2)
        change = publish_threshold(w, 'req')
        self.assertEqual((change.fact_cut, change.position), (2, 2))


class TestThresholdPublish(ShiftCase):
    def _prepare(self, new_threshold=10):
        w = self.warehouse
        load_batch(w, 'A', 'b-a', [('low', 8), ('high', 25)])
        load_batch(w, 'B', 'b-b', [('mid', 13), ('top', 25)])
        request_threshold(w, 'req', new_threshold)
        collect_threshold_partition(w, 'req', 'A')
        collect_threshold_partition(w, 'req', 'B')
        return w

    def test_publish_recomputes_same_cut_including_zero_old_delta_facts(self):
        w = self._prepare()
        opening = read_feed(w)
        self.assertEqual(deltas(opening), ((1, 25), (1, 25)))
        # A correction with zero old-basis delta but a real new-basis effect.
        edit = correct_row(w, 'edit-a', 'A', 'low', 8, 12)
        self.assertEqual(read_feed(w, opening.cursor).entries, ())
        change = publish_threshold(w, 'req')
        # Fact cut includes the correction even though it never hit the old feed.
        self.assertEqual(change.fact_cut, edit.fact_id)
        # Old basis at the same cut: only 25 L rows qualify (one per partition).
        self.assertEqual(_totals(change.before), (20, 2, 50))
        # New basis: 12, 25 in A and 13, 25 in B.
        self.assertEqual(_totals(change.after), (10, 4, 75))
        self.assertEqual(totals(read_live(w)), (4, 75))
        self.assertEqual(read_live(w).threshold, 10)
        self.assertEqual(totals(read_live(w, 'A')), (2, 37))
        # Rollups for every fixed partition are aligned to the new basis.
        self.assertEqual(totals(read_live(w, 'B')), (2, 38))

    def test_publish_takes_one_feed_position_and_cursor_resumes_after(self):
        w = self._prepare()
        opening = read_feed(w)
        self.assertEqual(deltas(opening), ((1, 25), (1, 25)))
        change = publish_threshold(w, 'req')
        self.assertEqual(change.position, opening.cursor.position + 1)
        page = read_feed(w, opening.cursor)
        delivered = change_from_page(page)
        self.assertEqual(delivered, change)
        # A subsequent ordinary entry is interpreted under the new threshold.
        edit = correct_row(w, 'drop', 'A', 'high', 25, None)
        tail = read_feed(w, page.cursor)
        self.assertEqual(deltas(tail), ((-1, -25),))
        self.assertEqual(tail.entries[0].fact_id, edit.fact_id)
        # The full feed keeps the original entries' positions, values and order.
        full = read_feed(w)
        self.assertEqual([entry.position for entry in full.entries], [1, 2, 3, 4])
        self.assertIsInstance(full.entries[2], FeedThresholdChange)
        # Even an equal-value change must be delivered as its own entry.
        w2 = open_warehouse(self.root / 'equal', 20)
        self.addCleanup(w2.close)
        request_threshold(w2, 'same', 20)
        equal_change = publish_threshold(w2, 'same')
        self.assertEqual(equal_change.before, equal_change.after)
        self.assertEqual(
            [type(entry).__name__ for entry in read_feed(w2).entries],
            ['FeedThresholdChange'],
        )

    def test_failed_publish_keeps_ledger_verifications_and_facts(self):
        w = self._prepare()
        edit = correct_row(w, 'meanwhile', 'B', 'mid', 13, 9)
        opening = read_feed(w)
        self.fail_operation = 'publish_threshold'
        with self.assertRaises(InjectedFailure):
            publish_threshold(w, 'req')
        self.fail_operation = None
        # Original threshold and ledger survive.
        self.assertEqual(read_live(w).threshold, 20)
        self.assertEqual(totals(read_live(w)), (2, 50))
        self.assertEqual(read_feed(w), opening)
        # The request is still pending, verifications retained.
        w = self.reopen()
        change = publish_threshold(w, 'req')
        self.assertEqual(change.fact_cut, edit.fact_id)
        self.assertEqual(_totals(change.before), (20, 2, 50))
        # New basis at the same cut: 25 in A, 25 in B (the 9 L row does not qualify).
        self.assertEqual(_totals(change.after), (10, 2, 50))

    def test_replay_after_publish_and_next_request(self):
        w = self._prepare()
        change = publish_threshold(w, 'req')
        self.assertEqual(publish_threshold(w, 'req'), change)
        self.assertEqual(request_threshold(w, 'req', 10), change)
        # Same id, different threshold is still rejected after publishing.
        with self.assertRaises(ValueError):
            request_threshold(w, 'req', 11)
        # After a completed publish the next request can be registered, again
        # fixing the currently known partitions.
        self.assertIsNone(request_threshold(w, 'req-2', 5))
        # New partitions cannot join while req-2 is pending.
        with self.assertRaises(ValueError):
            register_partition(w, 'C', 'volume-C')
        collect_threshold_partition(w, 'req-2', 'A')
        collect_threshold_partition(w, 'req-2', 'B')
        second = publish_threshold(w, 'req-2')
        self.assertEqual(second.position, change.position + 1)
        page = read_feed(w, FeedCursor(change.position))
        self.assertEqual(change_from_page(page), second)
        # Once req-2 completes, new partitions are accepted again.
        binding = register_partition(w, 'C', 'volume-C')
        self.assertEqual(binding.partition_id, 'C')

    def test_frozen_reports_keep_old_basis_new_one_uses_current(self):
        w = self._prepare()
        old_report = freeze(w, 'old-report')
        change = publish_threshold(w, 'req')
        new_report = freeze(w, 'new-report')
        self.assertEqual(read_frozen(w, 'old-report'), old_report)
        self.assertEqual(old_report.threshold, 20)
        self.assertEqual((new_report.threshold, new_report.fact_cut), (10, change.fact_cut))
        self.assertEqual((new_report.count, new_report.liters), (change.after.count, change.after.liters))

    def test_reopen_threshold_rules(self):
        w = self._prepare()
        publish_threshold(w, 'req')
        w.close()
        # Omitting the threshold reopens with the threshold now in use.
        w = self.reopen()
        self.assertEqual(read_live(w).threshold, 10)
        w.close()
        with self.assertRaises(ValueError):
            open_warehouse(self.root, threshold=20)
        w = self.reopen(10)
        self.assertEqual(read_live(w).threshold, 10)

    def test_zero_liter_row_and_deletion_at_new_threshold(self):
        w = open_warehouse(self.root / 'zero', 0)
        self.addCleanup(w.close)
        register_partition(w, 'Z', 'vol-z')
        load_batch(w, 'Z', 'b', [('z', 0)])
        request_threshold(w, 'req', 10)
        collect_threshold_partition(w, 'req', 'Z')
        change = publish_threshold(w, 'req')
        self.assertEqual(_totals(change.before), (0, 1, 0))
        self.assertEqual(_totals(change.after), (10, 0, 0))
        deletion = correct_row(w, 'del', 'Z', 'z', 0, None)
        page = read_feed(w, FeedCursor(change.position))
        self.assertEqual(deltas(page), ())
        self.assertEqual(totals(read_live(w)), (0, 0))
        self.assertEqual(deletion.fact_id, change.fact_cut + 1)
        self.assertEqual(readings(mount_read(w, 'Z')), ())

    def test_empty_registered_partition_rollup_is_aligned_at_publish(self):
        w = self.warehouse
        # B has never held data; its rollup row is missing until publish aligns it.
        load_batch(w, 'A', 'b-a', [('x', 25)])
        request_threshold(w, 'req', 10)
        collect_threshold_partition(w, 'req', 'A')
        self.assertEqual(collect_threshold_partition(w, 'req', 'B'), 0)
        publish_threshold(w, 'req')
        self.assertEqual(totals(read_live(w, 'B')), (0, 0))
        self.assertEqual(totals(read_live(w)), (1, 25))

    def test_intervening_facts_between_verifications_land_on_publish_cut(self):
        w = self.warehouse
        load_batch(w, 'A', 'b-a', [('x', 8)])
        load_batch(w, 'B', 'b-b', [('y', 13)])
        request_threshold(w, 'req', 10)
        covered_a = collect_threshold_partition(w, 'req', 'A')
        self.assertEqual(covered_a, 1)
        # Close and reopen; meanwhile intake continues normally across sessions.
        w = self.reopen()
        edit = correct_row(w, 'edit-a', 'A', 'x', 8, 12)
        intake = load_batch(w, 'A', 'b-a-2', [('z', 25)])
        covered_b = collect_threshold_partition(w, 'req', 'B')
        self.assertEqual(covered_b, 2)
        change = publish_threshold(w, 'req')
        # The cut is the publish transaction's latest fact, not any mount coverage.
        self.assertEqual(change.fact_cut, intake.fact_id)
        self.assertGreater(change.fact_cut, edit.fact_id)
        # New basis at that cut: 12 and 25 in A, 13 in B.
        self.assertEqual(_totals(change.after), (10, 3, 50))
        self.assertEqual(totals(read_live(w, 'A')), (2, 37))
        self.assertEqual(totals(read_live(w, 'B')), (1, 13))

    def test_two_sequential_changes_share_one_position_order(self):
        w = self.warehouse
        load_batch(w, 'A', 'b-a', [('x', 25)])
        opening = read_feed(w)
        request_threshold(w, 'req-1', 15)
        collect_threshold_partition(w, 'req-1', 'A')
        collect_threshold_partition(w, 'req-1', 'B')
        first = publish_threshold(w, 'req-1')
        request_threshold(w, 'req-2', 5)
        collect_threshold_partition(w, 'req-2', 'A')
        collect_threshold_partition(w, 'req-2', 'B')
        second = publish_threshold(w, 'req-2')
        self.assertEqual(
            (first.position, second.position),
            (opening.cursor.position + 1, opening.cursor.position + 2),
        )
        page = read_feed(w, opening.cursor)
        self.assertEqual(
            [type(entry).__name__ for entry in page.entries],
            ['FeedThresholdChange', 'FeedThresholdChange'],
        )
        self.assertEqual(
            [entry.change.request_id for entry in page.entries], ['req-1', 'req-2'],
        )
        self.assertEqual(page.entries[0].change.after.threshold, 15)
        self.assertEqual(page.entries[1].change.before.threshold, 15)
        self.assertEqual(page.entries[1].change.after.threshold, 5)
