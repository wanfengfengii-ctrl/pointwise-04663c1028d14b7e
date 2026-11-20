"""Run this project's full unittest suite and the threshold business checks."""
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

import threshold_demo
import threshold_fixture


def run_unittest_suite():
    suite = unittest.defaultTestLoader.discover('tests', top_level_dir='.')
    if suite.countTestCases():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print('No tests discovered in tests/', file=sys.stderr)
        result = unittest.TestResult()
    for status, failed_cases in (
        ('FAIL', (test for test, _ in result.failures)),
        ('ERROR', (test for test, _ in result.errors)),
        ('UNEXPECTED_SUCCESS', result.unexpectedSuccesses),
    ):
        for test in failed_cases:
            print(f'{status} {test.id()}')
    successful = result.testsRun > 0 and result.wasSuccessful()
    print(f'TEST SUMMARY total={result.testsRun} failures={len(result.failures)} '
          f'errors={len(result.errors)} skipped={len(result.skipped)} '
          f'expected_failures={len(result.expectedFailures)} '
          f'unexpected_success={len(result.unexpectedSuccesses)} '
          f'result={"PASS" if successful else "FAIL"}')
    return successful


def run_threshold_checks():
    case = json.loads(Path('samples/threshold-change.json').read_text(encoding='utf-8'))
    # The original V1 fixture still has to verify its two mounts, zero delta and
    # frozen daily report.
    with tempfile.TemporaryDirectory(prefix='linti-verify-v1-') as directory:
        v1 = threshold_fixture.inspect_v1(Path(directory) / 'warehouse', case)
    assert v1 == case['v1'], json.dumps({'expected': case['v1'], 'actual': v1}, sort_keys=True)
    # The V2 end-to-end workflow, asserted inside run_demo against the sample.
    with tempfile.TemporaryDirectory(prefix='linti-verify-v2-') as directory:
        observed = threshold_demo.run_demo(case, Path(directory) / 'warehouse')
    expected = case['v2']
    for key in ('fact_cut', 'position', 'before', 'after', 'after_delete', 'delete_delta'):
        assert observed[key] == expected[key], (key, observed[key], expected[key])
    print('THRESHOLD CHECKS v1_fixture=PASS v2_workflow=PASS')


def main():
    ok = run_unittest_suite()
    if ok:
        try:
            run_threshold_checks()
        except BaseException as exc:
            print(f'THRESHOLD CHECKS result=FAIL error={exc!r}')
            ok = False
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
