"""Protect saved experiment evidence when a checkpointed run is resumed."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_experiment import previous_observations, write_report


class RunnerStateTests(unittest.TestCase):
    def capture(self, work, number, run_id='run-a'):
        prefix = work / f'snapshot-{number:02d}'
        observation = {'run_id': run_id, 'kibana_render_verified': True,
                       'captured_at': '2026-11-10T04:26:20+00:00',
                       'elapsed_seconds': 120, 'batch_id': 0,
                       'rows': [{'entity': 'paris', 'count': 1}]}
        prefix.with_suffix('.json').write_text(json.dumps(observation))
        for suffix in ('.csv', '.png', '-kibana.png'):
            Path(str(prefix) + suffix).write_bytes(b'original capture')

    def test_resume_keeps_complete_captures_and_reserves_interrupted_numbers(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            self.capture(work, 1)
            self.capture(work, 2)
            (work / 'snapshot-02-kibana.png').unlink()
            (work / 'snapshot-03.json').write_text('{interrupted JSON')
            before = {p.name: p.read_bytes() for p in work.iterdir()}
            observations, next_number = previous_observations(work, 'run-a')
            self.assertEqual(next_number, 4)
            self.assertEqual([o['snapshot_number'] for o in observations], [1])
            self.assertEqual(before, {p.name: p.read_bytes() for p in work.iterdir()})

    def test_rejects_capture_from_another_run(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            self.capture(work, 1, 'other-run')
            with self.assertRaisesRegex(ValueError, 'different run'):
                previous_observations(work, 'run-a')

    def test_report_preserves_capture_numbers_and_sessions(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            self.capture(work, 1)
            self.capture(work, 3)
            observations, next_number = previous_observations(work, 'run-a')
            observations[1]['session_started_at'] = 'second-session'
            write_report(work, 'second-session', observations,
                         SimpleNamespace(feed='CONTROLLED TEST', controlled=True))
            report = (work / 'REPORT.md').read_text()
            self.assertEqual(next_number, 4)
            self.assertIn('snapshot-01-kibana.png', report)
            self.assertIn('snapshot-03-kibana.png', report)
            self.assertNotIn('snapshot-02.png', report)
            self.assertIn('Each elapsed time is measured from its own runner invocation', report)


if __name__ == '__main__':
    unittest.main()
