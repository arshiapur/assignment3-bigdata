"""Check strict SNAP parsing before graph construction."""
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import parse_edge, parse_vertex

class ParserTests(unittest.TestCase):
    def test_comments_blanks_and_whitespace(self):
        self.assertIsNone(parse_edge('  # FromNodeId ToNodeId'))
        self.assertIsNone(parse_edge('\n'))
        self.assertEqual(parse_edge('  30\t1412  '), (30, 1412))

    def test_malformed_or_out_of_range_edges_fail(self):
        for line in ('1', '1 2 3', 'one 2', '1.5 2', f'{2**63} 2'):
            with self.subTest(line=line):
                with self.assertRaises(ValueError):
                    parse_edge(line)

    def test_optional_vertex_list(self):
        self.assertEqual(parse_vertex('7'), (7,))
        self.assertIsNone(parse_vertex('# isolated vertex IDs'))
        with self.assertRaises(ValueError):
            parse_vertex('7 8')

if __name__ == '__main__':
    unittest.main()
