"""Dependency-free checks of reconstructed text normalization and NER filtering."""
import sys
from pathlib import Path
import unittest
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pipeline

class UnitTests(unittest.TestCase):
    def test_markup(self):
        self.assertEqual(pipeline.clean('<p>Paris &amp; <b>London</b></p>'), 'Paris & London')

    def test_entity_occurrences_and_filtering(self):
        original = pipeline._MODEL
        pipeline._MODEL = lambda _: SimpleNamespace(ents=[
            SimpleNamespace(text='Barack Obama', label_='PERSON'),
            SimpleNamespace(text='Barack Obama', label_='PERSON'),
            SimpleNamespace(text='  NEW   YORK ', label_='GPE'),
            SimpleNamespace(text='Tuesday', label_='DATE')])
        try:
            self.assertEqual(pipeline.entities('unused'), [('barack obama', 'PERSON'), ('barack obama', 'PERSON'), ('new york', 'GPE')])
        finally:
            pipeline._MODEL = original

if __name__ == '__main__':
    unittest.main()
