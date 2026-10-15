import csv
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from avro.schema import parse
from avro.compatibility import ReaderWriterCompatibilityChecker, SchemaCompatibilityType

from pipeline.cloud import schema_variants
from pipeline.config import load_env
from pipeline.dataset import validate_dataset, read_csv, TRANSACTIONS
from pipeline.reference import expected_alerts
from pipeline.simulate import generate


def event(identity, milliseconds, card='card-a', op='c'):
    return {'op': op, 'after': {'transaction_id': identity, 'card_token': card, 'occurred_at_ms': milliseconds}}


class FraudRuleTests(unittest.TestCase):
    def test_three_events_cross_minute_boundary(self):
        alerts = expected_alerts([event('a', 50000), event('b', 70000), event('c', 100000)])
        self.assertEqual([a['alert_id'] for a in alerts], ['card-a:c'])

    def test_exactly_sixty_seconds_is_included(self):
        self.assertEqual(len(expected_alerts([event('a', 0), event('b', 30000), event('c', 60000)])), 1)

    def test_one_millisecond_outside_is_excluded(self):
        self.assertEqual(expected_alerts([event('a', 0), event('b', 30000), event('c', 60001)]), [])

    def test_cards_are_isolated(self):
        self.assertEqual(expected_alerts([event('a', 0), event('b', 1000), event('c', 2000, 'other')]), [])

    def test_update_delete_tombstone_do_not_count(self):
        self.assertEqual(expected_alerts([event('a', 0), event('a', 1000, op='u'),
                                          {'op': 'd', 'after': None}, None, event('b', 2000)]), [])

    def test_snapshot_counts_like_insert(self):
        self.assertEqual(len(expected_alerts([event('a', 0, op='r'), event('b', 1000), event('c', 2000)])), 1)

    def test_overlapping_triplets(self):
        alerts = expected_alerts([event(str(i), i * 1000) for i in range(4)])
        self.assertEqual([a['transaction_ids'] for a in alerts], [['0', '1', '2'], ['1', '2', '3']])

    def test_offline_oracle_sorts_event_time(self):
        self.assertEqual(expected_alerts([event('c', 2000), event('a', 0), event('b', 1000)])[0]['transaction_ids'], ['a', 'b', 'c'])

    def test_repeated_id_is_not_three_transactions(self):
        self.assertEqual(expected_alerts([event('a', 0), event('a', 1000), event('a', 2000)]), [])


class DatasetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.expected = generate(self.root, 1767225600000)

    def test_simulation_has_known_alerts_and_negative_case(self):
        accounts, transactions = validate_dataset(self.root)
        self.assertEqual((len(accounts), len(transactions)), (3, 11))
        self.assertEqual({a['alert_id'] for a in self.expected['expected_alerts']},
                         {'card-burst:card-burst-3', 'card-burst:card-burst-4', 'card-boundary:card-boundary-3'})

    def rewrite(self, mutate):
        path = self.root / 'transactions.csv'
        with path.open(newline='', encoding='utf-8') as stream:
            rows = list(csv.DictReader(stream))
        mutate(rows)
        with path.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=TRANSACTIONS)
            writer.writeheader()
            writer.writerows(rows)

    def test_no_silent_overwrite(self):
        with self.assertRaisesRegex(ValueError, 'sobrescrito'):
            generate(self.root, 1767225600000)

    def test_dangling_account_is_rejected(self):
        self.rewrite(lambda rows: rows[0].update(account_id='unknown'))
        with self.assertRaisesRegex(ValueError, 'ausente'):
            validate_dataset(self.root)

    def test_duplicate_ids_are_rejected(self):
        self.rewrite(lambda rows: rows.append(rows[0]))
        with self.assertRaisesRegex(ValueError, 'duplicado'):
            validate_dataset(self.root)

    def test_money_cannot_be_float(self):
        self.rewrite(lambda rows: rows[0].update(amount_minor='12.30'))
        with self.assertRaisesRegex(ValueError, 'inteiro'):
            validate_dataset(self.root)

    def test_bigint_overflow_is_rejected(self):
        self.rewrite(lambda rows: rows[0].update(amount_minor=str(2**63)))
        with self.assertRaisesRegex(ValueError, 'BIGINT'):
            validate_dataset(self.root)

    def test_empty_dataset_is_rejected(self):
        path = self.root / 'transactions.csv'
        path.write_text(','.join(TRANSACTIONS) + '\n', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'sem registros'):
            read_csv(path, TRANSACTIONS)


class SchemaEvolutionTests(unittest.TestCase):
    # Fixture minima para testar Avro; nao e o schema real registrado pelo Debezium.
    schema = {'type': 'record', 'name': 'EnvelopeFixture', 'fields': [
        {'name': 'op', 'type': 'string'}, {'name': 'after', 'type': ['null', 'string'], 'default': None}]}

    def test_optional_field_is_forward_and_backward_compatible(self):
        good, _ = schema_variants(self.schema)
        old, new = parse(json.dumps(self.schema)), parse(json.dumps(good))
        checker = ReaderWriterCompatibilityChecker()
        self.assertEqual(checker.get_compatibility(new, old).compatibility, SchemaCompatibilityType.compatible)
        self.assertEqual(checker.get_compatibility(old, new).compatibility, SchemaCompatibilityType.compatible)

    def test_old_record_deserializes_with_new_default(self):
        good, _ = schema_variants(self.schema)
        old, new = parse(json.dumps(self.schema)), parse(json.dumps(good))
        buffer = io.BytesIO()
        DatumWriter(old).write({'op': 'c', 'after': 'fixture'}, BinaryEncoder(buffer))
        buffer.seek(0)
        row = DatumReader(old, new).read(BinaryDecoder(buffer))
        self.assertIsNone(row['audit_tag'])
        self.assertEqual(row['op'], 'c')

    def test_type_break_is_incompatible(self):
        _, bad = schema_variants(self.schema)
        result = ReaderWriterCompatibilityChecker().get_compatibility(parse(json.dumps(bad)), parse(json.dumps(self.schema)))
        self.assertEqual(result.compatibility, SchemaCompatibilityType.incompatible)


class EnvironmentTests(unittest.TestCase):
    def test_values_are_literal_and_existing_env_wins(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / '.env'
            path.write_text('PIPELINE_TEST_LITERAL="$(whoami)`echo`"\nPIPELINE_TEST_EXISTING=new\n', encoding='utf-8')
            with patch.dict(os.environ, {'PIPELINE_TEST_EXISTING': 'old'}, clear=True):
                load_env(path)
                self.assertEqual(os.environ['PIPELINE_TEST_LITERAL'], '$(whoami)`echo`')
                self.assertEqual(os.environ['PIPELINE_TEST_EXISTING'], 'old')


if __name__ == '__main__':
    unittest.main()
