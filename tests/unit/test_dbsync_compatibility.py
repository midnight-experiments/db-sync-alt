"""Source schema acceptance stays separate from the persisted contract."""
import unittest
from db_sync_alt.contract import SCHEMA_VERSION, Refusal
from db_sync_alt.sources.dbsync.compatibility import SOURCE_COLUMNS, source_version, validate_columns, validate_modes, validate_attestation

class SourceCompatibility(unittest.TestCase):
    def test_only_explicit_schema_versions_are_accepted_and_contract_unchanged(self):
        self.assertEqual(source_version((15,48,6)),'13.7.1.0')
        self.assertEqual(source_version((16,52,6)),'13.8.0.0')
        self.assertEqual(SCHEMA_VERSION,(15,48,6))
        for value in [(15,44,6),(16,52,0),(16,53,6),(15,48,0)]:
            with self.assertRaisesRegex(Refusal,'unsupported_source_schema'):source_version(value)
    def test_supported_shape_rejects_missing_and_mistyped_columns(self):
        rows=[dict(table_name=t,column_name=c,udt_name=v[0],domain_name=v[1],is_nullable=v[2],relkind='r') for t,cols in SOURCE_COLUMNS.items() for c,v in cols.items()]
        validate_columns(rows)
        with self.assertRaisesRegex(Refusal,'source_columns_missing'):validate_columns(rows[1:])
        for field,value in [('udt_name','text'),('domain_name','unqualified'),('is_nullable','YES'),('relkind','v')]:
            bad=[dict(r) for r in rows];bad[0][field]=value
            with self.assertRaisesRegex(Refusal,'unsupported_source_column_type'):validate_columns(bad)
    def test_modes_and_attestations_fail_closed(self):
        validate_modes([]);validate_modes(['StakeDistrEnded'])
        with self.assertRaisesRegex(Refusal,'unsupported_source_mode'):validate_modes(['Unknown'])
        for version in ['13.7.1.0','13.8.0.0']:
            good=dict(dbsync_version=version,tx_in_retained=True,epoch_stake_retained=True,operator_attested=True)
            validate_attestation(good,version)
            observed=dict(good,operator_attested=False,retention_evidence='observed-launch-and-indexed-history')
            validate_attestation(observed,version)
            with self.assertRaisesRegex(Refusal,'unknown_attestation_fields'):
                validate_attestation(dict(good,unchecked_filtering=False),version)
            for key,value,code in [('dbsync_version','unknown','source_version_attestation_mismatch'),('tx_in_retained',1,'retention_attestation_required'),('epoch_stake_retained',False,'retention_attestation_required'),('operator_attested',False,'retention_evidence_required')]:
                with self.assertRaisesRegex(Refusal,code):validate_attestation(dict(good,**{key:value}),version)

    def test_ordinal_upgrade_cli_requires_explicit_truthful_attestation(self):
        from db_sync_alt.store import upgrade
        import contextlib
        import io
        with contextlib.redirect_stderr(io.StringIO()):
            for args in [['--service','destination','--source-service','source'],['--service','destination','--source-attestation','att.json']]:
                with self.assertRaises(SystemExit) as caught:upgrade.main(args)
                self.assertEqual(caught.exception.code,2)
