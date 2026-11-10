"""Explicit relevance configuration and observable fail-closed modes."""
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from db_sync_alt.cli import acquire,run,compose
from db_sync_alt.contract import Profile,Refusal
from db_sync_alt.errors import LifecycleFailure
from db_sync_alt.networks import names
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.relevance import requested_coverage
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from fixtures.sync import Store,health

class RelevanceOptionTests(unittest.TestCase):
    def test_default_off_and_strict_boolean_on_all_packaged_networks(self):
        for network in names():
            profile=Profile(profile_dir(network))
            for value in (False,True):
                driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':value}),profile=profile)
                self.assertIs(driver.options['relevance_enabled'],value)
            self.assertIs(SyncDriver(SimpleNamespace(sync={}),profile=profile).options['relevance_enabled'],False)
            for value in (0,1,'false',None,[],{}):
                with self.assertRaisesRegex(LifecycleFailure,'invalid_sync_configuration'):
                    SyncDriver(SimpleNamespace(sync={'relevance_enabled':value}),profile=profile)
    def test_public_cli_default_and_explicit_flags(self):
        for flags,expected in (([],False),(['--relevance'],True),(['--no-relevance'],False)):
            self.assertIs(acquire.parser().parse_args(['--source','blockfrost','--destination-service','fixture','--network','preprod',*flags]).relevance_enabled,expected)
            self.assertIs(run.parser().parse_args(['--network','preview',*flags]).relevance_enabled,expected)
            with patch.object(compose,'initialize') as initialize:
                compose.main(['init','--private','fixture','--archive','dump','--manifest','manifest',*flags])
                self.assertIs(initialize.call_args.args[0].relevance_enabled,expected)
    def test_off_never_opens_relevance_parent_or_lineage(self):
        for network in names():
            profile=Profile(profile_dir(network));store=Store();status=health()
            driver=SyncDriver(SimpleNamespace(sync={}),profile=profile)
            with patch('db_sync_alt.sources.blockfrost.relevance.certificate',side_effect=AssertionError('unrequested proof work')):
                self.assertIsNone(requested_coverage(store,profile,101,None,False))
                driver._check_relevance(store,status,store.generation(),None)
            self.assertEqual(status.snapshot()['relevance'],{'requested':'full','effective':'full','reason':'disabled'})
    def test_enabled_unsupported_refuses_before_client_or_poller_starts(self):
        for network in names():
            profile=Profile(profile_dir(network));store=Store();status=health()
            driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),profile=profile)
            # This legacy store cannot supply a verified incremental parent.
            with self.assertRaisesRegex(LifecycleFailure,'relevance_parent_unsupported'):
                driver.run_once(store,status,threading.Event())
            self.assertIsNone(driver._client)
            self.assertEqual(status.snapshot()['relevance'],{'requested':'relevance','effective':'full','reason':'relevance_parent_unsupported'})
            self.assertIn('relevance_parent_unsupported',status.snapshot()['reason_codes'])
    def test_mode_flip_refuses_without_certificate_or_provider(self):
        for enabled,value in ((True,None),(False,{'kind':'declared'})):
            pending={'manifest':{} if value is None else {'relevance_coverage':value}}
            with patch('db_sync_alt.sources.blockfrost.relevance.certificate',side_effect=AssertionError('mode flip spent work')):
                with self.assertRaisesRegex(Refusal,'relevance_mode_mismatch'):
                    requested_coverage(None,None,101,pending,enabled)
    def test_enabled_health_mode_and_corrupt_proof_reason(self):
        profile=Profile(profile_dir('mainnet'));store=Store();store.supports_incremental=True;status=health()
        driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),profile=profile)
        with patch('db_sync_alt.sources.blockfrost.relevance.requested_coverage',return_value={'kind':'verified'}):
            driver._check_relevance(store,status,store.generation(),None)
        self.assertEqual(status.snapshot()['relevance'],{'requested':'relevance','effective':'relevance','reason':'verified'})
        for proof_code in ('relevance_coverage_invalid','completion_invalid','completion_mismatch'):
            with patch('db_sync_alt.sources.blockfrost.relevance.requested_coverage',side_effect=Refusal(proof_code)):
                with self.assertRaisesRegex(LifecycleFailure,'relevance_coverage_invalid'):
                    driver.start_polling(store,status,threading.Event(),lambda error:None)
            self.assertIsNone(driver._client);self.assertIsNone(driver._poller)
            self.assertEqual(status.snapshot()['relevance']['reason'],'relevance_coverage_invalid')
    def test_trusted_history_is_explicit_reported_provenance_not_a_health_error(self):
        profile=Profile(profile_dir('preprod'));store=Store();store.supports_incremental=True;status=health()
        driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),profile=profile)
        with patch('db_sync_alt.sources.blockfrost.relevance.requested_coverage',return_value={'version':2,'origin':'trusted-history'}):
            driver._check_relevance(store,status,store.generation(),None)
        snapshot=status.snapshot()
        self.assertEqual(snapshot['relevance'],{'requested':'relevance','effective':'relevance','reason':'trusted-history'})
        self.assertNotIn('trusted-history',snapshot['reason_codes']);self.assertNotIn('internal_error',snapshot['reason_codes'])
    def test_trust_command_hidden_and_mainnet_refuses_before_database_open(self):
        from db_sync_alt import cli
        from db_sync_alt.cli import relevance_history
        self.assertNotIn('_trust-relevance-history',cli.COMMANDS)
        self.assertNotIn('_trust-relevance-history',cli.usage())
        with patch.object(relevance_history,'PgStore',side_effect=AssertionError('Mainnet database opened')) as opened,patch('sys.stderr'):
            self.assertEqual(cli.main(['_trust-relevance-history','--destination-service','unused','--network','mainnet']),1)
            opened.assert_not_called()
        for module in (acquire,run):
            self.assertNotIn('trust-history',module.parser().format_help())
