"""Fact contract: numeric domains and the trusted acquisition profile."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from db_sync_alt.contract import NUMERIC_FIELDS, Profile, Refusal, validate_numeric, epoch_size, generation_capabilities, digest
from db_sync_alt.resources import profile_dir


class ContractTests(unittest.TestCase):
    def test_numeric_domains(self):
        for table,fields in NUMERIC_FIELDS.items():
            valid={key:str(low) if key in ('value','quantity','amount') else low for key,(low,high) in fields.items()}
            validate_numeric(table,valid)
            for key,(low,high) in fields.items():
                for value in (low-1,high+1):
                    with self.assertRaisesRegex(Refusal,'invalid_numeric_domain'):validate_numeric(table,dict(valid,**{key:str(value) if key in ('value','quantity','amount') else value}))
        for value in ('01','-0','+1',' 1','1.0','1e3'):
            with self.assertRaises(Refusal):validate_numeric('mints',{'quantity':value})

    def test_epoch_size_is_strict_inclusive_and_never_defaulted(self):
        for value in (0,12000,24000,2**31-1):
            self.assertEqual(epoch_size(value),value)
            validate_numeric('epoch_nonces',{'epoch':2,'nonce':b'n'*32,'max_tx_size':value})
        for value in (None,True,False,-1,2**31,12000.0,'12000'):
            with self.subTest(value=value),self.assertRaisesRegex(Refusal,'invalid_epoch_max_tx_size'):
                epoch_size(value)
        with self.assertRaisesRegex(Refusal,'invalid_numeric_domain'):
            validate_numeric('epoch_nonces',{'epoch':2,'nonce':b'n'*32})
        # Facts sealed in the nonce-only format are checked over their own columns.
        validate_numeric('epoch_nonces',{'epoch':2,'nonce':b'n'*32},legacy=True)

    def test_nonce_only_contract_is_recognised_but_never_current(self):
        from db_sync_alt import networks
        for name in networks.names():
            network=networks.network_profile(name)
            self.assertIs(networks.for_contract(network.contract),network)
            self.assertIs(networks.for_contract(network.legacy_contract,legacy=True),network)
            with self.assertRaisesRegex(Refusal,'unsupported_format'):networks.for_contract(network.legacy_contract)
            with self.assertRaisesRegex(Refusal,'unsupported_format'):networks.for_contract(network.contract,legacy=True)
            self.assertNotEqual(digest(network.contract),digest(network.legacy_contract))
        profile=Profile(profile_dir('preview'))
        # The profile identity is unchanged by the fact format; the contract and capability carry it.
        self.assertEqual(set(profile.identity),{'network','config_sha256','genesis','configs','profile'})
        self.assertIs(generation_capabilities()['epoch_max_tx_size_v1'],True)
        self.assertNotIn('epoch_max_tx_size_v1',generation_capabilities(legacy=True))

    def test_trusted_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            target=Path(tmp)/'profile';shutil.copytree(profile_dir('preprod'),target)
            self.assertEqual(Profile(target).start,(4500197,4))
            f=target/'registered-candidates-addresses.json';x=json.loads(f.read_text());x['committee_candidates_address']+='x';f.write_text(json.dumps(x))
            f2=target/'source-manifest.json';x=json.loads(f2.read_text());x['files'][f.name]['sha256']=hashlib.sha256(f.read_bytes()).hexdigest();f2.write_text(json.dumps(x))
            with self.assertRaises(Refusal):Profile(target)


if __name__=='__main__':unittest.main()
