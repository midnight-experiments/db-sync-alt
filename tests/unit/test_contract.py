"""Fact contract: numeric domains and the trusted acquisition profile."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from db_sync_alt.contract import NUMERIC_FIELDS, Profile, Refusal, validate_numeric
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

    def test_trusted_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            target=Path(tmp)/'profile';shutil.copytree(profile_dir('preprod'),target)
            self.assertEqual(Profile(target).start,(4500197,4))
            f=target/'registered-candidates-addresses.json';x=json.loads(f.read_text());x['committee_candidates_address']+='x';f.write_text(json.dumps(x))
            f2=target/'source-manifest.json';x=json.loads(f2.read_text());x['files'][f.name]['sha256']=hashlib.sha256(f.read_bytes()).hexdigest();f2.write_text(json.dumps(x))
            with self.assertRaises(Refusal):Profile(target)


if __name__=='__main__':unittest.main()
