"""Admission of compressed public fixtures must preserve source privacy checks."""
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC=importlib.util.spec_from_file_location('public_source',Path(__file__).with_name('check_public_source.py'))
checker=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)
FIXTURE='crates/dsa-provider/tests/fixtures/sample-corpus.json.gz'

def bundle(files):
    return {'format':'recorded-corpus-v1','network':'preprod','files':files,
            'sha256':{name:hashlib.sha256(text.encode()).hexdigest() for name,text in files.items()}}

def compressed(value):
    return gzip.compress(json.dumps(value).encode(),mtime=0)

class CompressedFixtureChecks(unittest.TestCase):
    def errors(self,raw,approved=True):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);target=root/FIXTURE
            target.parent.mkdir(parents=True);target.write_bytes(raw)
            admitted={FIXTURE:hashlib.sha256(raw).hexdigest()} if approved else {}
            with patch.object(checker,'APPROVED_JSON_GZIP',admitted):
                errors,_=checker.check(root)
            return [code for name,code in errors if name==FIXTURE]

    def test_only_exact_admitted_bytes_are_read_as_compressed_json(self):
        raw=compressed(bundle({'expected.json':'{}'}))
        self.assertEqual(self.errors(raw),[])
        self.assertEqual(self.errors(raw,False),['binary_file'])
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);target=root/FIXTURE
            target.parent.mkdir(parents=True);target.write_bytes(raw+b'x')
            with patch.object(checker,'APPROVED_JSON_GZIP',{FIXTURE:hashlib.sha256(raw).hexdigest()}):
                errors,_=checker.check(root)
            self.assertIn((FIXTURE,'unapproved_binary_asset'),errors)

    def test_member_hash_paths_and_json_are_checked(self):
        bad_hash=bundle({'expected.json':'{}'});bad_hash['sha256']['expected.json']='00'
        for value in [bad_hash,bundle({'../expected.json':'{}'}),
                      bundle({'provider/headers/./1.json':'{}'}),
                      bundle({'attempts.jsonl':'{}'}),bundle({'expected.json':'invalid'})]:
            self.assertEqual(self.errors(compressed(value)),['invalid_recorded_fixture'])

    def test_expanded_content_receives_existing_privacy_scans(self):
        values=[('provider_credential','mainnet'+'a'*32),
                ('personal_absolute_path','/home/'+'fixture-user/private/')]
        for code,text in values:
            raw=compressed(bundle({'expected.json':json.dumps({'value':text})}))
            self.assertEqual(self.errors(raw),[code])

    def test_compressed_and_expanded_limits(self):
        with self.assertRaises(ValueError):
            checker.recorded_fixture_text(b'0'*(5*1024*1024+1))
        raw=gzip.compress(b' '*(64*1024*1024+1),mtime=0)
        with self.assertRaises(ValueError):checker.recorded_fixture_text(raw)

class DocumentationChecks(unittest.TestCase):
    def errors(self,files):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,text in files.items():
                (root/name).parent.mkdir(parents=True,exist_ok=True);(root/name).write_text(text)
            errors,_=checker.check(root)
        return sorted(code for name,code in errors if name.startswith('docs/'))

    def test_a_link_to_a_missing_heading_is_refused(self):
        page='# Page\n\n## The `run` command\n\n```sh\n# Not a heading\n```\n'
        self.assertEqual(self.errors({'docs/a.md':page,'docs/b.md':'[x](a.md#the-run-command) [y](#here)\n\n## Here\n'}),[])
        self.assertEqual(self.errors({'docs/a.md':page,'docs/b.md':'[x](a.md#not-a-heading)\n'}),
                         ['broken_anchor:a.md#not-a-heading'])
        self.assertEqual(self.errors({'docs/b.md':'[y](#elsewhere)\n'}),['broken_anchor:#elsewhere'])

    def test_every_troubleshooting_code_exists_in_the_sources(self):
        source={'crates/x/src/lib.rs':'const A: &str = "known_code";\n'}
        page='| Code | Cause |\n| --- | --- |\n| `known_code` | x |\n'
        self.assertEqual(self.errors({**source,checker.TROUBLESHOOTING:page}),[])
        self.assertEqual(self.errors({**source,checker.TROUBLESHOOTING:page+'| `invented_code` | y |\n'}),
                         ['unknown_code:invented_code'])
        self.assertEqual(self.errors({**source,checker.TROUBLESHOOTING:'No table.\n'}),['no_codes'])

if __name__=='__main__':unittest.main()
