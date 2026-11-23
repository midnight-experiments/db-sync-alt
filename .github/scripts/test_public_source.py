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

class HistoryChecks(unittest.TestCase):
    def errors(self,files):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,text in files.items():
                (root/name).parent.mkdir(parents=True,exist_ok=True);(root/name).write_text(text)
            errors,_=checker.check(root)
        return sorted((name,code) for name,code in errors if ':line ' in code)

    def test_prose_names_no_earlier_version_upgrade_or_comparison_with_before(self):
        for name,text,code in [
                ('docs/a.md','Intro.\nRequires 0.4.9 first.\n','earlier_version:line 2'),
                ('README.md','Convert a 0.4.x database.\n','earlier_version:line 1'),
                ('SECURITY.md','Tag v0.3.0.\n','earlier_version:line 1'),
                ('deploy/README.md','To upgrade, set the image.\n','upgrade:line 1'),
                ('deploy/compose.yaml','# Upgrading needs a stop.\n','upgrade:line 1'),
                ('crates/x/tests/fixtures/README.md','Data for the migration test.\n','migration:line 1'),
                ('CONTRIBUTING.md','Unlike the previous release, it needs a key.\n','earlier_release:line 1'),
                ('profiles/x/README.md','Used by the Python implementation.\n','python_implementation:line 1'),
                ('CHANGELOG.md','**Breaking.** A new format.\n','breaking_release:line 1'),
                ('docs/b.md','The quota field is gone.\n','compared_with_before:line 1'),
                ('NOTICE','The service no longer reads a file.\n','compared_with_before:line 1'),
                ('.github/pull_request_template.md','Give the upgrade path.\n','upgrade:line 1')]:
            self.assertEqual(self.errors({name:text}),[(name,code)],(name,text))

    def test_numbers_and_files_that_are_not_versions_of_db_sync_alt_pass(self):
        page=('Listen on 0.0.0.0:18080 or 127.0.0.1; 0.19 s and 0.4 s; version=0.5.0;\n'
              'node 2.1.101, glibc 2.39, Compose 2.24.4, TLSv1.2, 10.4.1.\n'
              'Mark breaking changes clearly. Earlier blocks are pruned.\n')
        self.assertEqual(self.errors({'docs/a.md':page,'README.md':page,'deploy/compose.yaml':page}),[])
        # Crate versions in the generated notices, pinned upstream files and the
        # license texts are not prose of this repository.
        self.assertEqual(self.errors({'THIRD_PARTY_NOTICES.md':'| hex | 0.4.3 |\n',
                                      'reference/node/README.md':'Upgrade from 0.4.1.\n',
                                      'docs/licenses/x-LICENSE':'upgrade 0.1.0\n'}),[])

class ProcessReferenceChecks(unittest.TestCase):
    def errors(self,text,name='crates/x/tests/a.rs'):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/name).parent.mkdir(parents=True)
            (root/name).write_text(text)
            errors,_=checker.check(root)
        return sorted(code for found,code in errors if found==name)

    def test_development_references_are_refused(self):
        # Built by concatenation so that this file passes the check itself.
        refused=[('plan_test_number','/// '+'Test 3: a fork is replaced.\n'),
                 ('plan_test_number','    // '+'Tests 2 (first start), 4 and 5\n'),
                 ('question_reference','// See '+'Q'+'19.\n'),
                 ('question_reference','fn compare_'+'q'+'19_case() {}\n'),
                 ('question_reference','SELECT 1 '+'q'+'22_original;\n'),
                 ('requirement_reference','//! The digest ('+'FR'+'-044).\n'),
                 ('requirement_reference','// Bounded by '+'SC'+'-012.\n'),
                 ('commit_as_history','/// Captures from '+'b683'+'466 omit it.\n'),
                 ('commit_as_history','-- Exact `'+'e058'+'f39` query.\n')]
        for code,text in refused:
            self.assertEqual(self.errors(text),[code],text)

    def test_ordinary_text_passes(self):
        passed=['/// Tests the fork search.\n','// Test that 3 blocks follow.\n',
                'let quantity = q;\nlet sequence = seq1;\n',
                'const HASH: &str = "'+'b683466'+'e058f39'+'00";\n',
                '// The label 6500973 and 4194304 bytes.\n',
                '// FRAME-1 and SCALE-2 are not requirement numbers.\n',
                '// From deadbee: letters only; from 1234567: digits only.\n']
        for text in passed:
            self.assertEqual(self.errors(text),[],text)

class WorkflowImageChecks(unittest.TestCase):
    def errors(self,ci,dockerfile,compose):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,text in [(checker.CI_WORKFLOW,ci),('deploy/Dockerfile',dockerfile),
                              ('deploy/compose.yaml',compose)]:
                (root/name).parent.mkdir(parents=True,exist_ok=True);(root/name).write_text(text)
            errors,_=checker.check(root)
        return sorted(code for name,code in errors if name==checker.CI_WORKFLOW)

    def test_the_workflow_runs_in_the_pinned_images(self):
        rust='rust:1@sha256:'+'a'*64;postgres='postgres:17@sha256:'+'b'*64
        ci=f'jobs:\n  check:\n    container: {rust}\n    services:\n      postgres:\n        image: {postgres}\n'
        dockerfile=f'FROM {rust} AS build\n'
        compose=f'services:\n  postgres:\n    image: {postgres}\n'
        self.assertEqual(self.errors(ci,dockerfile,compose),[])
        self.assertEqual(self.errors(ci.replace(rust,'rust:1'),dockerfile,compose),
                         ['workflow_image_differs:rust'])
        self.assertEqual(self.errors(ci,dockerfile,compose.replace('b'*64,'c'*64)),
                         ['workflow_image_differs:postgres'])

if __name__=='__main__':unittest.main()
