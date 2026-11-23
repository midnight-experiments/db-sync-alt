"""The license texts of the release: crate files, shared texts once, SPDX fallback."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    'third_party_notices', Path(__file__).with_name('third_party_notices.py'))
notices = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(notices)


def package(root, name, license, files, authors=('A. Author <a@example.org>',), source='registry'):
    directory = root/f'{name}-1.0.0'
    directory.mkdir()
    (directory/'Cargo.toml').write_text('[package]\n')
    for relative, text in files.items():
        (directory/relative).parent.mkdir(parents=True, exist_ok=True)
        (directory/relative).write_text(text)
    return {'id': name, 'name': name, 'version': '1.0.0', 'license': license, 'source': source,
            'authors': list(authors), 'manifest_path': str(directory/'Cargo.toml')}


def metadata(root, packages):
    binary = {'id': 'bin', 'name': notices.BINARY, 'version': '0.0.0', 'license': 'Apache-2.0',
              'source': None, 'authors': [], 'manifest_path': str(root/'Cargo.toml')}
    normal = [{'kind': None, 'target': None}]
    nodes = [{'id': 'bin', 'deps': [{'pkg': p['id'], 'dep_kinds': normal} for p in packages]}]
    nodes += [{'id': p['id'], 'deps': []} for p in packages]
    return {'packages': [binary, *packages], 'resolve': {'nodes': nodes}}


class LicenseTexts(unittest.TestCase):
    def test_every_crate_file_is_kept_and_identical_texts_appear_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mit = 'MIT License\n\nCopyright (c) 2020 Someone\n'
            packages = [
                package(root, 'alpha', 'MIT OR Apache-2.0', {'LICENSE-MIT': mit, 'LICENSE-APACHE': 'Apache text\r\n'}),
                package(root, 'beta', 'MIT', {'LICENSE': mit+'\n\n', 'NOTICE': 'Beta notice'}),
                package(root, 'gamma', 'MIT', {'LICENSES/MIT.txt': 'Copyright gamma'}),
                package(root, 'delta', 'MIT', {'README.md': 'no license here'}),
            ]
            text, fallbacks = notices.licenses_file(notices.crates(metadata(root, packages)))
        self.assertEqual(fallbacks, ['delta 1.0.0'])
        self.assertEqual(text.count('Copyright (c) 2020 Someone'), 1, 'a shared text appears once')
        self.assertIn('alpha 1.0.0 (MIT OR Apache-2.0): LICENSE-APACHE [1], LICENSE-MIT [2]', text)
        self.assertIn('beta 1.0.0 (MIT): LICENSE [2], NOTICE [3]', text)
        self.assertIn('gamma 1.0.0 (MIT): LICENSES/MIT.txt [5]', text)
        self.assertIn('delta 1.0.0 (MIT): no license file in the crate; SPDX MIT text [4]; '
                      'copyright holders, as its manifest names its authors: A. Author <a@example.org>', text)
        self.assertIn((notices.SPDX/'MIT.txt').read_text().strip(), text)
        self.assertNotIn('\r', text)
        self.assertNotIn(directory, text, 'no local path enters the file')

    def test_a_crate_without_files_and_without_a_fallback_text_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            packages = [package(root, 'epsilon', 'Zlib', {})]
            with self.assertRaises(SystemExit) as refused:
                notices.licenses_file(notices.crates(metadata(root, packages)))
        self.assertIn('spdx/Zlib.txt', str(refused.exception))

    def test_only_normal_dependencies_from_a_registry_are_listed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            listed = package(root, 'zeta', 'MIT', {'LICENSE': 'x'})
            local = package(root, 'eta', 'MIT', {}, source=None)
            value = metadata(root, [listed, local])
            build_only = package(root, 'theta', 'MIT', {})
            value['packages'].append(build_only)
            value['resolve']['nodes'][0]['deps'].append(
                {'pkg': 'theta', 'dep_kinds': [{'kind': 'build', 'target': None}]})
            value['resolve']['nodes'].append({'id': 'theta', 'deps': []})
            self.assertEqual([p['name'] for p in notices.crates(value)], ['zeta'])


if __name__ == '__main__':
    unittest.main()
