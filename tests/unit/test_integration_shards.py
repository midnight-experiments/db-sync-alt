"""Every integration module remains scheduled, with density isolated."""
import ast
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('dev_shards', ROOT/'tools/dev.py')
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


def static_case_inventory():
    """Collect declarations and inherited cases without importing DB-gated packages."""
    tests = ROOT/'tests'
    modules = {}
    for path in tests.rglob('*.py'):
        name = '.'.join(path.relative_to(tests).with_suffix('').parts)
        tree = ast.parse(path.read_text())
        classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
        bindings = {}
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module and not node.level:
                for alias in node.names:
                    bindings[alias.asname or alias.name] = node.module+'.'+alias.name
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    bindings[alias.asname or alias.name.split('.')[0]] = (
                        alias.name if alias.asname else alias.name.split('.')[0])
        modules[name] = (classes, bindings, tree)

    def reference(node, module):
        if not isinstance(node, (ast.Name, ast.Attribute)):
            return ''
        text = ast.unparse(node)
        first, dot, rest = text.partition('.')
        classes, bindings, _ = modules[module]
        if first in classes:
            first = module+'.'+first
        else:
            first = bindings.get(first, first)
        return first+(dot+rest if dot else '')

    cache = {}

    def case_class(ref):
        if ref == 'unittest.TestCase':
            return True, set()
        if ref in cache:
            return cache[ref]
        module, _, name = ref.rpartition('.')
        if module not in modules or name not in modules[module][0]:
            return False, set()
        cls = modules[module][0][name]
        inherited = [case_class(reference(base, module)) for base in cls.bases]
        is_case = any(is_case for is_case, _ in inherited)
        methods = set().union(*(methods for _, methods in inherited))
        methods.update(node.name for node in cls.body
                       if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                       and node.name.startswith('test_'))
        cache[ref] = is_case, methods
        return cache[ref]

    inventory = {}
    for module, (classes, bindings, tree) in modules.items():
        if not module.startswith('integration.test_'):
            continue
        exported = [module+'.'+name for name in classes]+list(bindings.values())
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.value, (ast.Name, ast.Attribute)):
                exported.extend(reference(node.value, module) for target in node.targets
                                if isinstance(target, ast.Name))
        inventory[module] = [ref+'.'+method for ref in exported
                             for is_case, methods in [case_class(ref)] if is_case
                             for method in sorted(methods)]
    return inventory


class IntegrationShardTests(unittest.TestCase):
    def test_shards_cover_every_static_case_once(self):
        inventory = static_case_inventory()
        expected = [case for module in sorted(inventory) for case in inventory[module]]
        self.assertTrue(expected)
        self.assertEqual(len(expected), len(set(expected)))
        for count in (1, 2, 3, 4, 5, 8):
            with self.subTest(count=count):
                actual = [case for i in range(1, count+1)
                          for module in dev.shard_modules(f'{i}/{count}')
                          for case in inventory[module]]
                self.assertEqual(sorted(actual), sorted(expected))
                self.assertEqual(len(actual), len(set(actual)))

    def test_ci_shards_partition_every_module_without_duplication(self):
        expected = dev.shard_modules(None)
        for count in (1, 2, 3, 4, 5, 8):
            with self.subTest(count=count):
                shards = [dev.shard_modules(f'{i}/{count}') for i in range(1, count+1)]
                flat = sum(shards, [])
                self.assertEqual(sorted(flat), expected)
                self.assertEqual(len(flat), len(set(flat)))
                if count > 1:
                    self.assertEqual(shards[-1], ['integration.test_prerequisites'])
