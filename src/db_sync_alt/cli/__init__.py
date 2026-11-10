"""`db-sync-alt` command: one entry point with a subcommand per task.

A subcommand's module is imported only when it runs, so `compose init` works
without the PostgreSQL driver installed.
"""
import importlib
import sys

from db_sync_alt import __version__

# name: (module, summary, arguments placed before the user's arguments)
COMMANDS = {
    'run': ('db_sync_alt.cli.run', 'run in the foreground against a local PostgreSQL 17 database', ()),
    'service': ('db_sync_alt.service.runtime', 'run the service from a configuration file', ()),
    'acquire': ('db_sync_alt.cli.acquire', 'build or extend generations from cardano-db-sync or Blockfrost', ()),
    'upgrade': ('db_sync_alt.store.upgrade', 'upgrade a store to the current schema (5), adding epoch parameters to an older store', ()),
    'grant-reader': ('db_sync_alt.store.reader', "grant an operator-created login read access to the node's relations", ()),
    'backup': ('db_sync_alt.backup', 'write a PostgreSQL-native backup and its manifest', ('backup',)),
    'restore': ('db_sync_alt.backup', 'restore a verified backup into an empty database', ('restore',)),
    'verify': ('db_sync_alt.backup', 'verify every stored generation and the physical inventory', ('verify',)),
    'status': ('db_sync_alt.backup', 'print the store status', ('status',)),
    'compose': ('db_sync_alt.cli.compose', 'create private Compose configuration, or provision the bundled PostgreSQL as its operator', ()),
}


def usage():
    width = max(map(len, COMMANDS))
    lines = ['usage: db-sync-alt <command> [options]', '', 'commands:']
    lines += ['  ' + name.ljust(width) + '  ' + summary for name, (_, summary, _) in COMMANDS.items()]
    lines += ['', 'Run `db-sync-alt <command> --help` for the options of a command.']
    return '\n'.join(lines) + '\n'


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ('-h', '--help'):
        sys.stdout.write(usage())
        return 0 if argv else 2
    if argv[0] == '--version':
        print('db-sync-alt ' + __version__)
        return 0
    if argv[0] == '_trust-relevance-history':
        from db_sync_alt.cli.relevance_history import main as trust_history
        return trust_history(argv[1:])
    command = COMMANDS.get(argv[0])
    if command is None:
        sys.stderr.write('db-sync-alt: unknown command ' + repr(argv[0]) + '\n' + usage())
        return 2
    module, _, prefix = command
    result = importlib.import_module(module).main([*prefix, *argv[1:]])
    return 0 if result is None else result
