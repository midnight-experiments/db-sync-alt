"""Both provider adapters over the same recorded answers: the built-in client (`PortableClient`, real
JSON decoding of the raw body) and the shared one (`SharedClient` loading an external client module
that decodes answers itself with `json.loads`, like the shared accounting client)."""
import json
from pathlib import Path
from db_sync_alt.sources.blockfrost.provider import PortableClient, SharedClient
from fixtures.blockfrost import NOT_FOUND
from fixtures.networks import PREPROD

KEY = 'preprod' + 'synthetic0000000000000000'
NOT_FOUND_BODY = '{"status_code":404,"error":"Not Found","message":"The requested component has not been found."}'

EXTERNAL_CLIENT = '''"""Synthetic external accounting client: answers from answers.json, decoded with json.loads."""
import contextlib
import json
from pathlib import Path

@contextlib.contextmanager
def locked(path):
    yield

class Client:
    base = %r
    def __init__(self, profile, root):
        self.profile = profile
        self.directory = Path(root)
        self.answers = json.loads((self.directory / 'answers.json').read_text())
        self.used = 0
    def status(self):
        return {'budget_used': self.used, 'daily_limit': 50000}
    def _load(self):
        return {'cooldown_until': 0}
    def get(self, path, purpose):
        self.used += 1
        status, body = self.answers.get(path, (400, 'unknown path'))
        if status != 200:
            raise RuntimeError('HTTP %%d; %%s' %% (status, body))
        return json.loads(body)
''' % PREPROD.blockfrost_base


def answer(routes, path):
    """(HTTP status, body text) of `path`: 404 for NOT_FOUND, 400 for a path without a route."""
    if path not in routes:
        return 400, '{"status_code":400,"error":"Bad Request","message":"no route"}'
    value = routes[path]
    if value is NOT_FOUND:
        return 404, NOT_FOUND_BODY
    return 200, json.dumps(value() if callable(value) else value)


def portable(routes, root):
    """The built-in client; its one-request-per-61-seconds pacing runs on a fake clock."""
    now = [1_700_000_000.0]

    def transport(path):
        status, body = answer(routes, path)
        return status, {}, body.encode()
    return PortableClient(KEY, str(Path(root) / 'portable'), network=PREPROD, transport=transport,
                          clock=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))


def shared(routes, root):
    """The shared adapter over an external client module written to `root`/shared."""
    directory = Path(root) / 'shared'
    directory.mkdir(mode=0o700, parents=True)
    (directory / 'client.py').write_text(EXTERNAL_CLIENT)
    (directory / 'answers.json').write_text(json.dumps({path: answer(routes, path) for path in routes}))
    return SharedClient(str(directory), network=PREPROD)


ADAPTERS = {'built-in': portable, 'shared': shared}
