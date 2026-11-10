"""A minimal external accounting client for tests of DBSYNC_PROVIDER_ADAPTER=shared (test support only).

It offers the interface db-sync-alt's SharedClient uses (`Client(profile, root=)` with `base`,
`directory`, `status`, `_load`, `get`; `locked`) and decodes answers with the standard `json` module,
looked up at call time, as an operator's client does. Answers come from files under `answers/`
(named by the SHA-256 of the request path); its own settings and usage live in JSON files.
"""
import contextlib
import hashlib
import json
from pathlib import Path


@contextlib.contextmanager
def locked(path):
    yield


class Client:
    def __init__(self, profile, root):
        self.root = Path(root)
        self.base = json.loads((self.root / 'profiles.json').read_text())[profile]['base_url']
        self.directory = self.root / profile
        self.directory.mkdir(mode=0o700, exist_ok=True)

    def _load(self):
        usage = self.directory / 'usage.json'
        return json.loads(usage.read_text()) if usage.exists() else {'used': 0}

    def status(self):
        return {'budget_used': self._load()['used'], 'daily_limit': 50000}

    def get(self, path, purpose='db-sync-alt'):
        usage = self._load()
        usage['used'] += 1
        (self.directory / 'usage.json').write_text(json.dumps(usage))
        answer = self.root / 'answers' / hashlib.sha256(path.encode()).hexdigest()
        if not answer.exists():
            raise RuntimeError('HTTP 404; not found')
        try:
            return json.loads(answer.read_bytes())
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RuntimeError('Invalid JSON response') from None
