# SPDX-License-Identifier: Apache-2.0
"""Several Blockfrost requests of one update in flight (`sync.provider_concurrency`) and a pace for them
(`sync.provider_max_requests_per_hour`).

The traversals of an update request their answers one after another, in the same order with any number of requests
in flight. Where they know the exact paths they request next, they hint them (`ProviderClient.prefetch`): the
accounted provider (`sync.AccountedProvider`) sends up to N - 1 of them ahead on worker threads, after the same checks
and the same durable job reservation as a direct request, and hands each answer to the traversal when it requests that
path. Workers only call the client; every check, receipt and store write stays on the update thread.
"""
import collections
import queue
import threading
import time
from concurrent.futures import Future

MAX_CONCURRENCY = 8
MAX_REQUESTS_PER_HOUR = 28800


class Pacer:
    """Sends at least `3600 / per_hour` seconds apart, across threads: each send takes the next slot under a lock and
    waits for it without the lock."""
    def __init__(self, per_hour, *, clock=time.monotonic, sleep=time.sleep):
        self.interval, self.clock, self.sleep = 3600 / per_hour, clock, sleep
        self._lock, self._next = threading.Lock(), None

    def wait(self):
        with self._lock:
            now = self.clock()
            slot = now if self._next is None else max(now, self._next)
            self._next = slot + self.interval
        if slot > now:
            self.sleep(slot - now)


class Dispatcher:
    """The sync driver's sends: at most `size` in flight at once, the update thread's own included, each after its
    slot when `pacer` is set. Its worker threads are daemon threads, started on first use."""
    def __init__(self, size, pacer=None):
        self.size, self.pacer = size, pacer
        self._slots = threading.BoundedSemaphore(size)
        self._tasks = queue.SimpleQueue()
        self._workers = 0
        self._lock = threading.Lock()

    def call(self, send):
        """`send()` on the calling thread."""
        with self._slots:
            if self.pacer is not None:
                self.pacer.wait()
            return send()

    def submit(self, send, dropped):
        """A future of `send()` on a worker thread. A request dropped (`dropped()`) before it is sent is not sent."""
        with self._lock:
            if self._workers < self.size:
                self._workers += 1
                threading.Thread(target=self._work, name='db-sync-alt-request-%d' % self._workers, daemon=True).start()
        future = Future()
        self._tasks.put((future, send, dropped))
        return future

    def _work(self):
        while True:
            future, send, dropped = self._tasks.get()
            if not future.set_running_or_notify_cancel():
                continue
            try:
                with self._slots:
                    if self.pacer is not None:
                        self.pacer.wait()
                    result = None if dropped() else send()
            except BaseException as error:
                future.set_exception(error)
            else:
                future.set_result(result)


class Ahead:
    """A request sent ahead: its path, its reserved job attempt, the cut of its answer and its outcome."""
    def __init__(self, path, attempt, cut):
        self.path, self.attempt, self.cut = path, attempt, cut
        self.future, self.dropped = None, False


class Window:
    """The paths hinted during one block of work, in the order the traversals request them: `queued` not sent yet,
    `sent` sent and not consumed yet, oldest first. At most `size - 1` are sent ahead: the update thread's own request
    is the last of `size`."""
    def __init__(self, size):
        self.size = size
        self.queued = collections.deque()
        self.sent = []
        self.scopes = 0

    def room(self):
        return min(self.size - 1 - len(self.sent), len(self.queued))

    def take(self, path):
        """The oldest request of `path` sent ahead, out of the window; else None, and the oldest queued hint of
        `path` (the request it named is sent directly now) leaves the queue."""
        for index, ahead in enumerate(self.sent):
            if ahead.path == path:
                return self.sent.pop(index)
        try:
            self.queued.remove(path)
        except ValueError:
            pass
        return None

    def drop(self):
        """Forget every hint: a queued one is never sent, one sent ahead stays counted and is never read."""
        for ahead in self.sent:
            ahead.dropped = True
            ahead.future.cancel()
        self.sent.clear()
        self.queued.clear()
