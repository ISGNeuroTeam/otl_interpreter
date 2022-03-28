from redis import Redis
from pottery import Redlock

from core.settings import REDIS_CONNECTION_STRING
from otl_interpreter.settings import ini_config


one_process_mode = ini_config['dispatcher']['one_process_mode'].lower() == 'true'


class FakeLock:
    def acquire(self, blocking=True):
        return True

    def release(self):
        return None

    def locked(self):
        return 0


class Lock:
    def __init__(self, key):
        redis = Redis.from_url(REDIS_CONNECTION_STRING)
        if one_process_mode:
            self.lock = FakeLock()
        else:
            self.lock = Redlock(key=key, masters={redis})

    def acquire(self, blocking=True):
        return self.lock.acquire(blocking=blocking)

    def release(self):
        return self.lock.release()

    def locked(self):
        return bool(self.lock.locked())
