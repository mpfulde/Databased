import threading

# 2PL implementation
class TwoPL:
    def __init__(self):
        self.lock = threading.lock()


    def read_lock_acquire(self):
        self.lock.acquire()
        pass

    def write_lock_acquire(self):
        self.lock.acquire()
        pass

    def read_lock_release(self):
        self.lock.release()
        pass

    def write_lock_release(self):
        self.lock.release()
        pass

class LockManager:
    def __init__(self):
        self.read_list = {}
        self.write_list = {}
        pass

    def release_all_reads(self, locks):
        for lock in self.read_list[locks]:
            lock.read_lock_release()
        pass

    def release_all_writers(self, locks):
        for lock in self.write_list[locks]:
            self.write_list[lock].write_lock_release()
        pass
