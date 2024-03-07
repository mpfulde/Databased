import threading

# 2PL implementation
class TwoPL:
    def __init__(self):
        self.lock = threading.lock()


    def read_lock_acquire(self):
        pass

    def write_lock_acquire(self):
        pass

    def read_lock_release(self):
        pass

    def write_lock_release(self):
        pass