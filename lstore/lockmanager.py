import threading

# 2PL implementation
class TwoPL:
    def __init__(self):
        self.lock = threading.Lock()
        self.readers = 0 # count of current
        self.writer = False


    def read_lock_acquire(self):
        self.lock.acquire()

        if self.writer:
            self.lock.release()
            return False
        else:
            self.lock.release()
            self.readers += 1
            return True
        pass

    def write_lock_acquire(self):
        self.lock.acquire()

        # cannot be a write lock unless
        if self.readers != 0:
            self.lock.release()
            return False

        # writers are exclusive, so we cannot acquire multiple (by def of 2pl)
        elif self.writer:
            self.lock.release()
            return False

        # writer is good to go
        else:
            self.writer = True
            self.lock.release()
            return True

    def read_lock_release(self):
        self.lock.acquire()
        try:
            self.readers -= 1

            # double release shouldnt do anything (but will mess with the reader count)
            if self.readers < 0:
                self.readers = 0

        except:
            return False

        self.lock.release()
        return True

    def write_lock_release(self):
        self.lock.acquire()

        try:
            self.writer = False
        except:
            return False

        self.lock.release()
        return True

class LockManager:
    def __init__(self):
        self.locks = {}
        pass

    def release_all_reads(self):
        for lock in self.locks:
            lock.read_lock_release()
        pass

    def release_all_writers(self):
        for lock in self.locks:
            lock.write_lock_release()
        pass

    def release_all_locks(self):
        self.release_all_reads()
        self.release_all_writers()

    def acquire_new_read(self, key):

        if key < 0:
            return False

        if key in self.locks:
            new_lock = self.locks[key]
        else:
            new_lock = TwoPL()

        try:
            attempt = new_lock.read_lock_acquire()
        except:
            return False

        if not attempt:
            return False

        self.locks[key] = new_lock

        return True

    def acquire_new_write(self, key):

        if key < 0:
            return False

        if key in self.locks:
            new_lock = self.locks[key]
        else:
            new_lock = TwoPL()


        try:
            attempt = new_lock.write_lock_acquire()
        except:
            return False

        if not attempt:
            return False

        self.locks[key] = new_lock

        return True