from lstore.table import Table, Record
from lstore.lockmanager import LockManager
from lstore.index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.table = None
        self.lock_list = {}
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table is None:
            self.table = table
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # handles 2PL fails here, that way query's will always return true and will always commit
        # aborts if locking fails

        # sets up a lock
        for query, args in self.queries:
            # if table is different from the query, so we dont mess with the wrong part of the database
            if self.table.name != query.__self__.table.name:
                self.table = query.__self__.table

            key = args[0]  # will always give the record key (useful in keeping track of locks for specific records)
            lock = self.lock_list.get(key)
            if query == query.__self__.select:
                if lock is None:
                    acquired = self.table.lock_manager.acquire_new_read(key)
                    if not acquired:
                        return self.abort()
                    else:
                        self.lock_list[key] = "read"

            elif query == query.__self__.sum:
                if lock is None:
                    acquired = self.table.lock_manager.acquire_new_read(key)
                    if not acquired:
                        return self.abort()
                    else:
                        self.lock_list[key] = "read"

            # checks if select and updating in the same transaction
            elif query == query.__self__.update:
                if lock is None:
                    acquired = self.table.lock_manager.acquire_new_write(key)
                    if not acquired:
                        return self.abort()
                    else:
                        self.lock_list[key] = "write"
                elif lock == "read":
                    self.table.lock_manager.locks[key].read_lock_release()
                    acquired = self.table.lock_manager.acquire_new_write(key)
                    if not acquired:
                        return self.abort()
                    else:
                        self.lock_list[key] = "write"

            # all other queries are write
            else:
                if lock is None:
                    acquired = self.table.lock_manager.acquire_new_write(key)
                    if not acquired:
                        return self.abort()
                    else:
                        self.lock_list[key] = "write"
                elif lock == "read":
                    self.table.lock_manager.locks[key].read_lock_release()
                    acquired = self.table.lock_manager.acquire_new_write(key)
                    if not acquired:
                        return self.abort()
                    else:
                        self.lock_list[key] = "write"

        for query, args in self.queries:
            result = query(*args)
            if result is False:
                return self.abort()  # this code will probably never be reached

            # insert will abort anyway if trying to do this
            # if query == query.__self__.update:
            #     try:
            #         self.table.lock_manager.locks[key].write_lock_release()
            #     except:
            #         print("lol key")

        return self.commit()

    def abort(self):
        keys = list(self.lock_list.keys())
        for key in keys:
            if self.lock_list[key] == "read":
                self.table.lock_manager.locks[key].read_lock_release()
            elif self.lock_list[key] == "write":
                self.table.lock_manager.locks[key].write_lock_release()
        return False

    def commit(self):
        # running this assumes its all working
        keys = list(self.lock_list.keys())
        for key in keys:
            if self.lock_list[key] == "read":
                self.table.lock_manager.locks[key].read_lock_release()
            elif self.lock_list[key] == "write":
                self.table.lock_manager.locks[key].write_lock_release()

        return True
