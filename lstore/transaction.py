from lstore.table import Table, Record
from lstore.lockmanager import LockManager
from lstore.index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.lock_manager = LockManager()
        self.table = None
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
            if query == query.__self__.select:
                acquired = self.table.lock_manager.acquire_new_read(key)
                if not acquired:
                    return self.abort()

            elif query == query.__self__.sum:
                acquired = self.table.lock_manager.acquire_new_read(key)
                if not acquired:
                    return self.abort()

            # all other queries are write
            else:
                acquired = self.table.lock_manager.acquire_new_write(key)
                if not acquired:
                    return self.abort()

        for query, args in self.queries:
            result = query(*args)
            if result is False:
                return self.abort()  # this code will probably never be reached

        return self.commit()

    def abort(self):
        for query, args in self.queries:
            if self.table.name != query.__self__.table.name:
                self.table = query.__self__.table

            self.table.lock_manager.release_all_locks()

        return False

    def commit(self):
        # running this assumes its all working
        for query, args in self.queries:
            if self.table.name != query.__self__.table.name:
                self.table = query.__self__.table

            self.table.lock_manager.release_all_locks()

        return True
