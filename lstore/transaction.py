from lstore.table import Table, Record
from lstore.index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.keys_in_use = {}
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

        for query, args in self.queries:
            if self.table != query.__self__.table:
                self.table = query.__self__.table

            key = args[0]
            current_lock = self.keys_in_use.get("key")

            if query == query.__self__.insert:
                if current_lock is None:
                    attempt = self.table.lock_manager.acquire_new_write(key)
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
                pass
            elif query == query.__self__.select:
                if current_lock is None:
                    attempt = self.table.lock_manager.acquire_new_read(key)
                    if attempt:
                        self.keys_in_use[key] = "reader"
                    else:
                        self.abort()
                pass
            elif query == query.__self__.update:
                if current_lock is "reader":
                    self.table.lock_manager.release_read_lock(key)
                    attempt = self.table.lock_manager.acquire_new_write()
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
                elif current_lock is None:
                    attempt = self.table.lock_manager.acquire_new_write(key)
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
                pass
            elif query == query.__self__.sum:
                if current_lock is None:
                    attempt = self.table.lock_manager.acquire_new_read(key)
                    if attempt:
                        self.keys_in_use[key] = "reader"
                    else:
                        self.abort()
                pass
            elif query == query.__self__.delete:
                if current_lock is "reader":
                    self.table.lock_manager.release_read_lock(key)
                    attempt = self.table.lock_manager.acquire_new_write()
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
                elif current_lock is None:
                    attempt = self.table.lock_manager.acquire_new_write(key)
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
            else: # additional queries are handled like update (versions are not supported)
                if current_lock is "reader":
                    self.table.lock_manager.release_read_lock(key)
                    attempt = self.table.lock_manager.acquire_new_write()
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
                elif current_lock is None:
                    attempt = self.table.lock_manager.acquire_new_write(key)
                    if attempt:
                        self.keys_in_use[key] = "writer"
                    else:
                        self.abort()
                pass


        # any query will abort before the table is actually changed so no rollback required
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        for key in list(self.keys_in_use.keys()):
            if self.keys_in_use[key] == "reader":
                self.table.lock_manager.release_read_lock(key)
            elif self.keys_in_use[key] == "writer":
                self.table.lock_manager.release_write_lock(key)
        return False

    def commit(self):
        for key in list(self.keys_in_use.keys()):
            if self.keys_in_use[key] == "reader":
                self.table.lock_manager.release_read_lock(key)
            elif self.keys_in_use[key] == "writer":
                self.table.lock_manager.release_write_lock(key)
        return True