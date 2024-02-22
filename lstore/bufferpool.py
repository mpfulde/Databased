from config import *
PAGES_IN_POOL = 0

class Bufferpool:
    def __init__(self, path):
        self.path = path
        self.pages = []
        self.page_directory = {} # holds different values from the on in table.py
        pass

    # returns false if record is not in the pool
    def is_record_loaded(self, table, rid):
        if self.page_directory[rid] is not None:
            return True


        return False


    def load_page(self, base_page, column, page_range, rid ):
        pass

    def full(self):
        if len(self.pages) >= MAX_BUFFERPOOL_PAGES:
            return True
        return False