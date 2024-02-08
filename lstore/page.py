
MAX_PAGES = 8
RECORD_COLUMNS = 4 # 4 constant columns for all tables (definey in lstore/table.py)

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    # outside of setup, exclusive to tail page
    def write(self, value):
        self.num_records += 1
        pass

class PageRange:

    def __init__(self, num_columns, key)
        self.BasePages = [Page() for i in range(MAX_PAGES)]

        # add logic to link BasePages and connect a tailpage

        self.TailPages = Page()

