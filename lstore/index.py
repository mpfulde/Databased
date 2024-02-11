from lstore.table import Table
from lstore.page import Page, PageRange

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] * table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    # will update with milestone 2, right now mapped 1 to 1
    """

    def locate(self, column, value):
        for page_range in self.table.page_ranges:
            key_page = page_range.TailPages[self.indices[column]]
            rid_page = page_range.BasePages[3]  # this will change with indexing
            while key_page is not None:
                for i in range(512):  # reads each row
                    if key_page.read(i) is value:
                        return rid_page.read(i)
                    else:
                        key_page = key_page.parent
                        rid_page = rid_page.parent

        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):

        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
