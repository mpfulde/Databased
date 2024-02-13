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
        print(value)
        rid_list = []
        for page_range in self.table.page_ranges:
            column_page = page_range.BasePages[column + 4]
            rid_page = page_range.BasePages[1]  # this will change with indexing
            while column_page is not None:
                row = column_page.contains(value)
                print(row)
                if row is -1:
                    column_page = column_page.child
                    rid_page = rid_page.child
                else:
                    # needs to check the tail pages too
                    rid = rid_page.read(row)
                    rid_list.append(rid)
                    break

            # checks tail pages if not found in the base pages
            column_page = page_range.TailPages[column + 4]
            rid_page = page_range.TailPages[1]
            while column_page is not None:
                row = column_page.find_all(value)
                if len(row) == 0:
                    column_page = column_page.child
                    rid_page = rid_page.child
                else:
                    for r in row:
                        rid = rid_page.read(r)
                        rid_list.append(rid)

                    continue
        return -1

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):

        rid_list = []
        pos = begin

        for page_range in self.table.page_ranges:
            base_page = page_range.BasePages[3]
            while base_page is not None:
                row = base_page.contains(pos)
                if row is -1:
                    base_page = base_page.child
                else:
                    rid = base_page.read(row)
                    pos += 1
                    if rid is end:
                        rid_list.append(rid)
                        break
                    else:
                        rid_list.append(rid)

        return rid_list

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
