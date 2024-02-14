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
        # print(value)
        tid_list = []
        rid_list = -1
        for page_range in self.table.page_ranges:
            column_page = page_range.BasePages[column + 4]
            rid_page = page_range.BasePages[1]  # this will change with indexing
            while column_page is not None:
                row = column_page.contains(value)
                # print(row)
                if row == -1:
                    column_page = column_page.child
                    rid_page = rid_page.child
                else:
                    # needs to check the tail pages too
                    rid = rid_page.read(row)
                    rid_list = rid
                    break

            if rid_list == -1:
                continue



            tid_list = []
            if column_page is None:
                return rid_list, tid_list
            # gets all updates to the record with rid
            for i in range(len(column_page.TailPages)):
                update_list = column_page.TailPages[i].find_all(value)
                for j in range(len(update_list)):
                    tid = rid_page.TailPages[i].read(update_list[j])
                    tid_list.append(tid)

        return rid_list, tid_list

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):

        tid_list = []
        rid_list = []
        for page_range in self.table.page_ranges:
            column_page = page_range.BasePages[column + 4]
            rid_page = page_range.BasePages[1]  # this will change with indexing
            while column_page is not None:
                row = rid_page.contains(begin)

            tid_list = []
            # gets all updates to the record with rid
            for i in range(len(column_page.TailPages)):
                update_list = column_page.TailPages[i].find_all(value)
                for j in range(len(update_list)):
                    tid = rid_page.TailPages[i].read(update_list[j])
                    tid_list.append(tid)

        return rid_list, tid_list

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
