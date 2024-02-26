import math

from lstore.page import Page, PageRange
from lstore.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

# represents a single indice
class Indices:

    def __init__(self, table, column):
        self.column = column

        self.value_tree = {} # test with dictionary
        self.latest_rid = table.num_records - 1  # knows where to pick up when updating
        # populate the index with the table data
        if table.bufferpool is not None:
            table.bufferpool.commit_pool()

        # nothing to add to the table
        if self.latest_rid == -1:
            return

        last_page_range = table.page_directory[self.latest_rid].get("page_range") + 1
        last_base_page = table.page_directory[self.latest_rid].get("page")
        for i in range(0, last_page_range):
            for j in range(0, table.page_ranges[i].base_page_count):
                rid_page = Page("rid")
                rid_page.read_from_path(f"{table.path}/{i}/BasePages/{j}/rid.page")
                column_page = Page(column)
                column_page.read_from_path(f"{table.path}/{i}/BasePages/{j}/column_{column}.page")
                indirection_page = Page("Indirection")
                indirection_page.read_from_path(f"{table.path}/{i}/BasePages/{j}/indirection.page")
                schema_page = Page("Schema encoding")
                schema_page.read_from_path(f"{table.path}/{i}/BasePages/{j}/schema_encoding.page")

                rows = math.floor(PAGE_SIZE/NO_BYTES)
                if j == last_base_page:
                    rows = table.page_directory[self.latest_rid].get("row")

                for row in range(0, rows):
                    value = column_page.read(row)
                    rid = rid_page.read(row)
                    indirection = indirection_page.read(row)
                    schema_encoding = schema_page.read(row)

                    # Only none if last page
                    if value is None:
                        break

                    # skip the row/deleted
                    if indirection == -1:
                        continue

                    update = False
                    for i in range(table.num_columns):
                        bit = schema_encoding >> i
                        bit %= 10
                        if bit != 0:
                            update = True
                            break


                    if update:
                        tail_page = table.page_ranges[i].tail_directory[indirection].get("page")
                        tail_page_row = table.page_ranges[i].tail_directory[indirection].get("row")
                        tail_page_column = Page(column)
                        tail_page_column.read_from_path(f"{table.path}/{i}/TailPages/{tail_page}/column_{column}.page")
                        tail_rid_page = Page("rid")
                        tail_rid_page.read_from_path(f"{table.path}/{i}/TailPages/{tail_page}/rid.page")
                        value = tail_page_column.read(tail_page_row)
                        tid = tail_page_column.read(tail_page_row)

                        rid_list = self.value_tree[value].get("rid_list")
                        rid_list.append(tid)
                        self.value_tree[value]["rid_list"] = rid_list

                    else:
                        rid_list = self.value_tree[value].get("rid_list")
                        rid_list.append(rid)
                        self.value_tree[value]["rid_list"] = rid_list

    def get_rids(self, value):
        return self.value_tree[value].get("rid_list")

    def update_tree(self, value, rid):
        if value in self.value_tree:
            rid_list = self.value_tree[value].get("rid_list")
            rid_list.append(rid)
            self.value_tree[value]["rid_list"] = rid_list
        else:
            self.value_tree[value] = {"rid_list": [rid]}


    def remove_from_tree(self, value):
        self.value_tree.remove_value(value)


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
        indice = self.indices[column]
        rids = indice.get_rids(value)
        return rids

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):

        rid_list = []

        for i in range(begin, end):
            value = self.locate(column, i)
            if value is not None:
                if len(value) > 1:
                    for rid in value:
                        rid_list.append(rid)
                else:
                    rid_list.append(value[0])

        return rid_list

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = Indices(self.table, column_number)
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices.pop(column_number)
        pass
