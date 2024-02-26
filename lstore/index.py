import math

from lstore.page import Page, PageRange
from lstore.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""


# A very basic b-tree implementation to handle Index creation
class IndexNode:

    def __init__(self, size, leaf):
        self.leaf = leaf  # Is true when node is leaf. Otherwise false
        self.size = size
        self.keys = [(None, [None])] * size
        self.num_keys = 0
        self.child = [None] * size

    # the actual insert function, called recursively to insert the values
    def insert(self, value, rid):
        index = self.num_keys - 1
        if self.leaf:
            while index >= 0 and self.keys[index][0] > value:
                self.keys[index + 1] = self.keys[index]
                index -= 1
            self.keys[index + 1] = (value, [rid])
            self.num_keys += 1
        else:
            while index >= 0 and self.keys[index][0] > value:
                index -= 1
            if self.child[index + 1].num_keys == self.size:
                self.split_nodes(index + 1, self.child[index + 1])
                if self.keys[index + 1][0] < value:
                    index += 1
            self.child[index + 1].insert(value, rid)

    def split_nodes(self, index, node):
        new_node = IndexNode(node.size, node.leaf)
        new_node.num_keys = math.floor(self.size/2) - 1
        for j in range(math.floor(self.size/2) - 1):
            new_node.keys[j] = node.keys[j + math.floor(self.size/2)]
        if not node.leaf:
            for j in range(math.floor(self.size/2)):
                new_node.child[j] = node.child[j + math.floor(self.size/2)]
        node.num_keys = math.floor(self.size/2) - 1
        for j in range(self.num_keys, index, -1):
            self.child[j + 1] = self.child[j]
        self.child[index + 1] = new_node
        for j in range(self.num_keys - 1, index - 1, -1):
            self.keys[j + 1] = self.keys[j]
        self.keys[index] = node.keys[math.floor(self.size/2) - 1]
        self.num_keys += 1

class IndexTree:
    def __init__(self, node_size):
        self.root = None
        self.node_size = node_size
        pass

    def insert(self, value, rid):



        if self.root is None:
            self.root = IndexNode(self.node_size, True)
            self.root.keys[0] = (value, [rid])  # Insert key
            self.root.num_keys = 1
        else:
            if self.get_rids(value) is not None:
                self.insert_to_existing_node(value, rid)
                return

            if self.root.num_keys == self.node_size:
                new_node = IndexNode(self.node_size, False)
                new_node.child[0] = self.root
                new_node.split_nodes(0, self.root)
                index = 0
                if new_node.keys[0][0] < value:
                    index += 1
                new_node.child[index].insert(value, rid)
                self.root = new_node
            else:
                self.root.insert(value, rid)

    def get_rids(self, value, node=None):
        if node is None:
            return self.get_rids(value, self.root)

        else:
            index = 0
            while index < node.num_keys and value > node.keys[index][0]:
                index += 1
            if index < len(node.keys) and value == node.keys[index][0]:
                return node.keys[index][1]
            elif node.leaf:
                return None
            else:
                return self.get_rids(value, node.child[index])

    def remove_value(self, value, node=None):
        if node is None:
            self.get_rids(value, self.root)

        else:
            index = 0
            while index < len(node.keys) and value > node.keys[index][0]:
                index += 1
            if index < len(node.keys) and value == node.keys[index][0]:
                node.keys.pop(index)
            elif node.leaf:
                return
            else:
                self.get_rids(value, node.child[index])

    def insert_to_existing_node(self, value, rid, node=None):
        if node is None:
            self.insert_to_existing_node(value, rid, self.root)

        else:
            index = 0
            while index < node.num_keys and value > node.keys[index][0]:
                index += 1
            if index < node.num_keys and value == node.keys[index][0]:
                new_rids = node.keys[index][1]
                new_rids.append(rid)
                new_key = list(node.keys[index])
                new_key[1] = new_rids
                node.keys[index] = tuple(new_key)
            elif node.leaf:
                return False
            else:
                self.insert_to_existing_node(value, rid, node.child[index])


# represents a single indice
class Indices:

    def __init__(self, table, column):
        self.column = column

        self.value_tree = IndexTree(TREE_SIZE)
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

                        self.value_tree.insert(value, tid)

                    else:
                        self.value_tree.insert(value, rid)

    def get_rids(self, value):
        return self.value_tree.get_rids(value)

    def update_tree(self, value, rid):
        self.value_tree.insert(value, rid)

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
