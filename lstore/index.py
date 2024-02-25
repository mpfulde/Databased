import math

from lstore.page import Page, PageRange
from lstore.config import *

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""


# A very basic b-tree implementation to handle Index creation
class IndexNode:
    def __init__(self, leaf):
        self.leaf = leaf
        self.keys = []  # list of keys per node, each key is a tuple (value _ ridlist)
        self.child = []


class IndexTree:
    def __init__(self, node_size):
        self.root = IndexNode(True)
        self.node_size = node_size
        pass

    def insert(self, value, rid):
        node = self.root
        if len(node.keys) == self.node_size:
            temp = IndexNode(False)
            self.root = temp
            temp.child.insert(0, node)
            self.split_full_node(temp, 0)
            self.recursive_insert(temp, value, rid)
        else:
            self.recursive_insert(node, value, rid)

    # the actual insert function, called recursively to insert the values
    def recursive_insert(self, node, value, rid):
        index = len(node.keys) - 1
        if node.leaf:
            node.keys.append((None, []))
            while index >= 0 and value < node.keys[index][0]:
                node.keys[index + 1] = node.keys[index]
                index -= 1
            if node.keys[index][0] == value:
                node.keys[index][1].append(rid)
                node.keys.pop(index + 1)
                return
            if node.keys[index + 1][0] == value:
                new_key = (value, node.keys[index + 1][1].append(rid))
            else:
                new_key = (value, [rid])

            node.keys[index + 1] = new_key
        else:
            while index >= 0 and value < node.keys[index][0]:
                index -= 1
            index += 1
            if len(node.keys) == self.node_size:
                self.split_full_node(node, index)
                if value > node.keys[index][0]:
                    index += 1
            self.recursive_insert(node.child[index], value, rid)

    # if a node is full, split into children
    def split_full_node(self, node, index):
        size = self.node_size
        child = node.child[index]
        new_node = IndexNode(child.leaf)
        node.child.insert(index + 1, new_node)
        node.keys.insert(index, child.keys[math.floor(size / 2) - 1])
        new_node.keys = child.keys[math.floor(size / 2): size]
        child.keys = child.keys[0: math.floor(size / 2) - 1]
        if not child.leaf:
            node.child = child.child[math.floor(size / 2): size]
            child.child = child.child[0:math.floor(size / 2)]

        pass

    def get_rids(self, value, node=None):
        if node is None:
            return self.get_rids(value, self.root)

        else:
            index = 0
            while index < len(node.keys) and value > node.keys[index][0]:
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


# represents a single indice
class Indices:

    def __init__(self, table, column):
        self.column = column

        self.value_tree = IndexTree(TREE_SIZE)
        self.latest_rid = table.num_records - 1  # knows where to pick up when updating
        # populate the index with the table data
        table.bufferpool.commit_pool()

        last_page_range = table.page_directory[self.latest_rid].get("page_range")
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

                for row in range(0, PAGE_SIZE / NO_BYTES):
                    value = column_page.read(row)
                    rid = column_page.read(row)
                    indirection = column_page.read(row)
                    schema_encoding = column_page.read(row)

                    # Only none if last page
                    if value is None:
                        break

                    # skip the row/deleted
                    if indirection == -1:
                        continue

                    update = False
                    while i in range(table.num_columns):
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
