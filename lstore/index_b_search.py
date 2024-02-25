"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from lstore.index_avl import AVLTree
from lstore.table import Record


# Tree = AVLTree()
# root = None
# root = Tree.insert_node(root, 40)
# root = Tree.insert_node(root, 60)
# root = Tree.delete_node(root, 60)
# root = Tree.insert_node(root, 60)
#
# root = Tree.insert_node(root, 50)
# root = Tree.insert_node(root, 70)

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.avl = AVLTree()
        self.indices = [None] * table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """

    def insert(self, record):
        for v in range(len(self.indices)):
            if not self.indices[v]:
                root = None
                self.indices[v] = self.avl.insert_record_index(root, record, v)
            else:
                self.indices[v] = self.avl.insert_record_index(self.indices[v], record, v)

    def update(self, primary_key, record):

        for v in range(len(self.indices)):
            self.indices[v] = self.avl.delete_record_index(self.indices[v], record, v)
            self.indices[v] = self.avl.insert_record_index(self.indices[v], record, v)

    def delete(self, primary_key, record):
        for v in range(len(self.indices)):
            self.indices[v] = self.avl.delete_record_index(self.indices[v], record, v)

    def locate(self, column, value):
        if not self.indices[0]:
            return
        else:
            records = []
            result = self.avl.search_bsearch(self.indices[column], value)
            if result:
                return result.records
            return records

    """
    # Returns the records of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if not self.indices[0]:
            return []
        else:
            records = []
            for i in range(begin, end + 1):
                records.append(self.avl.search_bsearch(self.indices[column], i).record)
            return records

    def locate_range_sum(self, begin, end, column):
        if not self.indices[0]:
            return []
        else:
            records = []
            for i in range(begin, end + 1):
                records.append(self.avl.search_bsearch(self.indices[column], i).record)
            return sum([p.columns[column] for p in records])

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
