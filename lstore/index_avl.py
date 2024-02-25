class avl_Node(object):
    def __init__(self, record, column):
        self.record = record
        self.column = column
        self.left = None
        self.right = None
        self.height = 1
        self.records = [record]


class AVLTree(object):

    def insert_record_index(self, root, record, column):

        if not root:
            return avl_Node(record, column)
        elif record.columns[column] < root.record.columns[column]:
            root.left = self.insert_record_index(root.left, record, column)
        elif record.columns[column] > root.record.columns[column]:
            root.right = self.insert_record_index(root.right, record, column)
        else:
            root.records.append(record)
            return root

        root.height = 1 + max(self.avl_height(root.left),
                              self.avl_height(root.right))

        balance_factor = self.avl_balance_factor(root)
        if balance_factor > 1:
            if record.columns[column] < root.left.record.columns[column]:
                return self.right_rotate(root)
            else:
                root.left = self.left_rotate(root.left)
                return self.right_rotate(root)

        if balance_factor < -1:
            if record.columns[column] > root.right.record.columns[column]:
                return self.left_rotate(root)
            else:
                root.right = self.right_rotate(root.right)
                return self.left_rotate(root)

        return root

    @staticmethod
    def avl_height(root):
        if not root:
            return 0
        return root.height

    def avl_balance_factor(self, root):
        if not root:
            return 0
        return self.avl_height(root.left) - self.avl_height(root.right)

    def avl_min_value(self, root):
        if root is None or root.left is None:
            return root
        return self.avl_min_value(root.left)

    def left_rotate(self, b):
        a = b.right
        T2 = a.left
        a.left = b
        b.right = T2
        b.height = 1 + max(self.avl_height(b.left),
                           self.avl_height(b.right))
        a.height = 1 + max(self.avl_height(a.left),
                           self.avl_height(a.right))
        return a

    def right_rotate(self, b):
        a = b.left
        T3 = a.right
        a.right = b
        b.left = T3
        b.height = 1 + max(self.avl_height(b.left),
                           self.avl_height(b.right))
        a.height = 1 + max(self.avl_height(a.left),
                           self.avl_height(a.right))
        return a

    def delete_record_index(self, root, record, column):

        if not root:
            return root
        elif record.columns[column] < root.record.columns[column]:
            root.left = self.delete_record_index(root.left, record, column)
        elif record.columns[column] > root.record.columns[column]:
            root.right = self.delete_record_index(root.right, record, column)
        else:
            if root.left is None:
                temp = root.right
                root = None
                return temp
            elif root.right is None:
                temp = root.left
                root = None
                return temp
            temp = self.avl_min_value(root.right)
            root.record.columns[column] = temp.record.columns[column]
            root.right = self.delete_record_index(root.right, record, column)
        if root is None:
            return root

        root.height = 1 + max(self.avl_height(root.left), self.avl_height(root.right))
        balance_factor = self.avl_balance_factor(root)

        if balance_factor > 1:
            if self.avl_balance_factor(root.left) >= 0:
                return self.right_rotate(root)
            else:
                root.left = self.left_rotate(root.left)
                return self.right_rotate(root)
        if balance_factor < -1:
            if self.avl_balance_factor(root.right) <= 0:
                return self.left_rotate(root)
            else:
                root.right = self.right_rotate(root.right)
                return self.left_rotate(root)
        return root

    def search_bsearch(self, root, target):
        if root is None or root.record.columns[root.column] == target:
            return root
        if target < root.value:
            return self.search_bsearch(root.left, target)
        else:
            return self.search_bsearch(root.right, target)

# Tree = AVLTree()
# root = None
# root = Tree.insert_node(root, 40)
# root = Tree.insert_node(root, 60)
# root = Tree.delete_node(root, 60)
# root = Tree.insert_node(root, 60)
#
# root = Tree.insert_node(root, 50)
# root = Tree.insert_node(root, 70)
#
# print("PREORDER")
# Tree.preOrder(root)
# print()
# print(Tree.search_bsearch(root, 80).value)
