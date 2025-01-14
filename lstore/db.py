from lstore.table import Table


class Database:

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index):
        # print("creating table named: " + name + "with num_cols: " + str(num_columns))
        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                self.tables[i].delete_table()
                self.tables.remove(self.tables[i])
                pass

        print("no table of that name found")
        pass

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table

        print("no table of that name found")
        pass
