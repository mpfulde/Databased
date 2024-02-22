from lstore.table import Table, page_directory_from_file
from lstore.bufferpool import Bufferpool
import pickle
import os

class Database:

    def __init__(self):
        self.tables = []
        self.tables_data = {}
        self.bufferpool = None
        self.path = None
        pass

    # Not required for milestone1
    def open(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
        else:
            tables_to_add = []
            file = f"{path}/tables.txt"
            if os.path.exists(file):
                with open(file, "r") as names:
                    tables_to_add = names.read().splitlines()

            for name in tables_to_add:
                table_path = self.tables_data[name].get("table_path")
                num_columns = self.tables_data[name].get("num_columns")
                key = self.tables_data[name].get("key")
                table = Table(name, num_columns, key)
                with open(f"{table_path}/page_info.dat", "rb") as page_info:
                    table.page_directory = pickle.load(page_info)
                page_info.close()

                with open(f"{table_path}/table_data.dat", "r") as table_data:
                    table.reload_data(table_data)
                table_data.close()

                with open(f"{table_path}/index_data.dat", "rb") as index_data:
                    table.index = pickle.load(index_data)
                index_data.close()

                index = self.tables_data[name].get("index")
                self.tables[index] = table

        self.path = path
        self.bufferpool = Bufferpool(path)


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
