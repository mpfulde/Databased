from lstore.table import Table, table_from_json
from lstore.bufferpool import Bufferpool
import pickle
import os
import json

class Database:

    def __init__(self):
        self.tables = []
        self.bufferpool = None
        self.root = None
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

                    with open(f"{path}/{name}/table_data.json", "r") as metadata:
                        table_json = metadata.read()
                        table = table_from_json(table_json)
                    metadata.close()

                    with open(f"{path}/{name}/page_info.dat", "rb") as page_info:
                        table.page_directory = pickle.load(page_info)
                    page_info.close()

                    with open(f"{path}/{name}/index_data.dat", "rb") as index_data:
                        table.index = pickle.load(index_data)
                    index_data.close()

                    self.tables.append(table)

        self.root = path
        self.bufferpool = Bufferpool(path)


    def close(self):
        with open(f"{self.root}/tables.txt", "w") as table_list:
            table_list.write("\n".join(table.name for table in self.tables))
        table_list.close()



        # writes all the table data to files
        for table in self.tables:
            table.write_to_files()
            self.drop_table(table.name)

        # cleans up all the dirty bits and ensures a clean closing of the tables
        self.bufferpool.close()


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index):
        if not self.get_table(name):
            # print("creating table named: " + name + "with num_cols: " + str(num_columns))
            table = Table(name, num_columns, key_index, f"{self.root}/{name}")
            self.tables.append(table)
            table.index.create_index(key_index)
        else:
            print("table of that name already exists")
            table = self.get_table(name)

        table.bufferpool = self.bufferpool

        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                # self.tables[i].delete_table()
                self.tables.remove(self.tables[i])
                break

        pass

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                table.bufferpool = self.bufferpool
                return table

        print("no table of that name found")
        return False
