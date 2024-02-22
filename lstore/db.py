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

                table_json = open(f"{path}/{name}/table.json")
                table = table_from_json(table_json["metadata"])


                with open(f"{path}/{name}/page_info.dat", "rb") as page_info:
                    table.page_directory = pickle.load(page_info)
                page_info.close()

                with open(f"{path}/{name}/index_data.dat", "rb") as index_data:
                    table.index = pickle.load(index_data)
                index_data.close()

                index = self.tables_data[name].get("index")
                self.tables[index] = table

        self.root = path
        self.bufferpool = Bufferpool(path)


    def close(self):
        with open(f"{self.root}/tables.txt", "w") as table_list:
            table_list.write("\n".join(name for name in self.tables_data))
        table_list.close()

        # cleans up all the dirty bits and ensures a clean closing of the tables
        self.bufferpool.close()

        # writes all the table data to files
        for table in self.tables:
            path = f"{self.root}/{table.name}"
            table.write_to_files(path)

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
