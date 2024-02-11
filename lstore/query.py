from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.num_updates = 0
        self.table = table
        print(self.table.num_columns)

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = self.table.get_rid_from_key(primary_key)
        try:
            self.table.delete_record(rid)
        except:
            return False
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        print("attempting to insert")

        schema_encoding = '0' * self.table.num_columns
        
        # converts columns to a list for ease of use
        cols = list(columns)
        print(self.table.num_columns)
        print(self.table.name)

        if len(cols) > self.table.num_columns:
            print ("trying to insert too many columns")
            return False

        if len(cols) < self.table.num_columns:
            print ("trying to insert too few columns")
            # while len(cols) < self.table.num_columns:
            #    cols.append(None)
            return False

        try:
            record = Record(self.table.new_rid(), schema_encoding, cols[0], cols)
            self.table.write_record(record)
        except:
            print("Something went wrong please see exception list")
            return False

        # can only get here if write was successful
        return True

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # version will be negative but its easier to deal with when positive
        version = abs(relative_version)

        # if version doesn't exist yet, we just want to deal with the latest (according to the tester)
        if version > self.num_updates:
            version = 0

        if version is self.num_updates:
            # logic for looking at values for base pages
            result = self.table.get_base_columns(projected_columns_index)
            return result

        result = self.table.get_column_with_indirection(search_key, projected_columns_index, relative_version)
        return result

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        cols = list(columns)

        try:
            old_rid = self.table.get_row(primary_key)
            old_record = self.table.readRecord(old_rid)
            new_schema = old_record.schema_encoding
            updated_columns = old_record.columns
            for i in range(len(cols)):
                if not cols[i] == None:
                    updated_columns[i] = cols[i]
                    new_schema[i] = '1'
                else:
                    new_schema[i] = '0'
                    continue
            updated_record = Record(self.table.new_rid(), new_schema, primary_key, updated_columns)
            self.table.update_record(old_rid, updated_record)
        except:
            print ("Something went wrong check exception")
            return False

        pass
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
