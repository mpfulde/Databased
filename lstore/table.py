from lstore.index import Index
from lstore.page import Page, PageRange
import datetime
import math
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

RECORDS_PER_PAGE = 4096 / 8  # 4kb / 8 byte ints
MAX_PAGES = 16  # max number of pages in page range (based off example exclusively)

class Record:

    def __init__(self, rid, schema_encoding, key, columns):
        self.rid = rid
        self.timestamp = datetime.now()
        self.schema_encoding = schema_encoding
        self.key = key
        self.columns = columns

    def createList(self):
        list = [None] * (len(self.columns) + 4)
        list[INDIRECTION_COLUMN] = 1
        list[RID_COLUMN] = self.rid
        list[TIMESTAMP_COLUMN] = self.timestamp
        list[SCHEMA_ENCODING_COLUMN] = self.schema_encoding
        list[4:len(list)] = self.columns
        return list

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_records = 0
        self.page_ranges = [PageRange(num_columns)]
        self.page_directory = {}
        self.index = Index(self)
        pass

    # does this one column at a time (will change later)
    def write_record(self, record):
        # grabs the current page range from the page range hashmap
        page_range_id = self.page_directory[record.rid].get("page_range")
        page_range = self.page_ranges[page_range_id]
        successful_write = page_range.write_record(record)
        return successful_write

    def read_record(self, rid):

        pass

    def new_rid(self):
        rid = self.num_records
        self.num_records += 1

        # determines what page range we are on (i.e. if we are past the 16 page mark)
        page_range_id = math.floor(rid / (MAX_PAGES * RECORDS_PER_PAGE))

        # gets the row we are on
        row = rid % (MAX_PAGES * RECORDS_PER_PAGE)
        page = math.floor(row / (MAX_PAGES * RECORDS_PER_PAGE))

        #


        if page_range_id >= len(self.page_ranges):
            self.add_new_page_range()

        self.page_directory[rid] = {
            'page_range': page_range_id,
            'row': row,
            'page': page
        }

        return rid

    def get_rid_from_key(self, key):
        self.key = key
        return key

    # clean up function so we dont lose memory
    def delete_table(self):
        for page_range in self.page_ranges:
            page_range.clear_data()
            page_range.BasePages.clear()
            page_range.TailPages.clear()

        self.page_ranges.clear()
        self.page_directory.clear()


    def __merge(self):
        pass

    def add_new_page_range(self):
        self.page_ranges.append(PageRange(self.num_records))
        pass