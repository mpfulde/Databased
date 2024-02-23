import os.path
import pickle
import json

from lstore.index import Index
from lstore.page import Page, PageRange
# from lstore.bufferpool import Bufferpool
import math
from time import time

INDIRECTION_COLUMN = 0  # VALUE SHOULD BE AN RID
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

RECORDS_PER_PAGE = 4096 / 8  # 4kb / 8 byte ints
MAX_PAGES = 16  # max number of pages in page range (based off example exclusively)


def record_from_list(rlist, original):
    record = Record(rlist[RID_COLUMN], rlist[SCHEMA_ENCODING_COLUMN], rlist[4], rlist[4:(len(rlist))], original)
    record.timestamp = rlist[TIMESTAMP_COLUMN]
    return record


def table_from_json(json_):
    metadata = json_["metadata"]
    table = Table(metadata['name'], metadata['num_columns'], metadata['key'], metadata['path'])
    table.num_records = metadata['num_records']

    if len(json_) > 1:
        table.page_ranges.clear()

    for key in json_:

        if key != "metadata":
            if not os.path.exists(f"{metadata['path']}/{key}"):
                os.mkdir(f"{metadata['path']}/{key}")

            range_ = PageRange(metadata['num_columns'])
            range_.base_page_count = json_[key]["base_page_count"]
            range_.tail_page_count = json_[key]["tail_page_count"]
            range_.tail_records = json_[key]["tail_directory"]
            table.page_ranges.append(range_)

            pass

    return table


class Record:

    def __init__(self, rid, schema_encoding, key, columns, original):
        self.rid = rid
        self.timestamp = round(time())
        self.schema_encoding = schema_encoding
        self.key = key
        self.columns = columns
        self.original = original

    def create_list(self):
        list_ = [None] * (len(self.columns) + 4)
        list_[INDIRECTION_COLUMN] = self.rid  # indirection should point to itself by default
        list_[RID_COLUMN] = self.rid
        list_[TIMESTAMP_COLUMN] = self.timestamp
        list_[SCHEMA_ENCODING_COLUMN] = self.schema_encoding
        list_[4:len(list_)] = self.columns
        return list_


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, path):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_records = 0
        self.path = path
        self.page_ranges = [PageRange(num_columns)]
        self.page_directory = {}
        self.index = Index(self)
        pass

    # does this one column at a time (will change later)
    def write_record(self, record):
        # grabs the current page range from the page range hashmap
        page_range_id = self.page_directory[record.rid].get("page_range")
        page = self.page_directory[record.rid].get("page")
        page_range = self.page_ranges[page_range_id]
        successful_write = page_range.write_record(record, page)
        return successful_write

    def read_record(self, rid, tid_list):
        if rid == -1:
            raise Exception("accessing unknown record")

        page_range_id = self.page_directory[rid].get("page_range")
        page_range = self.page_ranges[page_range_id]
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")
        records = page_range.get_record(row, page, tid_list)
        record_list = []
        for i in range(0, len(records)):
            if i == 0:
                record_list.append(record_from_list(records[i], True))
            else:
                record_list.append(record_from_list(records[i], False))
        return record_list

    def delete_record(self, rid):
        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")
        page_range = self.page_ranges[page_range_id]

        successful_delete = page_range.delete_record(row, page)

        return successful_delete

    def update_record(self, rid, original, new_cols):

        old_rid = rid

        if not original:
            rid = self.index.locate(self.key, new_cols[self.key])[0]

        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")

        page_range = self.page_ranges[page_range_id]
        successful_update = page_range.update_record(row, page, old_rid, new_cols)
        return successful_update

    # new tail ID, rid for tail pages
    def new_tid(self, rid):
        pass

    def new_rid(self):

        rid = self.num_records
        self.num_records += 1

        # determines what page range we are on (for example: if we are past the 16 page mark)
        page_range_id = math.floor(rid / (MAX_PAGES * RECORDS_PER_PAGE))

        # gets the row we are on
        row_in_range = rid % (MAX_PAGES * RECORDS_PER_PAGE)

        # gets the current page user is on
        page = math.floor(row_in_range / RECORDS_PER_PAGE)

        # gets the current row for the
        row = math.floor(row_in_range % RECORDS_PER_PAGE)

        if page_range_id >= len(self.page_ranges):
            self.add_new_page_range()

        self.page_directory[rid] = {
            'page_range': page_range_id,
            'row': row,
            'page': page,
            'tps': 0
        }

        return rid

    def get_rid_from_key(self, search_key, column):
        rid, tid_list = self.index.locate(column, search_key)

        return rid, tid_list

    # clean up function so we dont lose memory
    def delete_table(self):
        for page_range in self.page_ranges:
            page_range.clear_data()
            page_range.BasePages.clear()
            # page_range.TailPages.clear()

        self.page_ranges.clear()
        self.page_directory.clear()

    def add_new_page_range(self):
        self.page_ranges.append(PageRange(self.num_columns, INDIRECTION_COLUMN))
        pass

    def get_records(self, search_key, index):
        rid, tid_list = self.get_rid_from_key(search_key, index)

        record = self.read_record(rid, tid_list)
        return record

    def get_column_range(self, start, end, column):
        pass

    def write_to_files(self):
        # does not write any pages to files, that is handled by bufferpool.py
        if not os.path.exists(self.path):
            os.mkdir(self.path)

        table_data = {
            "metadata": {
                "name": self.name,
                "num_columns": self.num_columns,
                "num_records": self.num_records,
                "key": self.key,
                "path": self.path
            }}

        for i in range(len(self.page_ranges)):
            table_data[i] = self.page_ranges[i].to_json()

        table_data_file = open(f"{self.path}/table_data.json", "w")
        json.dump(table_data, table_data_file)
        table_data_file.close()

        page_info = open(f"{self.path}/page_info.dat", "wb")
        pickle.dump(self.page_directory, page_info)
        page_info.close()

        index_data = open(f"{self.path}/index_data.dat", "wb")
        pickle.dump(self.index, index_data)
        index_data.close()

        pass

    def __merge(self):
        pass
