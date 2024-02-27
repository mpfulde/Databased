import ast
import math
import os.path
import pickle
import threading
from time import time

from lstore.config import *
from lstore.index import Index
from lstore.page import PageRange





def record_from_list(rlist, original):
    record = Record(rlist[RID_COLUMN], rlist[SCHEMA_ENCODING_COLUMN], rlist[NO_METADATA], rlist[NO_METADATA:(len(rlist))], original)
    record.base_rid = rlist[BASE_RID_COLUMN]
    record.original = record.base_rid == record.rid
    record.timestamp = rlist[TIMESTAMP_COLUMN]
    return record


def table_from_json(data):
    json_ = ast.literal_eval(data)
    metadata = json_["metadata"]
    table = Table(metadata['name'], metadata['num_columns'], metadata['key'], metadata['path'])
    table.num_records = metadata['num_records']

    if len(json_) > 1:
        table.page_ranges.clear()

    for key in json_:

        if key != "metadata":
            if not os.path.exists(f"{metadata['path']}/{key}"):
                os.mkdir(f"{metadata['path']}/{key}")
                os.mkdir(f"{metadata['path']}/{key}/BasePages")
                os.mkdir(f"{metadata['path']}/{key}/TailPages")

            range_ = PageRange(metadata['num_columns'], f"{metadata['path']}/{key}")
            range_.base_page_count = json_[key]["base_page_count"]
            range_.tail_page_count = json_[key]["tail_page_count"]
            range_.num_updates = json_[key]["num_updates"]
            range_.tail_directory = json_[key]["tail_directory"]
            table.page_ranges.append(range_)

            pass

    return table


class Record:

    def __init__(self, rid, schema_encoding, key, columns, original):
        self.rid = rid
        self.base_rid = rid
        self.timestamp = round(time())
        self.schema_encoding = schema_encoding
        self.key = key
        self.columns = columns
        self.original = original

    def create_list(self):
        list_ = [None] * (len(self.columns) + NO_METADATA)
        list_[INDIRECTION_COLUMN] = self.rid  # indirection should point to itself by default
        list_[RID_COLUMN] = self.rid
        list_[BASE_RID_COLUMN] = self.base_rid
        list_[TIMESTAMP_COLUMN] = self.timestamp
        list_[SCHEMA_ENCODING_COLUMN] = self.schema_encoding
        list_[(NO_METADATA):len(list_)] = self.columns
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
        self.page_ranges = [PageRange(num_columns, f"{path}/0")]
        self.page_directory = {}
        self.index = Index(self)
        self.bufferpool = None

        self.merge_lock = threading.Lock()
        self.merge_thread = threading.Thread(target=self.__merge)
        pass

    # does this one column at a time (will change later)
    def write_record(self, record):
        # grabs the current page range from the page range hashmap
        page_range_id = self.page_directory[record.rid].get("page_range")
        page = self.page_directory[record.rid].get("page")
        row = self.page_directory[record.rid].get("row")
        page_range = self.page_ranges[page_range_id]

        if not self.bufferpool.is_page_loaded(page_range_id, page, True):
            self.bufferpool.load_page_to_pool(self.path, page_range_id, self.num_columns, page, True)

        spot_in_pool = (page_range_id, page, True)

        record_list = record.create_list()

        # try:
        for i in range(len(record_list)):
            self.bufferpool.pool[spot_in_pool]["pages"].pages[i].write(record_list[i], row)
        # except Exception as e:
        #     print(e)
        #     return False

        self.index.indices[self.key].update_tree(record.columns[self.key], record_list[RID_COLUMN])

        self.bufferpool.pool[spot_in_pool]["pages"].dirty = True
        self.bufferpool.pool[spot_in_pool]["pages"].last_use = time()
        self.bufferpool.pool[spot_in_pool]["pages"].pin = False



        return True

    def read_record(self, rid_list):
        self.merge_lock.acquire()
        first_page_range_id = self.page_directory[rid_list[0]].get("page_range")
        first_page_range = self.page_ranges[first_page_range_id]
        record_list = []
        for rid in rid_list:

            if rid > self.num_records:  # ie if tail record
                page_range = first_page_range_id
                page = first_page_range.tail_directory[rid].get("page")
                row = first_page_range.tail_directory[rid].get("row")
                is_base = False
                pass
            else:
                page_range = self.page_directory[rid].get("page_range")
                page = self.page_directory[rid].get("page")
                row = self.page_directory[rid].get("row")
                is_base = True

            if not self.bufferpool.is_page_loaded(page_range, page, is_base):
                self.bufferpool.load_page_to_pool(self.path, page_range, self.num_columns, page, is_base)

            spot_in_pool = (page_range, page, is_base)
            record_as_list = []
            for page in self.bufferpool.pool[spot_in_pool]["pages"].pages:
                record_as_list.append(page.read(row))

            record = record_from_list(record_as_list, is_base)
            record_list.append(record)

        self.merge_lock.release()
        return record_list

    def delete_record(self, rid, key):
        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")
        page_range = self.page_ranges[page_range_id]

        if not self.bufferpool.is_page_loaded(page_range_id, page, True):
            self.bufferpool.load_page_to_pool(self.path, page_range_id, self.num_columns, page, True)

        spot_in_pool = (page_range_id, page, True)

        # try:
        self.bufferpool.pool[spot_in_pool]["pages"].pages[INDIRECTION_COLUMN].write(-1, row)
        self.bufferpool.pool[spot_in_pool]["pages"].pages[RID_COLUMN].write(-1, row)

        self.index.indices[self.key].remove_from_tree(key)

        self.bufferpool.pool[spot_in_pool]["pages"].dirty = True
        self.bufferpool.pool[spot_in_pool]["pages"].last_use = time()
        self.bufferpool.pool[spot_in_pool]["pages"].pin = False


        return True

    def update_record(self, rid, schema, original, new_cols):

        old_rid = rid

        if not original:
            rid = self.index.locate(self.key, new_cols[self.key])[0]

        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")

        page_range = self.page_ranges[page_range_id]

        tid = page_range.new_tid(page)
        self.page_directory[rid]["tps"] = tid
        record = Record(tid, schema, new_cols[0], new_cols, False)
        record.base_rid = rid
        page = page_range.tail_directory[tid].get("page")
        row = page_range.tail_directory[tid].get("row")

        if not self.bufferpool.is_page_loaded(page_range_id, page, False):
            self.bufferpool.load_page_to_pool(self.path, page_range_id, self.num_columns, page, False)

        spot_in_pool = (page_range_id, page, False)

        record_list = record.create_list()

        for i in range(len(record_list)):
            self.bufferpool.pool[spot_in_pool]["pages"].pages[i].write(record_list[i], row)

        self.index.indices[self.key].update_tree(record.columns[self.key], record_list[RID_COLUMN])

        self.bufferpool.pool[spot_in_pool]["pages"].dirty = True
        self.bufferpool.pool[spot_in_pool]["pages"].last_use = time()
        self.bufferpool.pool[spot_in_pool]["pages"].pin = False

        return True

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

        if page >= self.page_ranges[page_range_id].base_page_count - 1:
            self.page_ranges[page_range_id].base_page_count += 1

        self.page_directory[rid] = {
            'page_range': page_range_id,
            'row': row,
            'page': page,
            'tps': rid
        }

        return rid

    def get_rid_from_key(self, search_key, column):
        rid = self.index.locate(column, search_key)

        return rid

    # clean up function so we dont lose memory
    def delete_table(self):
        for page_range in self.page_ranges:
            page_range.clear_data()
            # page_range.TailPages.clear()

        self.page_ranges.clear()
        self.page_directory.clear()

    def add_new_page_range(self):
        self.page_ranges.append(PageRange(self.num_columns, f"{self.path}/{len(self.page_ranges)}"))
        pass

    def get_records(self, search_key, index):
        if self.index.indices[index] is None:
            self.index.create_index(index)

        rid_list = self.index.locate(index, search_key)

        record = self.read_record(rid_list)
        return record

    def write_to_files(self):
        # does not write any pages to files, that is handled by bufferpool.py

        self.merge_lock = None
        self.merge_thread = None

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
        str_to_add = str(table_data)
        table_data_file.write(str_to_add)
        table_data_file.close()

        page_info = open(f"{self.path}/page_info.dat", "wb")
        pickle.dump(self.page_directory, page_info)
        page_info.close()

        index_data = open(f"{self.path}/index_data.dat", "wb")
        pickle.dump(self.index, index_data)
        index_data.close()

        pass

    def get_record_range(self, start, end):
        rids = self.index.locate_range(start, end, self.key)
        records = []
        for rid_list in rids:
            records.append(self.read_record(rid_list))

        return records

    def ready_to_merge(self, rid):
        page_range_id = self.page_directory[rid].get("page_range_id")
        page_range = self.page_ranges[page_range_id]
        if page_range.num_updates % NUM_UPDATES_TO_MERGE == 0:
            # tells to start merging
            self.merge_thread.start()

    def __merge(self):
        self.merge_lock.acquire()
        self.merge_lock.release()
        pass
