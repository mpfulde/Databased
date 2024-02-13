from lstore.index import Index
from lstore.page import Page, PageRange
import math
from time import time

INDIRECTION_COLUMN = 0 # VALUE SHOULD BE AN RID
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

RECORDS_PER_PAGE = 4096 / 8  # 4kb / 8 byte ints
MAX_PAGES = 16  # max number of pages in page range (based off example exclusively)


def record_from_list(rlist):
    record = Record(rlist[RID_COLUMN], rlist[SCHEMA_ENCODING_COLUMN], rlist[4], rlist[4:(len(rlist))])
    record.timestamp = rlist[TIMESTAMP_COLUMN]
    return record


class Record:

    def __init__(self, rid, schema_encoding, key, columns):
        self.rid = rid
        self.timestamp = round(time())
        self.schema_encoding = schema_encoding
        self.key = key
        self.columns = columns

    def create_list(self):
        list = [None] * (len(self.columns) + 4)
        list[INDIRECTION_COLUMN] = self.rid # indirection should point to itself by default
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
        self.key = key + 4 # + 4 is for the 4 metadata columns
        self.num_columns = num_columns
        self.num_records = 0
        self.page_ranges = [PageRange(num_columns, INDIRECTION_COLUMN)]
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

    def read_record(self, rid, indirected):
        page_range_id = self.page_directory[rid].get("page_range")
        page_range = self.page_ranges[page_range_id]
        record = page_range.get_record(rid, indirected)
        return record_from_list(record)

    def delete_record(self, rid):
        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")
        page_range = self.page_ranges[page_range_id]

        successful_delete = page_range.delete_record(row, page)

        return successful_delete

    def update_record(self, rid, record):
        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")

        page_range = self.page_ranges[page_range_id]
        successful_update = page_range.update_record(row, page, record)
        return successful_update

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
        row = row_in_range % RECORDS_PER_PAGE

        if page_range_id >= len(self.page_ranges):
            self.add_new_page_range()

        self.page_directory[rid] = {
            'page_range': page_range_id,
            'row': row,
            'page': page
        }

        return rid

    def get_rid_from_key(self, search_key, column):
        rid = self.index.locate(column, search_key)

        return rid


    def get_rid_from_key_version(self, search_key, column, version):
        rid_list = self.index.locate(column, search_key)

        # indirect_rid, indirected = self.get_indirected_rid(rid, version)  # gets the true rid from the indirection column

        indirect_list = []
        for i in range(len(rid_list)):
            if i == 0:
                indirect_list.append(False)
            else:
                indirect_list.append(True)

        indirect_rid, indirected = rid_list, indirect_list

        return indirect_rid, indirected

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
        self.page_ranges.append(PageRange(self.num_columns, INDIRECTION_COLUMN))
        pass

    def get_indirected_rid(self, rid, version):
        page_range_id = self.page_directory[rid].get("page_range")
        page = self.page_directory[rid].get("page")
        row = self.page_directory[rid].get("row")
        id_rid = rid

        if version is 0:
            return id_rid, False

        page_range = self.page_directory[page_range_id]

        id_point = page_range.BasePages[page * page_range.base_page_count + INDIRECTION_COLUMN].read(row)
        if id_point is not rid:
            for page_range in self.page_ranges:
                curr_page = page_range.TailPages[RID_COLUMN]
                indirection_page = page_range.TailPages[INDIRECTION_COLUMN]
                while curr_page is not None:
                    row = curr_page.contains(id_point)
                    if row is -1:
                        if curr_page.child is None:
                            raise Exception("No child found")
                        curr_page = curr_page.child
                        indirection_page = indirection_page.child
                        continue

                    if indirection_page.read(row) is not id_point:
                        id_point = indirection_page.read(row)
                        id_rid = id_point
                        version += 1
                        if version is 0:
                            return id_rid, True
                        curr_page = curr_page.child
                        indirection_page = indirection_page.child
                        continue
                    else:
                        id_rid = curr_page.read(row)
                        return id_rid, True
        else:
            return id_rid, False

    def get_records(self, search_key, index, version):
        rid, indirected = self.get_rid_from_key_version(search_key, index, version)

        record = []
        for i in range(len(rid)):
            record.append(self.read_record(rid[i], indirected[i]))
        return record
    def get_column_range(self, start, end, column, version):
        rid_list = self.index.locate_range(start, end, column)

        indirected_list = [False] * len(rid_list)

        values_in_column = []

        for i in range(len(rid_list)):
            rid_list[i], indirected_list[i] = self.get_indirected_rid(rid_list[i], version)
            values_in_column.append(self.read_record(rid_list[i], indirected_list[i]).columns[column])

        return values_in_column