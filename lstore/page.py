import math
import os
import time
from lstore.config import *


class Page:

    def __init__(self, name):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        # if we run out of space we want to have another page that links back to the original (in the same column)
        self.name = name

    # if there are more records than available bytes return False
    def has_capacity(self):
        return self.num_records * NO_BYTES < 4096

    def write(self, value, row):
        if not self.has_capacity():
            return False

        # print(value)
        if type(value) == bytes:
            self.data[row * NO_BYTES: (row * NO_BYTES + 8)] = value
            # print(value)
        else:
            self.data[row * NO_BYTES: (row * NO_BYTES + 8)] = value.to_bytes(NO_BYTES, byteorder='big', signed=True)
            # print(self.data[self.num_records * NO_BYTES: (self.num_records * NO_BYTES + 8)])
            # print(value)
        self.num_records += 1
        # print(self.num_records)
        return True

    """
    :param name: space         #the space in memory of the first bit of the data you are reading
    """

    def read(self, space):
        # grabs one 64 bit data piece from the data array
        req_data = self.data[space * NO_BYTES: (space * NO_BYTES + NO_BYTES)]
        # print(req_data)
        # print(space)
        value = int.from_bytes(req_data, byteorder='big', signed=True)
        return value

    def contains(self, value):
        for row in range(0, math.floor(4096 / NO_BYTES)):
            val = self.read(row)
            # print(val)
            if val == value:
                return row

        return -1

    def find_all(self, value):
        row_list = []
        for row in range(0, math.floor(PAGE_SIZE / NO_BYTES)):
            val = self.read(row)
            # print(val)
            if val == value:
                row_list.append(row)

        return row_list

    # writes itself to a specific file path
    def write_to_path(self, path):
        if not os.path.exists(path):
            file = open(path, 'x')
            file.close()

        file = open(path, "wb")
        for i in range(self.num_records):
            file.write(self.data)
        file.close()

        return True

    def read_from_path(self, path):
        file = open(path, "rb")
        self.data = bytearray(file.read(PAGE_SIZE))

        file.close()

class PageRange:

    def __init__(self, num_columns, path):

        # will regret later but for now just storing all base pages in a list its easier although slower
        if not os.path.exists(path):
            os.makedirs(f"{path}")
            os.mkdir(f"{path}/BasePages")
            os.mkdir(f"{path}/TailPages")

        self.base_page_count = 1
        self.tail_page_count = 0
        self.num_columns = num_columns
        self.num_updates = 0
        self.tail_directory = {}

    def to_json(self):
        page_range_data = {
            "base_page_count": self.base_page_count,
            "tail_page_count": self.tail_page_count,
            "num_updates": self.num_updates,
            "tail_directory": self.tail_directory
        }

        return page_range_data

    # new tail ID, rid for tail pages
    def new_tid(self, base_page):
        tid = pow(2, 32) - self.num_updates

        # gets the current page user is on
        page = math.floor(self.num_updates / round(PAGE_SIZE / NO_BYTES))

        # gets the current row for the
        row = math.floor(self.num_updates % round(PAGE_SIZE / NO_BYTES))

        if page >= self.tail_page_count - 1:
            self.tail_page_count += 1

        self.tail_directory[tid] = {
            'row': row,
            'page': page,
            'base_page': base_page
        }

        self.num_updates += 1
        return tid

    def get_record(self, row, page, update_list):

        record_list = []
        records = []
        # reads basepage at record page and row
        for i in range(0, self.base_page_count):
            record_list.append(self.BasePages[page * self.base_page_count + i].read(row))

        records.append(record_list)

        updates = []

        # print(len(update_list))
        if len(update_list) > 0:
            for tid in update_list:
                tail_page = self.BasePages[page * self.base_page_count + 1].tail_directory[tid].get("page")
                tail_row = self.BasePages[page * self.base_page_count + 1].tail_directory[tid].get("row")

                for i in range(0, self.base_page_count):
                    updates.append(self.BasePages[page * self.base_page_count + i].TailPages[tail_page].read(tail_row))

                records.append(updates)
                updates = []

        # record_list.append([])

        return records

    def delete_record(self, row, page):

        curr_page = self.base_page_count * page

        if self.BasePages[curr_page + self.indirection] is None:
            raise Exception("Trying to delete a nonexistent record")

        if self.BasePages[self.indirection + curr_page].parent is not None and curr_page < 0:
            raise Exception("Went past the page limit")

        successful_write = self.BasePages[self.indirection + curr_page].write_row(-1, row)
        successful_write = self.BasePages[1 + curr_page].write_row(-1, row)

        return successful_write

    def update_record(self, row, page, old_rid, updates):

        successful_update = False

        # creates tail pages if none exist
        if len(self.BasePages[page * self.base_page_count].TailPages) == 0:
            for i in range(0, self.base_page_count):
                self.BasePages[page * self.base_page_count + i].TailPages.append(Page())

        tid = self.BasePages[page * self.base_page_count + 1].new_tid()
        tail_page = self.BasePages[page * self.base_page_count + 1].tail_directory[tid].get("page")

        if self.BasePages[page * self.base_page_count + 1].read(row) == old_rid:
            successful_update = self.BasePages[page * self.base_page_count + self.indirection].write_row(tid, row)
        else:
            old_row = self.BasePages[page * self.base_page_count + 1].tail_directory[old_rid].get("row")
            old_page = self.BasePages[page * self.base_page_count + 1].tail_directory[old_rid].get("page")

            successful_update = self.BasePages[page * self.base_page_count + self.indirection].TailPages[
                old_page].write_row(tid, old_row)

        if tail_page > len(self.BasePages[page * self.base_page_count].TailPages) - 1:
            for i in range(0, self.base_page_count):
                new_tail = Page()
                self.BasePages[page * self.base_page_count + i].TailPages[tail_page - 1].child = new_tail
                new_tail.parent = self.BasePages[page * self.base_page_count + i].TailPages[tail_page - 1]
                self.BasePages[page * self.base_page_count + i].TailPages.append(new_tail)

        successful_write = self.BasePages[page * self.base_page_count].TailPages[tail_page].write(tid)
        successful_write = successful_write and self.BasePages[page * self.base_page_count + 1].TailPages[
            tail_page].write(tid)
        successful_write = successful_write and self.BasePages[page * self.base_page_count + 2].TailPages[
            tail_page].write(round(time.time()))
        successful_write = successful_write and self.BasePages[page * self.base_page_count + 3].TailPages[
            tail_page].write(self.BasePages[page * self.base_page_count + 3].read(row))

        for i in range(len(updates)):
            successful_write = successful_write and self.BasePages[page * self.base_page_count + i + 4].TailPages[
                tail_page].write(updates[i])

        return successful_write and successful_update

    def clear_data(self):
        for page in self.BasePages:
            page.data.clear()
