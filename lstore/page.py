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
