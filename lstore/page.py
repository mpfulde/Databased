import math
import time

NO_METADATA = 4  # 4 constant columns for all tables (defined in lstore/table.py)
NO_BYTES = 8  # 64 bit integers so needing 8 bytes


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.data[0: 4096] = (0).to_bytes(NO_BYTES, byteorder='big')
        # if we run out of space we want to have another page that links back to the original (in the same column)
        self.parent = None
        self.child = None
        self.child_index = 0

    def has_child(self):
        return self.child is not None

    # if there are more records than available bytes return False
    def has_capacity(self):
        return self.num_records * NO_BYTES < 4096

    def write(self, value):
        if not self.has_capacity():
            return False

        # print(value)
        if type(value) == bytes:
            self.data[self.num_records * NO_BYTES: (self.num_records * NO_BYTES + 8)] = value
            # print(value)
        else:
            self.data[self.num_records * NO_BYTES: (self.num_records * NO_BYTES + 8)] = value.to_bytes(NO_BYTES,
                                                                                                       byteorder='big')
            # print(self.data[self.num_records * NO_BYTES: (self.num_records * NO_BYTES + 8)])
            # print(value)
        self.num_records += 1
        # print(self.num_records)
        return True

    def write_row(self, value, row):
        if not self.has_capacity():
            return False

        if row >= self.num_records:
            return self.write(value)
        # print(value.to_bytes(NO_BYTES, byteorder='big'))
        self.data[row * NO_BYTES: (row * NO_BYTES + 8)] = value.to_bytes(NO_BYTES, byteorder='big')
        # print(int.from_bytes(self.data[row * NO_BYTES: (row * NO_BYTES + 8)], byteorder='big'))
        return True

    """
    :param name: space         #the space in memory of the first bit of the data you are reading
    """

    def read(self, space):
        # grabs one 64 bit data piece from the data array
        req_data = self.data[space * NO_BYTES: (space * NO_BYTES + 8)]
        # print(req_data)
        # print(space)
        value = int.from_bytes(req_data, byteorder='big')
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
        for row in range(0, math.floor(4096)):
            val = self.read(row)
            # print(val)
            if val == value:
                row_list.append(row)

        return row_list


class BasePage(Page):
    def __init__(self):
        super().__init__()
        self.TailPages = []
        self.num_tails = round(
            4096 / NO_BYTES) * 2  # will always be 1 more than the allowed records in the page range as to not have conlficting ID's
        self.tail_directory = {}  # similar system to the page_directory in table.py

    def new_tid(self):
        tid = self.num_tails
        self.num_tails += 1

        # gets the current page user is on
        page = math.floor(tid / round(4096 / NO_BYTES))

        # gets the current row for the
        row = math.floor(tid % round(4096 / NO_BYTES))

        # adds a new tail page if not existant
        if page > len(self.TailPages):
            new_tail = Page()
            self.TailPages.append(new_tail)

        self.tail_directory[tid] = {
            'row': row,
            'page': page
        }

        return tid


class PageRange:

    def __init__(self, num_columns, indirection_col):

        # will regret later but for now just storing all base pages in a list its easier although slower

        self.indirection = indirection_col

        self.base_page_count = (NO_METADATA + num_columns)

        self.BasePages = [BasePage()] * self.base_page_count
        for i in range(0, self.base_page_count - 1):
            self.BasePages[i] = BasePage()

    # when inserting we are only dealing with base pages
    def write_record(self, record, page):
        record_list = record.create_list()
        successful_write = False

        latest_page = self.base_page_count * page

        # determine if we need ot make a new page
        if latest_page > len(self.BasePages) - 1:
            for i in range(self.base_page_count):
                # if self.BasePages[i + latest_page - self.base_page_count].has_capacity():
                #     raise Exception("Trying to make a new page when previous page has capacity")\

                new_page = BasePage()
                self.BasePages[i + latest_page - self.base_page_count].child = new_page
                new_page.parent = self.BasePages[i + latest_page - self.base_page_count]
                self.BasePages.append(new_page)

        # checks if randomly accessing a deleted row
        if self.BasePages[self.indirection] == (-1):
            raise Exception("Accessing a deleted row")

        # attempts to write to base pages
        for i in range(0, self.base_page_count):
            if i >= self.base_page_count:
                raise Exception("Outside of allowed column space")

            if i is 3:
                successful_write = self.BasePages[i + latest_page].write(0)
            else:

                successful_write = self.BasePages[i + latest_page].write(record_list[i])
                # if i is 4:
                #     print(record_list[i])
                #     print(self.BasePages[i + latest_page].read(self.BasePages[i+latest_page].num_records - 1))

        # if successful return True, if unsuccessful will throw exception
        return successful_write

    def get_record(self, row, page, update_list):

        record_list = []
        records = []
        # reads basepage at record page and row
        for i in range(0, self.base_page_count):
            record_list.append(self.BasePages[page * self.base_page_count + i].read(row))

        records.append(record_list)

        updates = []

        if len(update_list) > 0:
            for tid in update_list:
                tail_page = self.BasePages[page * self.base_page_count].tail_directory[tid].get("page")
                tail_row = self.BasePages[page * self.base_page_count].tail_directory[tid].get("row")

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

            successful_update = self.BasePages[page * self.base_page_count + self.indirection].TailPages[old_page].write_row(tid, old_row)

        successful_write = self.BasePages[page * self.base_page_count].TailPages[tail_page].write(tid)
        successful_write = successful_write and self.BasePages[page * self.base_page_count + 1].TailPages[tail_page].write(tid)
        successful_write = successful_write and self.BasePages[page * self.base_page_count + 2].TailPages[tail_page].write(round(time.time()))
        successful_write = successful_write and self.BasePages[page * self.base_page_count + 3].TailPages[tail_page].write(self.BasePages[page*self.base_page_count + 3])

        for i in range(len(updates)):
            successful_write = successful_write and self.BasePages[page * self.base_page_count + i + 4].TailPages[tail_page].write(updates[i])

        return successful_write and successful_update

    def clear_data(self):
        for page in self.BasePages:
            page.data.clear()
