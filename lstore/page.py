from table import Record
import math

NO_BASE_PAGES = 4  # 4 constant columns for all tables (defined in lstore/table.py)
NO_BYTES = 8  # 64 bit integers so needing 8 bytes


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
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

        self.data[self.num_records * NO_BYTES] = value.to_byte(NO_BYTES, 'big')
        self.num_records += 1
        return True

    def write_row(self, value, row):
        if not self.has_capacity():
            return False

        self.data[row * NO_BYTES: (row * NO_BYTES + 8)] = value.to_byte(NO_BYTES, 'big')
        return True
    """
    :param name: space         #the space in memory of the first bit of the data you are reading
    """

    def read(self, space):
        # grabs one full byte from the data
        req_data = self.data[space * NO_BYTES: (space * NO_BYTES + 8)]
        value = int.from_bytes(req_data, byteorder='big')
        return value



class PageRange:

    def __init__(self, num_columns, indirection_col):
        # will regret later but for now just storing all base pages in a list its easier although slower
        self.BasePages = [Page() for i in range(NO_BASE_PAGES)]

        self.indirection = indirection_col

        self.base_page_count = 0
        self.tail_page_count = 0

        # assigns the parent to
        self.TailPages = [Page()] * num_columns

    def write_record(self, record, page):
        record_list = record.create_list()
        successful_write = False

        # determine if we need ot make a new page
        if page > self.base_page_count:
            for i in range(len(self.BasePages)):
                if self.BasePages[i].has_capacity():
                    raise Exception("told to make a new page when previous page has capacity")
                else:
                    new_base = Page()
                    new_base.parent = self.BasePages[i]
                    self.BasePages[i].child = new_base
                    self.BasePages[i] = new_base
                    self.base_page_count += 1

        # checks if randomly accessing a deleted row
        if self.BasePages[self.indirection] is -1:
            raise Exception("Accessing a deleted row")

        # does the same check for tail pages
        if page > self.tail_page_count:
            for i in range(len(self.TailPages)):
                if self.TailPages[i].has_capacity():
                    raise Exception("told to make a new page when previous page has capacity")
                else:
                    new_tail = Page()
                    new_tail.parent = self.BasePages[i]
                    self.TailPages[i].child = new_tail
                    self.TailPages[i] = new_tail
                    self.tail_page_count += 1

        # attempts to write to base pages
        for i in range(NO_BASE_PAGES):
            if i >= NO_BASE_PAGES:
                raise Exception("Outside of allowed column space")

            successful_write = self.BasePages[i].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didnt happen in the write function")

        # attempts to write to tail pages
        for i in range(len(record.columns)):
            if i >= len(self.TailPages):
                raise Exception("Outside of allowed column space")

            successful_write = self.TailPages[i].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didnt happen in the write function")

        # if successful return True, if unsuccessful will throw exception
        return successful_write

    def get_record(self, row, page):
        pass

    def delete_record(self, row, page):

        curr_page_num = self.base_page_count
        curr_page = self.BasePages[self.indirection]
        while curr_page_num is not page:
            if self.BasePages[self.indirection].parent is not None and curr_page_num < 0:
                raise Exception("Went past the page limit")

            curr_page_num -= 1
            curr_page = self.BasePages[self.indirection].parent

        curr_page.write_row(-1, row)

        return True

    def update_record(self, row, page, record):
        curr_page_num = self.base_page_count
        curr_page = self.BasePages[self.indirection]
        while curr_page_num is not page:
            if self.BasePages[self.indirection].parent is not None and curr_page_num < 0:
                raise Exception("Went past the page limit")

            curr_page_num -= 1
            curr_page = self.BasePages[self.indirection].parent

        successful_update = curr_page.write_row(record.rid, row)

        successful_write = self.write_record(record, self.base_page_count)

        return successful_write and successful_update

    def clear_data(self):
        for page in self.BasePages:
            page.data.clear()

        for page in self.TailPages:
            page.data.clear()
