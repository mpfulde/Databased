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

    """
    :param name: space         #the space in memory of the first bit of the data you are reading
    """

    def read(self, space):
        # grabs one full byte from the data
        req_data = self.data[space * NO_BYTES: (space * NO_BYTES + 8)]
        value = int.from_bytes(req_data, byteorder='big')
        return value


class PageRange:

    def __init__(self, num_columns):
        # will regret later but for now just storing all base pages in a list its easier although slower
        self.BasePages = [Page() for i in range(NO_BASE_PAGES)]

        self.base_page_count = 0
        self.tail_page_count = 0

        # assigns the parent to
        self.TailPages = [Page()] * num_columns


    def write_record(self, record, page):
        record_list = record.createList()
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

    def clear_data(self):
        for page in self.BasePages:
            page.data.clear()

        for page in self.TailPages:
            page.data.clear()
