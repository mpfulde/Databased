import math


NO_METADATA = 4  # 4 constant columns for all tables (defined in lstore/table.py)
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

        print(value)
        if type(value) == bytes:
            self.data[self.num_records * NO_BYTES: (self.num_records * NO_BYTES + 8)] = value
        else:
            self.data[self.num_records * NO_BYTES: (self.num_records * NO_BYTES + 8)] = value.to_bytes(NO_BYTES, "big")
        self.num_records += 1
        print(self.num_records)
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
        # grabs one 64 bit data piece from the data array
        req_data = self.data[space * NO_BYTES: (space * NO_BYTES + 8)]
        value = int.from_bytes(req_data, byteorder='big')
        return value

    def contains(self, value):
        row = 0
        while row is not (4096/NO_BYTES):
            val = self.read(row)
            if val is value:
                return row

        return -1


class PageRange:

    def __init__(self, num_columns, indirection_col):
        # will regret later but for now just storing all base pages in a list its easier although slower
        self.BasePages = [Page()] * (NO_METADATA + num_columns)

        self.indirection = indirection_col

        self.base_page_count = (NO_METADATA + num_columns)
        self.tail_page_count = (NO_METADATA + num_columns)

        # assigns the parent to
        self.TailPages = [Page()] * (NO_METADATA + num_columns)

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

                new_page = Page()
                self.BasePages[i + latest_page - self.base_page_count].child = new_page
                new_page.parent = self.BasePages[i + latest_page - self.base_page_count]
                self.BasePages.append(new_page)

        # checks if randomly accessing a deleted row
        if self.BasePages[self.indirection] == (-1):
            raise Exception("Accessing a deleted row")

        # attempts to write to base pages
        for i in range(0, self.base_page_count - 1):
            if i >= self.base_page_count:
                raise Exception("Outside of allowed column space")

            successful_write = self.BasePages[i + latest_page].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didn't happen in the write function")

        # if successful return True, if unsuccessful will throw exception
        return successful_write

    def get_record(self, rid, indirected):

        record_list = []

        if indirected:
            page = 0
            curr_page = self.TailPages[3]
            while curr_page is not None:
                row = curr_page.contains(rid)
                if row is -1:
                    curr_page = curr_page.child
                    page += 1
                else:
                    break

            for i in range(page * self.tail_page_count, page * self.tail_page_count + (self.tail_page_count - 1)):
                record_list.append(self.TailPages[page * self.tail_page_count + i])

        else:
            page = 0
            curr_page = self.BasePages[3]
            while curr_page is not None:
                row = curr_page.contains(rid)
                if row is -1:
                    curr_page = curr_page.child
                    page += 1
                else:
                    break

            for i in range(page * self.base_page_count, page * self.base_page_count + (self.base_page_count - 1)):
                record_list.append(self.BasePages[page * self.base_page_count + i])

        return record_list

    def delete_record(self, row, page):

        curr_page = self.base_page_count * page

        if self.BasePages[curr_page + self.indirection] is None:
            raise Exception("Trying to delete a nonexistent record")

        if self.BasePages[self.indirection + curr_page].parent is not None and curr_page < 0:
            raise Exception("Went past the page limit")

        successful_write = self.BasePages[self.indirection + curr_page].write_row(-1, row)

        return successful_write

    def update_record(self, row, page, record):

        curr_page = self.base_page_count * page

        # writing to latest indirection point in tail pages if Base Page isnt latest
        bp_indirect = self.BasePages[curr_page + self.indirection].read(row)
        bp_rid = self.BasePages[curr_page + self.indirection + 1].read(row)
        if bp_indirect is not bp_rid:
            tail_id = self.TailPages[0]
            tail_rid = self.TailPages[1]
            tail_page = page
            row = -1
            while bp_indirect is not bp_rid:
                row = tail_rid.contains(bp_indirect)
                if row is -1:
                    if tail_rid.child is None:
                        raise Exception("Unable to access latest data")
                    tail_id = tail_id.child
                    tail_rid = tail_rid.child
                    page += 1
                else:
                    bp_indirect = tail_id.read(row)
                    bp_rid = tail_rid.read(row)
                    tail_id = tail_id.child
                    tail_rid = tail_rid.child
                    if (bp_indirect is not bp_rid):
                        page += 1

            if row is -1:
                raise Exception("Unable to access latest data")
            self.TailPages[(tail_page * self.tail_page_count) + self.indirection].write_row(record.rid, row)

        else:
            successful_update = self.BasePages[curr_page + self.indirection].write_row(record.rid, row)

        record_list = record.create_list()
        new_pages = []
        successful_write = False

        # determine if we need ot make a new page
        if not self.TailPages[-1].has_capacity():
            for i in range(self.tail_page_count):
                if self.BasePages[i - self.base_page_count].has_capacity():
                    raise Exception("Trying to make a new page when previous page has capacity") \

                new_page = Page()
                # gets each last page for each column
                self.TailPages[i - self.tail_page_count].child = new_page
                new_page.parent = self.TailPages[i - self.tail_page_count - 1]
                new_pages.append(new_page)

        # adds new tail pages to the tail page list (so we dont append while creating new pages)
        if len(new_pages) > 0:
            for page in new_pages:
                self.TailPages.append(page)

        # checks if randomly accessing a deleted row
        if self.TailPages[self.indirection] is -1:
            raise Exception("Accessing a deleted row")

        # attempts to write to base pages
        for i in range(self.tail_page_count):
            if i >= NO_METADATA:
                raise Exception("Outside of allowed column space")

            successful_write = self.TailPages[i - self.tail_page_count].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didnt happen in the write function")

        return successful_write and successful_update


    def clear_data(self):
        for page in self.BasePages:
            page.data.clear()

        for page in self.TailPages:
            page.data.clear()
