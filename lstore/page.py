from table import Record

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

        self.tailID = NO_BASE_PAGES

        # assigns the parent to
        self.TailPages = [Page()] * num_columns

    def createChildPage(self, parent):
        basePage = Page()
        basePage.parent = parent
        return basePage

    def write_record(self, record):
        record_list = record.createList()
        successful_write = False

        # attempts to write to base pages
        for i in range(NO_BASE_PAGES):

            if i >= NO_BASE_PAGES:
                raise Exception("Outside of allowed column space")

            index = i

            # gets the latest page (where there is no child)
            while self.BasePages[index].has_child():
                index = self.BasePages[index].child_index

            if not self.BasePages[index].has_capacity() and not self.BasePages[index].has_child():
                # smarter way to do this is to just set BasePages[i] to be the latest child and then recall through parents
                # will implement at a later date (@someone yell at me to do it before the due date)
                self.BasePages[index].child_index = len(self.BasePages)
                new_base = self.createChildPage(self.BasePages[index])
                self.BasePages[index].child = new_base

            successful_write = self.BasePages[index].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didnt happen in the write function")

        # attempts to write to tail pages
        for i in range(len(record.columns)):
            if i >= len(self.TailPages):
                raise Exception("Outside of allowed column space")

            index = i

            # gets the latest page (where there is no child)
            while self.TailPages[index].has_child():
                index = self.TailPages[index].child_index

            if not self.TailPages[index].has_capacity() and not self.TailPages[index].has_child():
                # smarter way to do this is the same as above
                self.TailPages[index].child_index = len(self.BasePages)
                new_tail = self.createChildPage(self.BasePages[index])
                self.TailPages[index].child = new_tail

            successful_write = self.TailPages[index].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didnt happen in the write function")

        # if successful return True, if unsuccessful will throw exception
        return successful_write

    def clear_data(self):
        for page in self.BasePages:
            page.data.clear()

        for page in self.TailPages:
            page.data.clear()
