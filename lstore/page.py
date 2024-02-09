from table import Record

NO_BASE_PAGES = 4  # 4 constant columns for all tables (definey in lstore/table.py)
NO_BYTES = 1


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
        pass

    # converts a record into a usable array to write to the data array


class PageRange:

    def __init__(self, num_columns):
        # will regret later but for now just storing all base pages in a list its easier although slower
        self.BasePages = [Page() for i in range(NO_BASE_PAGES)]

        self.tailID = NO_BASE_PAGES

        # assigns the parent to
        self.TailPages = [Page()] * num_columns

    def createNewBasePage(self, parent):
        basePage = Page()
        basePage.parent = parent
        return basePage

    def write_record(self, record):
        record_list = record.createList()
        successful_write = False
        for i in range(NO_BASE_PAGES):

            if i >= NO_BASE_PAGES:
                raise Exception("Outside of allowed column space")

            index = i

            # gets the latest page (where there is no child)
            while self.BasePages[index].has_child():
                index = self.BasePages[index].child_index

            if not self.BasePages[i].has_capacity() and not self.BasePages[i].has_child():
                self.BasePages[i].child_index = len(self.BasePages)
                new_base = self.createNewBasePage(self.BasePages[i])
                self.BasePages[i].child = new_base

            successful_write = self.BasePages[index].write(record_list[i])

            if not successful_write:
                raise Exception("Something went wrong and it didnt happen in the write function")

        # if successful return True, if unsuccessful will throw exception
        return successful_write
