import os
from lstore.page import Page
from lstore.config import *
import time

PAGES_IN_POOL = 0


# loads a batch of pages from a folder
def get_batch_from_folder(page_path, num_columns):
    # gets all metadata pages loaded
    pages = [Page("indirection"), Page("rid"), Page("base_rid"), Page("timestamp"), Page("schema_encoding")]

    for page in pages:
        page.read_from_path(f"{page_path}/{page.name}.page")

    for i in range(num_columns):
        page = Page(i)
        page.read_from_path(f"{page_path}/column_{page.name}.page")
        pages.append(page)

    return pages


class Bufferpool:
    def __init__(self, path):
        self.path = path
        self.pages = []
        self.pages_in_pool = 0  #
        self.pages_directory = {}  # holds different values from the on in table.py
        pass

    # returns false if record is not in the pool
    def is_page_loaded(self, rid):
        for key in self.pages_directory:
            if (self.pages_directory[key].get("lowest_rid") <= rid <= self.pages_directory[key].get("highest_rid") and
                    self.pages_directory[key].get("is_base_page")):
                return True

        return False

    def load_base_page(self, table_name, page_range_id, page_range, num_columns, page, is_base):
        page_range_path = f"{self.path}/{table_name}/{page_range_id}"
        if is_base:
            page_path = f"{page_range_path}/BasePages/{page}"
        else:
            page_path = f"{page_range_path}/TailPages/{page}"

        # capactiy check
        if not self.has_capacity():
            ## do work
            self.evict()
            pass

        new_pages = PagesInPool()
        new_pages.pinned = True
        # load basepages into pool
        base_pages = get_batch_from_folder(page_path, num_columns=num_columns)
        new_pages.pages = base_pages
        new_pages.last_use = time.time()

        self.pages.append(new_pages)

        index = self.pages_in_pool
        self.pages_directory[index] = {
            "index": index,
            "lowest_RID": page * (PAGE_SIZE / NO_BYTES),
            "highest_RID": page * (PAGE_SIZE / NO_BYTES) + 511,
            "is_base_page": is_base
        }
        self.pages_in_pool += 1

        return index

    def commit_page(self):
        pass

    def evict(self):
        pass

    def has_capacity(self):
        return self.pages_in_pool < MAX_BUFFERPOOL_PAGES


# a loaded page into memory
class PagesInPool:
    def __init__(self):
        super().__init__()

        self.pages = []
        self.dirty = False
        self.pinned = False
        self.last_use = 0  # gives a timestamp of last used time
        self.path = None

    # Name of metadata column or Column number
    def pool_to_file(self):

        successful_write = True
        for page in self.pages:
            if type(page.name) is int:
                # example "ECS165/test1/0/BasePages/0/column_0.page
                file_path = f"{self.path}/column_{page.name}.page"
            else:
                # example "ECS165/test1/0/BasePages/0/rid.page
                file_path = f"{self.path}/{page.name}.page"

            successful_write = successful_write and page.write_to_path(file_path)

        return successful_write
