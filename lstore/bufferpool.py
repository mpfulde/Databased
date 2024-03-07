import os
from lstore.page import Page
from lstore.config import *
import time
import threading

PAGES_IN_POOL = 0


# loads a batch of pages from a folder
def get_batch_from_folder(page_path, num_columns):
    # gets all metadata pages loaded
    pages = [Page("indirection"), Page("rid"), Page("base_rid"), Page("timestamp"), Page("schema_encoding")]

    new = True

    for page in pages:
        if os.path.exists(f"{page_path}/{page.name}.new.page") and new:
            page.read_from_path(f"{page_path}/{page.name}.new.page")
            new = new & True
        elif os.path.exists(f"{page_path}/{page.name}.page"):
            page.read_from_path(f"{page_path}/{page.name}.page")
            new = False

    for i in range(num_columns):
        page = Page(i)
        if os.path.exists(f"{page_path}/column_{page.name}.new.page") and new:
            page.read_from_path(f"{page_path}/column_{page.name}.new.page")
            new = new & True
        elif os.path.exists(f"{page_path}/column_{page.name}.page"):
            page.read_from_path(f"{page_path}/column_{page.name}.page")
            new = False

        pages.append(page)

    return pages, new


class Bufferpool:
    def __init__(self, path):
        self.path = path
        self.pool = {}
        self.pages_in_pool = 0  #
        self.ignore_limit = False  # will only be set if its a bufferpool for merge
        self.pool_lock = threading.Lock()
        pass

    # returns false if record is not in the pool
    def is_page_loaded(self, page_range, page, is_base):
        key = (page_range, page, is_base)
        if key in self.pool:
            return True

        return False

    def load_page_to_pool(self, path, page_range_id, num_columns, page, is_base):
        self.pool_lock.acquire()
        page_range_path = f"{path}/{page_range_id}"
        if is_base:
            page_path = f"{page_range_path}/BasePages/{page}"
        else:
            page_path = f"{page_range_path}/TailPages/{page}"

        # capactiy check
        if not self.has_capacity() and not self.ignore_limit:
            # do work
            self.evict()
            pass

        new_pages = PagesInPool()
        # load basepages into pool
        base_pages, new = get_batch_from_folder(page_path, num_columns=num_columns)
        new_pages.pages = base_pages
        new_pages.new = new
        new_pages.path = page_path
        new_pages.last_use = time.time()
        index = (page_range_id, page, is_base)
        new_pages.pool_index = index

        self.pool[index] = {
            "index": index,
            "is_base_page": is_base,
            "pages": new_pages
        }

        self.pages_in_pool += 1
        self.pool_lock.release()
        return index

    def commit_pool(self):
        for page in self.pool:
            if self.pool[page]["pages"].dirty:
                self.pool[page]["pages"].pool_to_file()
                self.pool[page]["pages"].dirty = False

    def evict(self):
        self.pool_lock.acquire()
        # uses the lru eviction strategy
        oldest_use = self.pool[list(self.pool.keys())[0]]["pages"]
        oldest_index = list(self.pool.keys())[0]

        for index in self.pool:
            if self.pool[index]["pages"].last_use < oldest_use.last_use:
                oldest_index = index
                oldest_use = self.pool[index]["pages"]

        if oldest_use.dirty:
            oldest_use.pool_to_file()

        del self.pool[oldest_index]
        self.pages_in_pool -= 1
        self.pool_lock.release()
        return True

    def evict_index(self, index):
        pages = self.pool[index]["pages"]
        if pages.dirty:
            pages.pool_to_file()

        del self.pool[index]
        self.pages_in_pool -= 1
        return True

    def has_capacity(self):
        return self.pages_in_pool < MAX_BUFFERPOOL_PAGES

    def close(self):
        while len(self.pool) > 0:
            self.evict()

        self.pool_lock = None
        pass


# a loaded page into memory
class PagesInPool:
    def __init__(self):
        super().__init__()

        self.pages = []
        self.dirty = False
        self.pin = 0
        self.last_use = 0  # gives a timestamp of last used time
        self.pool_index = None
        self.path = None
        self.new = False

    # Name of metadata column or Column number
    def pool_to_file(self):

        if not os.path.exists(self.path):
            os.mkdir(self.path)

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
