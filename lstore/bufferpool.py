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
        if os.path.exists(f"{page_path}/{page.name}.page"):
            page.read_from_path(f"{page_path}/{page.name}.page")

    for i in range(num_columns):
        page = Page(i)
        if os.path.exists(f"{page_path}/column_{page.name}.page"):
            page.read_from_path(f"{page_path}/column_{page.name}.page")

        pages.append(page)

    return pages


class Bufferpool:
    def __init__(self, path):
        self.path = path
        self.pool = {}
        self.pages_in_pool = 0  #
        pass

    # returns false if record is not in the pool
    def is_page_loaded(self, page, page_range, is_base):
        key = (page, page_range, is_base)
        if key in self.pool:
            return True

        return False

    def load_page_to_pool(self, path, page_range_id, num_columns, page, is_base):
        page_range_path = f"{path}/{page_range_id}"
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

        return index

    def commit_pool(self):
        for page in self.pool:
            if self.pool[page]["pages"].dirty:
                self.pool[page]["pages"].pool_to_file()
                self.pool[page]["pages"].dirty = False

    def evict(self):
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

        return True

    def has_capacity(self):
        return self.pages_in_pool < MAX_BUFFERPOOL_PAGES

    def close(self):
        self.commit_pool()
        while len(self.pool) > 0:
            self.evict()

        pass


# a loaded page into memory
class PagesInPool:
    def __init__(self):
        super().__init__()

        self.pages = []
        self.dirty = False
        self.pinned = False
        self.last_use = 0  # gives a timestamp of last used time
        self.pool_index = None
        self.path = None

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
