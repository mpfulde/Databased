'''
Constants page and other configuration stuff
'''

# Bufferpool
MAX_BUFFERPOOL_PAGES = 64

# Table
INDIRECTION_COLUMN = 0  # VALUE SHOULD BE AN RID
RID_COLUMN = 1
BASE_RID_COLUMN = 2
TIMESTAMP_COLUMN = 3
SCHEMA_ENCODING_COLUMN = 4
RECORDS_PER_PAGE = 4096 / 8  # 4kb / 8 byte ints
MAX_PAGES = 20

NUM_UPDATES_TO_MERGE = 20

# Page
PAGE_SIZE = 4096 # 4 kb
NO_METADATA = 5  # 5 constant columns for all tables (defined in lstore/table.py)
NO_BYTES = 8  # 64 bit integers so needing 8 bytes

# Index
TREE_SIZE = 5