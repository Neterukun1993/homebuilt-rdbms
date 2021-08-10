from __future__ import annotations
from typing import List
from src.disk import PAGE_SIZE, PageID
from src.buffer import Page

"""
HEADER
0            1                 5
+------------+-----------------+
| [int] flag | [int] key_count |
+------------+-----------------+

CELL
0                    4          4 + key_size
+--------------------+-------------+
|[int] child_page_id | [bytes] key |
+--------------------+-------------+

PAGE = HEADER + CELL + CELL + CELL + ...
"""

FLAG: int                = 0
LEAF_BIT: int            = 0b00000001

KEY_COUNT_BEGIN: int     = 1
KEY_COUNT_END: int       = 5

CELL_BEGIN: int          = 5

PAGE_ID_SIZE: int        = 4
RECORD_SIZE: int         = PAGE_SIZE - CELL_BEGIN


class InnerPage:
    key_size: int
    max_key_count: int

    keys: List[bytearray]
    children: List[PageID]

    def __init__(self, page: Page, key_size: int) -> None:
        self.key_size = key_size
        self.max_key_count = (
            (RECORD_SIZE - PAGE_ID_SIZE) // (PAGE_ID_SIZE + key_size)
        )
        self.keys = self._parse_keys(page)
        self.children = self._parse_children(page)

    @staticmethod
    def empty_inner(key_size: int) -> InnerPage:
        page = bytearray(PAGE_SIZE)
        inner = InnerPage(page, key_size)
        inner.children = []
        return inner

    def _parse_key_count(self, page: Page) -> int:
        count = int.from_bytes(page[KEY_COUNT_BEGIN:KEY_COUNT_END], 'big')
        return count

    def _parse_keys(self, page: Page) -> List[bytearray]:
        keys = []
        begin = CELL_BEGIN + PAGE_ID_SIZE
        for _ in range(self._parse_key_count(page)):
            end = begin + self.key_size
            keys.append(page[begin:end])
            begin += PAGE_ID_SIZE + self.key_size
        return keys

    def _parse_children(self, page: Page) -> List[PageID]:
        children = []
        begin = CELL_BEGIN
        for _ in range(self._parse_key_count(page) + 1):
            end = begin + PAGE_ID_SIZE
            children.append(PageID(int.from_bytes(page[begin:end], 'big')))
            begin += PAGE_ID_SIZE + self.key_size
        return children

    def keys_pop(self) -> bytearray:
        return self.keys.pop()

    def to_page(self) -> Page:
        page = bytearray(PAGE_SIZE)

        size = KEY_COUNT_END - KEY_COUNT_BEGIN
        page[KEY_COUNT_BEGIN:KEY_COUNT_END] = (
            len(self.keys).to_bytes(size, 'big')
        )
        begin = CELL_BEGIN + PAGE_ID_SIZE
        for key in self.keys:
            end = begin + self.key_size
            page[begin:end] = key
            begin += PAGE_ID_SIZE + self.key_size

        begin = CELL_BEGIN
        for child in self.children:
            end = begin + PAGE_ID_SIZE
            page[begin:end] = child.to_int().to_bytes(PAGE_ID_SIZE, 'big')
            begin += PAGE_ID_SIZE + self.key_size
        return page
