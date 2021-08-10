from __future__ import annotations
from typing import List
from src.disk import PAGE_SIZE, PageID
from src.buffer import Page

"""
HEADER
0            1                    5                    9                 13
+------------+--------------------+--------------------+-----------------+
| [int] flag | [int] prev_page_id | [int] next_page_id | [int] key_count |
+------------+--------------------+--------------------+-----------------+

CELL
0          key_size        key_size + value_size
+-------------+---------------+
| [bytes] key | [bytes] value |
+-------------+---------------+

PAGE = HEADER + CELL + CELL + CELL + ...
"""


FLAG: int               = 0
LEAF_BIT: int           = 0b00000001
PREV_PAGE_BIT: int      = 0b00000010
NEXT_PAGE_BIT: int      = 0b00000100

PREV_PAGE_ID_BEGIN: int = 1
PREV_PAGE_ID_END: int   = 5

NEXT_PAGE_ID_BEGIN: int = 5
NEXT_PAGE_ID_END: int   = 9

KEY_COUNT_BEGIN: int    = 9
KEY_COUNT_END: int      = 13

CELL_BEGIN: int         = 13

RECORD_SIZE: int        = PAGE_SIZE - CELL_BEGIN


class LeafPage:
    key_size: int
    value_size: int
    max_key_count: int

    prev_page_id: PageID
    next_page_id: PageID
    keys: List[bytearray]
    values: List[bytearray]

    def __init__(self, page: Page, key_size: int, value_size: int) -> None:
        self.key_size = key_size
        self.value_size = value_size
        self.max_key_count = RECORD_SIZE // (key_size + value_size)

        self.prev_page_id = self._parse_prev_page_id(page)
        self.next_page_id = self._parse_next_page_id(page)
        self.keys = self._parse_keys(page)
        self.values = self._parse_values(page)

    @staticmethod
    def empty_leaf(key_size: int, value_size: int) -> LeafPage:
        page = bytearray(PAGE_SIZE)
        page[FLAG] |= LEAF_BIT
        return LeafPage(page, key_size, value_size)

    def _parse_prev_page_id(self, page: Page) -> PageID:
        if not (page[FLAG] & PREV_PAGE_BIT):
            return None
        id_ = int.from_bytes(page[PREV_PAGE_ID_BEGIN:PREV_PAGE_ID_END], 'big')
        return PageID(id_)

    def _parse_next_page_id(self, page: Page) -> PageID:
        if not (page[FLAG] & NEXT_PAGE_BIT):
            return None
        id_ = int.from_bytes(page[NEXT_PAGE_ID_BEGIN:NEXT_PAGE_ID_END], 'big')
        return PageID(id_)

    def _parse_key_count(self, page: Page) -> int:
        count = int.from_bytes(page[KEY_COUNT_BEGIN:KEY_COUNT_END], 'big')
        return count

    def _parse_keys(self, page: Page) -> List[bytearray]:
        keys = []
        begin = CELL_BEGIN
        for i in range(self._parse_key_count(page)):
            end = begin + self.key_size
            keys.append(page[begin:end])
            begin += self.key_size + self.value_size
        return keys

    def _parse_values(self, page: Page) -> List[bytearray]:
        values = []
        begin = CELL_BEGIN + self.key_size
        for i in range(self._parse_key_count(page)):
            end = begin + self.value_size
            values.append(page[begin:end])
            begin += self.key_size + self.value_size
        return values

    def keys_pop(self) -> bytearray:
        return self.keys[-1]

    def to_page(self) -> Page:
        page = bytearray(PAGE_SIZE)
        page[FLAG] |= LEAF_BIT

        id_size = PREV_PAGE_ID_END - PREV_PAGE_ID_BEGIN
        if self.prev_page_id is not None:
            page[FLAG] |= PREV_PAGE_BIT
            page[PREV_PAGE_ID_BEGIN:PREV_PAGE_ID_END] = (
                self.prev_page_id.to_int().to_bytes(id_size, 'big')
            )
        if self.next_page_id is not None:
            page[FLAG] |= NEXT_PAGE_BIT
            page[NEXT_PAGE_ID_BEGIN:NEXT_PAGE_ID_END] = (
                self.next_page_id.to_int().to_bytes(id_size, 'big')
            )
        size = KEY_COUNT_END - KEY_COUNT_BEGIN
        page[KEY_COUNT_BEGIN:KEY_COUNT_END] = (
            len(self.keys).to_bytes(size, 'big')
        )
        begin = CELL_BEGIN
        for key, value in zip(self.keys, self.values):
            end = begin + self.key_size + self.value_size
            page[begin:end] = key + value
            begin += self.key_size + self.value_size
        return page
