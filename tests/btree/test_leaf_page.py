import pytest
from src.btree.leaf_page import LeafPage
from src.disk import PAGE_SIZE, PageID


@pytest.fixture
def empty_leaf():
    key_size = 4
    value_size = 4
    return LeafPage.empty_leaf(key_size, value_size)


class TestLeafPageConstant:

    def test_constant(self):
        from src.btree.leaf_page import (
            FLAG, LEAF_BIT, PREV_PAGE_BIT, NEXT_PAGE_BIT,
            PREV_PAGE_ID_BEGIN, PREV_PAGE_ID_END,
            NEXT_PAGE_ID_BEGIN, NEXT_PAGE_ID_END,
            KEY_COUNT_BEGIN, KEY_COUNT_END,
            CELL_BEGIN,
            RECORD_SIZE
        )

        assert FLAG               == 0
        assert LEAF_BIT           == 0b00000001
        assert PREV_PAGE_BIT      == 0b00000010
        assert NEXT_PAGE_BIT      == 0b00000100

        assert PREV_PAGE_ID_BEGIN == 1
        assert PREV_PAGE_ID_END   == 5

        assert NEXT_PAGE_ID_BEGIN == 5
        assert NEXT_PAGE_ID_END   == 9

        assert KEY_COUNT_BEGIN    == 9
        assert KEY_COUNT_END      == 13

        assert CELL_BEGIN         == 13

        assert RECORD_SIZE        == PAGE_SIZE - CELL_BEGIN


class TestLeafPage:

    def test_empty_leaf_properties(self, empty_leaf):
        assert empty_leaf.prev_page_id is None
        assert empty_leaf.next_page_id is None

        assert empty_leaf.keys == []
        assert empty_leaf.values == []

    def test_full_leaf_properties(self, empty_leaf):
        leaf = empty_leaf

        key_size = leaf.key_size
        value_size = leaf.value_size
        max_key_count = leaf.max_key_count
        next_page_id = PageID(1)
        prev_page_id = PageID(2)

        for i in range(max_key_count):
            leaf.keys.append(bytearray((2 * i).to_bytes(leaf.key_size, 'big')))
            leaf.values.append(bytearray(i.to_bytes(leaf.value_size, 'big')))
        leaf.next_page_id = next_page_id
        leaf.prev_page_id = prev_page_id
        page = leaf.to_page()

        full_leaf = LeafPage(page, key_size, value_size)
        assert full_leaf.key_size == key_size
        assert full_leaf.value_size == value_size
        assert full_leaf.prev_page_id.to_int() == prev_page_id.to_int()
        assert full_leaf.next_page_id.to_int() == next_page_id.to_int()
        assert full_leaf.max_key_count == max_key_count

        for i in range(max_key_count):
            assert full_leaf.keys[i] == leaf.keys[i]
            assert full_leaf.values[i] == leaf.values[i]
