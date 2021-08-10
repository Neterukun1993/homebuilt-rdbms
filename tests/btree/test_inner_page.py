import pytest
from src.btree.inner_page import InnerPage
from src.disk import PAGE_SIZE, PageID


@pytest.fixture
def empty_inner():
    key_size = 4
    page = bytearray(PAGE_SIZE)
    return InnerPage.empty_inner(key_size)


class TestInnerPageConstant:

    def test_constant(self):
        from src.btree.inner_page import (
            FLAG, LEAF_BIT,
            KEY_COUNT_BEGIN, KEY_COUNT_END,
            CELL_BEGIN,
            PAGE_ID_SIZE, RECORD_SIZE
        )

        assert FLAG                == 0
        assert LEAF_BIT            == 0b00000001

        assert KEY_COUNT_BEGIN     == 1
        assert KEY_COUNT_END       == 5

        assert CELL_BEGIN          == 5

        assert PAGE_ID_SIZE        == 4
        assert RECORD_SIZE         == PAGE_SIZE - CELL_BEGIN


class TestInnerPageConstructor:

    def test_empty_inner_properties(self, empty_inner):
        assert empty_inner.keys == []
        assert empty_inner.children == []

    def test_full_inner_properties(self, empty_inner):
        inner = empty_inner

        key_size = inner.key_size
        max_key_count = inner.max_key_count

        for i in range(max_key_count):
            inner.keys.append(bytearray(i.to_bytes(inner.key_size, 'big')))
        for i in range(max_key_count + 1):
            inner.children.append(PageID(i))

        page = inner.to_page()
        full_inner = InnerPage(page, key_size)
        assert full_inner.key_size == key_size
        assert full_inner.max_key_count == max_key_count

        for i in range(max_key_count):
            assert full_inner.keys[i] == inner.keys[i]
        for i in range(max_key_count + 1):
            assert full_inner.children[i].to_int() == (
                inner.children[i].to_int()
            )
