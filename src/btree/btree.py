from typing import Optional
from bisect import bisect_left
from src.disk import PageID, PAGE_SIZE
from src.buffer import Page, BufferPoolManager
from src.btree.leaf_page import LeafPage
from src.btree.inner_page import InnerPage


def is_leaf(page: Page) -> bool:
    return (page[0] & 1) == 1


class BTree:
    bufmgr: BufferPoolManager
    root_page_id: PageID
    key_size: int
    value_size: int

    def __init__(self, bufmgr: BufferPoolManager,
                 key_size: int, value_size: int,
                 root_page_id: Optional[PageID] = None) -> None:
        self.bufmgr = bufmgr
        if root_page_id is None:
            buffer_ = self.bufmgr.create_page()
            buffer_.page = LeafPage.empty_leaf(key_size, value_size).to_page()
            buffer_.is_dirty = True
            self.root_page_id = buffer_.page_id
        else:
            self.root_page_id = root_page_id
        self.key_size = key_size
        self.value_size = value_size

    def __contains__(self, key: bytearray) -> bool:
        return self._search(key)

    def _search(self, key: bytearray) -> bool:
        buffer_ = self.bufmgr.fetch_page(self.root_page_id)
        while not is_leaf(buffer_.page):
            page = InnerPage(buffer_.page, self.key_size)
            index = bisect_left(page.keys, key)
            buffer_ = self.bufmgr.fetch_page(page.children[index])
        leaf = LeafPage(buffer_.page, self.key_size, self.value_size)
        index = bisect_left(leaf.keys, key)
        return index != len(leaf.keys) and leaf.keys[index] == key

    def leaf_split(self, page: LeafPage,
                   page_id: PageID) -> Optional[LeafPage]:
        if len(page.keys) != page.max_key_count:
            return None

        new_buffer = self.bufmgr.create_page()
        new_page_id = new_buffer.page_id
        new = LeafPage.empty_leaf(self.key_size, self.value_size)

        new.keys = page.keys[:page.max_key_count // 2]
        new.values = page.values[:page.max_key_count // 2]
        page.keys = page.keys[page.max_key_count // 2:]
        page.values = page.values[page.max_key_count // 2:]

        if page.prev_page_id is not None:
            prev_buffer = self.bufmgr.fetch_page(page.prev_page_id)
            prev = LeafPage(prev_buffer.page, self.key_size, self.value_size)
            prev.next_page_id = new_page_id
            prev_buffer.page = prev.to_page()
            prev_buffer.is_dirty = True
            new.prev_page_id = page.prev_page_id
        new.next_page_id = page_id
        page.prev_page_id = new_buffer.page_id

        buffer_ = self.bufmgr.fetch_page(page_id)
        buffer_.page = page.to_page()
        buffer_.is_dirty = True

        buffer_ = self.bufmgr.fetch_page(new_page_id)
        buffer_.page = new.to_page()
        buffer_.is_dirty = True

        return new_page_id, new

    def inner_split(self, page: InnerPage,
                    page_id: PageID) -> Optional[InnerPage]:
        if len(page.keys) != page.max_key_count:
            return None

        new_buffer = self.bufmgr.create_page()
        new_page_id = new_buffer.page_id
        new = InnerPage.empty_inner(self.key_size)

        new.keys = page.keys[:page.max_key_count // 2]
        new.children = page.children[:page.max_key_count // 2]
        page.keys = page.keys[page.max_key_count // 2:]
        page.children = page.children[page.max_key_count // 2:]

        buffer_ = self.bufmgr.fetch_page(page_id)
        buffer_.page = page.to_page()
        buffer_.is_dirty = True

        buffer_ = self.bufmgr.fetch_page(new_page_id)
        buffer_.page = new.to_page()
        buffer_.is_dirty = True

        return new_page_id, new

    def _add_rec(self, page_id: PageID,
                 key: bytearray, value: bytearray) -> PageID:
        buffer_ = self.bufmgr.fetch_page(page_id)
        if is_leaf(buffer_.page):
            leaf = LeafPage(buffer_.page, self.key_size, self.value_size)
            index = bisect_left(leaf.keys, key)
            leaf.keys.insert(index, key)
            leaf.values.insert(index, value)
            buffer_ = self.bufmgr.fetch_page(page_id)
            buffer_.page = leaf.to_page()
            buffer_.is_dirty = True
            return self.leaf_split(leaf, page_id)
        else:
            inner = InnerPage(buffer_.page, self.key_size)
            index = bisect_left(inner.keys, key)
            new = self._add_rec(inner.children[index], key, value)
            if new is not None:
                new_page_id, new_page = new
                inner.keys.insert(index, new_page.keys[-1])
                inner.children.insert(index, new_page_id)
            buffer_ = self.bufmgr.fetch_page(page_id)
            buffer_.page = inner.to_page()
            buffer_.is_dirty = True
            return self.inner_split(inner, page_id)

    def add(self, key: bytearray, value: bytearray) -> bool:
        if key in self:
            return False

        new = self._add_rec(self.root_page_id, key, value)
        if new is not None:
            page_id_l, page_l = new
            new_root_buffer = self.bufmgr.create_page()
            new_root = InnerPage(new_root_buffer.page, self.key_size)
            new_root.keys.insert(0, page_l.keys_pop())
            new_root.children.insert(0, page_id_l)
            new_root.children.insert(1, self.root_page_id)
            self.root_page_id = new_root_buffer.page_id

            buffer_ = self.bufmgr.fetch_page(page_id_l)
            buffer_.page = page_l.to_page()
            buffer_.is_dirty = True

            buffer_ = self.bufmgr.fetch_page(self.root_page_id)
            buffer_.page = new_root.to_page()
            buffer_.is_dirty = True
        return True
