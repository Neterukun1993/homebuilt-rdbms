import pytest
from src.disk import DiskManager
from src.buffer import BufferPool, BufferPoolManager
from src.btree.btree import BTree


@pytest.fixture
def empty_buffer_pool_manager(tmp_path):
    file_path = tmp_path / "test.txt"
    disk = DiskManager(file_path)
    pool = BufferPool(100)
    bufmgr = BufferPoolManager(disk, pool)
    return bufmgr


class TestBTree:

    def test_add_ascending(self, empty_buffer_pool_manager):
        key_size = 500
        value_size = 100
        record_count = 1000
        bt = BTree(empty_buffer_pool_manager, key_size, value_size)

        for i in range(record_count):
            key = bytearray(i.to_bytes(key_size, 'big'))
            value = bytearray(i.to_bytes(value_size, 'big'))
            bt.add(key, value)

        for i in range(record_count):
            assert (bytearray(i.to_bytes(key_size, 'big')) in bt)

    def test_add_descending(self, empty_buffer_pool_manager):
        key_size = 500
        value_size = 100
        record_count = 1000
        bt = BTree(empty_buffer_pool_manager, key_size, value_size)

        for i in reversed(range(record_count)):
            key = bytearray(i.to_bytes(key_size, 'big'))
            value = bytearray(i.to_bytes(value_size, 'big'))
            bt.add(key, value)

        for i in range(record_count):
            assert (bytearray(i.to_bytes(key_size, 'big')) in bt)
