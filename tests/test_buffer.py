import pytest
from src.disk import PageID, PAGE_SIZE, DiskManager
from src.buffer import Buffer, Frame, BufferPool, BufferPoolManager


@pytest.fixture
def empty_buffer_pool_manager(tmp_path):
    file_path = tmp_path / "test.txt"
    disk = DiskManager(file_path)
    pool = BufferPool(1)
    bufmgr = BufferPoolManager(disk, pool)
    return bufmgr


class TestBuffer:

    def test_create_buffer(self):
        empty_buffer = Buffer()
        assert empty_buffer.page_id.to_int() == -1
        assert empty_buffer.page == bytearray(PAGE_SIZE)
        assert empty_buffer.is_dirty == False


class TestFrame:
    usage_count = 0
    buffer_ = Buffer()

    def test_create_frame(self):
        frame = Frame(self.usage_count, self.buffer_)
        assert frame.usage_count == self.usage_count
        assert frame.buffer == self.buffer_


class TestBufferPool:
    pool_size = 10

    def create_burffer_pool(self):
        buffer_pool = BufferPool(self.pool_size)
        assert len(buffer_pool.buffers) == self.pool_size
        assert buffer_pool.next_victim_id == 0


class TestBufferPoolManager:
    hello: bytearray = bytearray(PAGE_SIZE)
    world: bytearray = bytearray(PAGE_SIZE)
    hello[0:len("hello")] = map(ord, "hello")
    world[0:len("world")] = map(ord, "world")

    def test_io(self, empty_buffer_pool_manager):
        bufmgr = empty_buffer_pool_manager

        buffer_ = bufmgr.create_page()
        buffer_.page = self.hello
        buffer_.is_dirty = True
        hello_id = buffer_.page_id

        buffer_ = bufmgr.fetch_page(hello_id)
        page = buffer_.page
        assert(self.hello == page)

        buffer_ = bufmgr.create_page()
        buffer_.page = self.world
        buffer_.is_dirty = True
        world_id = buffer_.page_id

        buffer_ = bufmgr.fetch_page(world_id)
        page = buffer_.page
        assert(self.world == page)

        buffer_ = bufmgr.fetch_page(hello_id)
        page = buffer_.page
        assert(self.hello == page)

        buffer_ = bufmgr.fetch_page(world_id)
        page = buffer_.page
        assert(self.world == page)

    def test_flush_memory_data_to_disk(self, empty_buffer_pool_manager):
        bufmgr = empty_buffer_pool_manager

        buffer_ = bufmgr.create_page()
        buffer_.page = self.hello
        buffer_.is_dirty = True
        hello_id = buffer_.page_id

        bufmgr.flush()

        disk = bufmgr.disk
        new_pool = BufferPool(1)
        new_bufmgr = BufferPoolManager(disk, new_pool)

        buffer_ = new_bufmgr.fetch_page(hello_id)
        page = buffer_.page
        assert(self.hello == page)
