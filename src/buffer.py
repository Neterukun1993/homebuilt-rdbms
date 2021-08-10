from typing import List, Dict
from src.disk import PageID, DiskManager, PAGE_SIZE


BufferID = int
Page = bytearray


class Buffer:
    page_id: PageID
    page: Page
    is_dirty: bool

    def __init__(self) -> None:
        self.page_id = PageID(-1)
        self.page = bytearray(PAGE_SIZE)
        self.is_dirty = False


class Frame:
    usage_count: int
    buffer_: Buffer

    def __init__(self, usage_count: int, buffer_: Buffer) -> None:
        self.usage_count = usage_count
        self.buffer = buffer_


class BufferPool:
    buffers: List[Frame]
    next_victim_id: BufferID

    def __init__(self, pool_size: int) -> None:
        self.buffers = [Frame(0, Buffer()) for _ in range(pool_size)]
        self.next_victim_id = 0

    def __len__(self) -> int:
        return len(self.buffers)

    def __getitem__(self, buffer_id: BufferID) -> Frame:
        return self.buffers[buffer_id]

    def __setitem__(self, buffer_id: BufferID, buffer_: Frame) -> None:
        self.buffers[buffer_id] = buffer_

    def evict(self) -> BufferID:
        pool_size = len(self)
        consective_pinned = 0

        while True:
            next_victim_id = self.next_victim_id
            frame = self[next_victim_id]
            if frame.usage_count == 0:
                return self.next_victim_id
            frame.usage_count -= 1
            consective_pinned = 0
            self.next_victim_id = self.increment_id(self.next_victim_id)

    def increment_id(self, buffer_id: BufferID) -> BufferID:
        return (buffer_id + 1) % len(self)


class BufferPoolManager:
    disk: DiskManager
    pool: BufferPool
    page_table: Dict[PageID, BufferID]

    def __init__(self, disk: DiskManager, pool: BufferPool) -> None:
        self.disk = disk
        self.pool = pool
        self.page_table = {}

    def fetch_page(self, page_id: PageID) -> Buffer:
        if page_id in self.page_table:
            buffer_id = self.page_table[page_id]
            frame = self.pool[buffer_id]
            frame.usage_count += 1
            return frame.buffer

        buffer_id = self.pool.evict()
        frame = self.pool[buffer_id]
        evict_page_id = frame.buffer.page_id

        buffer_ = frame.buffer
        if buffer_.is_dirty:
            self.disk.write_page_data(evict_page_id, buffer_.page)

        buffer_.page_id = page_id
        buffer_.is_dirty = False
        buffer_.page = self.disk.read_page_data(page_id)
        frame.usage_count = 1

        page = frame.buffer
        self.page_table.pop(evict_page_id, None)
        self.page_table[page_id] = buffer_id
        return page

    def create_page(self) -> Buffer:
        buffer_id = self.pool.evict()
        frame = self.pool[buffer_id]
        evict_page_id = frame.buffer.page_id

        buffer_ = frame.buffer
        if buffer_.is_dirty:
            self.disk.write_page_data(evict_page_id, buffer_.page)
        page_id = self.disk.allocate_page()
        frame.buffer = Buffer()
        frame.buffer.page_id = page_id
        frame.buffer.is_dirty = True
        frame.usage_count = 1

        page = frame.buffer
        self.page_table.pop(evict_page_id, None)
        self.page_table[page_id] = buffer_id
        return page

    def flush(self) -> None:
        for page_id, buffer_id in self.page_table.items():
            frame = self.pool[buffer_id]
            if frame.buffer.is_dirty:
                page = frame.buffer.page
                self.disk.write_page_data(page_id, page)
                frame.buffer.is_dirty = False
                frame.usage_count = 1
