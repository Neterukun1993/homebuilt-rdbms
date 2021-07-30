import pytest
from src.disk import PageID, PAGE_SIZE, DiskManager


class TestPage:

    def test_create_page_id(self):
        page_id = PageID(10)
        assert page_id.to_int() == 10

    def test_page_size(self):
        assert PAGE_SIZE == 4096


@pytest.fixture
def empty_disk(tmp_path):
    file_path = tmp_path / "test"
    disk = DiskManager(file_path)
    return disk


def to_page_data(string: str) -> bytearray:
    page_data = bytearray(PAGE_SIZE)
    for i, char in enumerate(string):
        page_data[i] = ord(char)
    return page_data


class TestDisk:
    hello = to_page_data("hello")
    world = to_page_data("world")

    def test_allocate_page(self, empty_disk):
        disk = empty_disk
        page_id = disk.allocate_page()
        next_page_id = disk.allocate_page()
        assert page_id.to_int() + 1 == next_page_id.to_int()

    def test_write_to_disk_and_read_from_disk(self, empty_disk):
        disk = empty_disk

        id_hello = disk.allocate_page()
        disk.write_page_data(id_hello, self.hello)

        id_world = disk.allocate_page()
        disk.write_page_data(id_world, self.world)

        assert disk.read_page_data(id_hello) == self.hello
        assert disk.read_page_data(id_world) == self.world
