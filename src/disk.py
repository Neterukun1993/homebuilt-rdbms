from typing import IO, Union
import pathlib


PAGE_SIZE: int = 4096


class PageID:
    page_id: int

    def __init__(self, page_id: int) -> None:
        self.page_id = page_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PageID):
            return False
        return self.page_id == other.page_id

    def __hash__(self) -> int:
        return hash(self.page_id)

    def to_int(self) -> int:
        return self.page_id


class DiskManager:
    heap_file: IO[bytes]
    next_page_id: int

    def __init__(self, heap_file_path: pathlib.Path) -> None:
        if not heap_file_path.is_file():
            heap_file_path.touch()
        self.heap_file = heap_file_path.open(mode='br+')
        file_size = heap_file_path.stat().st_size
        self.next_page_id = file_size // PAGE_SIZE

    def allocate_page(self) -> PageID:
        page_id = self.next_page_id
        self.next_page_id += 1
        return PageID(page_id)

    def write_page_data(self, page_id: PageID,
                        data: Union[bytes, bytearray]) -> None:
        offset = PAGE_SIZE * page_id.to_int()
        self.heap_file.seek(offset)
        self.heap_file.write(data)

    def read_page_data(self, page_id: PageID) -> bytearray:
        offset = PAGE_SIZE * page_id.to_int()
        self.heap_file.seek(offset)
        return bytearray(self.heap_file.read(PAGE_SIZE))
