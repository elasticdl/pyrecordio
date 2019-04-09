from .chunk import Chunk


class RangeReader(object):
    """
    Reader that returns records in a given range.
    """

    def __init__(self, in_file, file_index, start=0, end=None):
        """
        in_file: File object of underlying data file.
        file_index: Index object of the RecordIO file.
        start, end: start and end record number of the range. By default, start
            from 0 and end at the last record in the file. Has the same
            semantics as python list[start:end].
    """
        self._data_file = in_file
        self._file_index = file_index
        self._start = start
        self._end = file_index.total_records() if end is None else end
        self._chunk_idx, self._idx_in_chunk = file_index.locate_record(start)
        self._chunk = None

    def __next__(self):
        if (
            self._start >= self._end
            or self._chunk_idx < 0
            or self._idx_in_chunk < 0
        ):
            raise StopIteration()

        if self._chunk is None:
            self._chunk = Chunk.parse(
                self._data_file, self._file_index.chunk_offset(self._chunk_idx)
            )

        record = self._chunk.get(self._idx_in_chunk)
        self._start += 1
        self._idx_in_chunk += 1
        if self._idx_in_chunk >= self._file_index.chunk_records(
            self._chunk_idx
        ):
            self._chunk_idx += 1
            self._chunk = None
            self._idx_in_chunk = 0
            if self._chunk_idx >= self._file_index.total_chunks():
                self._chunk_idx = -1

        return record

    def __iter__(self):
        return self
