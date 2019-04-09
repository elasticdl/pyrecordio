import unittest
import tempfile

from .file_index import FileIndex
from .writer import Writer
from .reader import RangeReader


class ReaderTest(unittest.TestCase):
    def test_read(self):
        data_source = [
            b"china",
            b"usa",
            b"russia",
            b"india",
            b"thailand",
            b"finland",
            b"france",
            b"germany",
            b"poland",
            b"san marino",
            b"sweden",
            b"neuseeland",
            b"argentina",
            b"canada",
            b"ottawa",
            b"bogota",
            b"panama",
            b"united states",
            b"brazil",
            b"barbados",
        ]

        with tempfile.NamedTemporaryFile() as tmp_file:
            # Use a small chunksize so multiple chunks are created.
            writer = Writer(tmp_file, 20)

            for data in data_source:
                writer.write(data)
            writer.flush()

            tmp_file.seek(0)
            index = FileIndex(tmp_file)

            # Ensure RangeReader has same semantics as List.
            for start in range(0, len(data_source) + 3, 3):
                for end in range(0, len(data_source) + 3, 3):
                    reader = RangeReader(tmp_file, index, start, end)
                    self.assertEqual(
                        data_source[start:end],
                        list(reader),
                        msg=f"start: {start} end: {end}",
                    )


if __name__ == "__main__":
    unittest.main()
