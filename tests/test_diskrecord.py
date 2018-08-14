import struct
import tempfile
import unittest

from pixiv_fetcher.cache.strategy import DiskRecord


class TestDiskRecord(unittest.TestCase):

    def setUp(self):
        self.temp_file = tempfile.mktemp()
        self.dr = DiskRecord(self.temp_file, key_len=4,
                             key_func=lambda k: struct.pack("<L", k))

    def tearDown(self):
        super(TestDiskRecord, self).tearDown()
        self.dr.flush()

    def test_get_set(self):
        self.assertEqual(self.dr.length(), 0)
        self.dr.set(0xff, 1)
        self.dr.set(0xffff, 2)
        self.assertEqual(self.dr.length(), 2)
        self.assertEqual(self.dr.get(0xffff), 2)

    def test_insert(self):
        self.dr.set(1, 1)
        self.dr.insert(4, 2, 3)
        self.dr.insert(0, 3, 4)
        self.dr.insert(1, 4, 5)

        self.dr.set(4, 100)

        self.assertTrue(self.dr.has(4))
        self.assertEqual(self.dr.pop_idx()[1], 3)
        self.assertEqual(self.dr.pop_idx()[1], 1)
        self.assertEqual(self.dr.pop_idx()[1], 100)
        self.assertFalse(self.dr.has(4))
        self.assertEqual(self.dr.pop_idx()[1], 4)

    def test_pop(self):
        self.dr.set(1, 1)
        self.dr.set(2, 2)
        self.dr.set(3, 3)
        self.dr.set(4, 5)
        self.assertEqual(self.dr.pop_idx()[1], 5)
        self.assertEqual(self.dr.pop_idx(-2)[1], 2)
        self.assertEqual(self.dr.pop_idx(0)[1], 1)
        self.assertEqual(self.dr.length(), 1)

        self.dr.set(0, 4)
        self.assertEqual(self.dr.pop(0), 4)
        self.assertEqual(self.dr.pop(3), 3)

    def test_swap(self):
        self.dr.set(1, 2)
        self.dr.set(2, 3)
        self.dr.set(3, 4)
        self.dr.set(4, 5)
        self.dr.swap(0, 2)
        self.dr.swap(1, 3)
        result = [self.dr.pop_idx(0)[1] for i in range(self.dr.length())]
        self.assertEqual(result, [4, 5, 2, 3])


if __name__ == '__main__':
    unittest.main()
