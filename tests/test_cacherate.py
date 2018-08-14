import random
import threading
import unittest

from pixiv_fetcher.cache import _CacheRate


class TestCacheRate(unittest.TestCase):

    def setUp(self):
        self._rate = _CacheRate()

    def tearDown(self):
        self._rate.reset()

    def test_reset(self):
        for i in range(random.randint(0, 100)):
            self._rate.update(random.randint(0, 1))
        self._rate.reset()

        self.assertEqual(self._rate.hit_count, 0)
        self.assertEqual(self._rate.missing_count, 0)

    def test_single_thread(self):
        hit_num = random.randint(0, 300)
        mis_num = random.randint(0, 300)

        for i in range(0, hit_num):
            self._rate.hit()
        for i in range(0, mis_num):
            self._rate.missing()

        self.assertEqual(self._rate.total, hit_num + mis_num)

        self.assertEqual(self._rate.hit_count, hit_num)
        self.assertEqual(self._rate.missing_count, mis_num)

        self.assertEqual(self._rate.hit_rate, hit_num / float(mis_num + hit_num))
        self.assertEqual(self._rate.missing_rate, mis_num / float(mis_num + hit_num))

    def test_multi_threading(self):
        thread_num = 10

        hit_nums = [random.randint(0, 100) for _ in range(thread_num)]
        mis_nums = [random.randint(0, 100) for _ in range(thread_num)]

        def _thread(num, is_hit):
            for _ in range(num):
                self._rate.update(is_hit)

        threads = []
        for h_n, m_n in zip(hit_nums, mis_nums):
            thread = threading.Thread(target=_thread, args=(h_n, True))
            thread.start()
            threads.append(thread)

            thread = threading.Thread(target=_thread, args=(m_n, False))
            thread.start()
            threads.append(thread)

        [t.join() for t in threads]

        total_hit_num = sum(hit_nums)
        total_mis_num = sum(mis_nums)

        self.assertEqual(self._rate.total, total_hit_num + total_mis_num)

        self.assertEqual(self._rate.hit_count, total_hit_num)
        self.assertEqual(self._rate.missing_count, total_mis_num)

        self.assertEqual(self._rate.hit_rate, total_hit_num
                         / float(total_mis_num + total_hit_num))
        self.assertEqual(self._rate.missing_rate, total_mis_num
                         / float(total_mis_num + total_hit_num))


if __name__ == '__main__':
    unittest.main()
