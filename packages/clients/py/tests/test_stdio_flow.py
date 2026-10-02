import unittest

from tangram.process.stdio.flow import (
    Receiver,
    capacity,
    chunk_size,
    max_chunks,
    window,
)


class FlowTests(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(chunk_size, 32 * 1024)
        self.assertEqual(max_chunks, 64)
        self.assertEqual(capacity, 132)
        self.assertEqual(window, 2 * 1024 * 1024)

    def test_byte_threshold_and_cumulative_reports(self):
        receiver = Receiver()
        self.assertIsNone(receiver.consume(window // 2 - 1))
        self.assertEqual(receiver.consume(1), {"consumed": window // 2})
        self.assertIsNone(receiver.consume(window // 2 - 1))
        self.assertEqual(receiver.consume(1), {"consumed": window})

    def test_chunk_threshold_and_reset(self):
        receiver = Receiver()
        for _ in range(31):
            self.assertIsNone(receiver.consume(1))
            self.assertIsNone(receiver.consume(0))
        self.assertEqual(receiver.consume(1), {"consumed": 32})
        for _ in range(31):
            self.assertIsNone(receiver.consume(1))
        self.assertEqual(receiver.consume(1), {"consumed": 64})

    def test_read_attempts_have_independent_counts(self):
        first = Receiver()
        second = Receiver()
        self.assertEqual(first.consume(window), {"consumed": window})
        self.assertEqual(second.consume(window // 2), {"consumed": window // 2})

    def test_safe_integer_limit(self):
        receiver = Receiver()
        self.assertEqual(receiver.consume(2**53 - 1), {"consumed": 2**53 - 1})
        with self.assertRaisesRegex(ValueError, "the stdio byte count is too large"):
            receiver.consume(1)

    def test_non_integer_and_non_finite_counts(self):
        for value in (0.5, float("nan"), float("inf"), -(2**53)):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError, "the stdio byte count is too large"
                ):
                    Receiver().consume(value)


if __name__ == "__main__":
    unittest.main()
