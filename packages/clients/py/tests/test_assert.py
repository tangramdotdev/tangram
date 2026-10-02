"""Match the assertion helpers and message defaults in assert.ts."""

import unittest

from tangram.assert_ import assert_, todo, unimplemented, unreachable


class AssertTests(unittest.TestCase):
    def test_assertion(self):
        self.assertIsNone(assert_(True))
        with self.assertRaisesRegex(AssertionError, "^failed assertion$"):
            assert_(False)
        with self.assertRaisesRegex(AssertionError, "^custom message$"):
            assert_(False, "custom message")
        with self.assertRaises(AssertionError) as context:
            assert_(False, "")
        self.assertEqual(str(context.exception), "")

    def test_unimplemented_and_unreachable(self):
        for function, default in (
            (unimplemented, "reached unimplemented code"),
            (unreachable, "reached unreachable code"),
        ):
            with self.subTest(function=function.__name__):
                with self.assertRaisesRegex(RuntimeError, f"^{default}$"):
                    function()
                with self.assertRaisesRegex(RuntimeError, "^custom message$"):
                    function("custom message")
                with self.assertRaises(RuntimeError) as context:
                    function("")
                self.assertEqual(str(context.exception), "")

    def test_todo(self):
        with self.assertRaisesRegex(RuntimeError, "^reached todo$"):
            todo()
