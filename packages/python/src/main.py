"""Runtime entry point - sets up tg global and start function.

This module mirrors the JavaScript packages/js/src/main.ts:
1. Imports the client library
2. Sets up the handle
3. Makes tg available globally
4. Exports the start function
"""

import tangram
from handle import Handle
from start import start

# Set the handle.
tangram.set_handle(Handle())

# Create tg as an alias for tangram.
tg = tangram

__all__ = ["tg", "start"]
