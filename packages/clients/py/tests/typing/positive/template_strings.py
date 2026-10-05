"""T-string values are opaque; interpolation types are validated at runtime."""

import sys
from collections.abc import Awaitable
from typing import assert_type

import tangram as tg

if sys.version_info >= (3, 14):
    from string.templatelib import Template

    async def examples(value: Template, pending: Awaitable[Template]) -> None:
        assert_type(await tg.resolve(value), Template)
        assert_type(await tg.resolve(pending), Template)
        assert_type(await tg.template(value), tg.Template)
        assert_type(await tg.template(pending), tg.Template)
        assert_type(await tg.Template.new(pending), tg.Template)
        assert_type(await tg.Template.raw(value), tg.Template)
        assert_type(await tg.Template.join(value, value), tg.Template)
        assert_type(await tg.file(value).executable(), tg.File)
        assert_type(await tg.file(pending), tg.File)
        assert_type(await tg.File.new(pending), tg.File)
        assert_type(await tg.File.Builder(True, value), tg.File)
