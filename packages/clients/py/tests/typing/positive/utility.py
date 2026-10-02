"""Structured metadata and utility inference for consumers."""

from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from typing import assert_type

from tangram.diagnostic import Diagnostic, DiagnosticObject
from tangram.error import Error, ErrorBuilder
from tangram.http import Response
from tangram.module import Module, ModuleDataObject
from tangram.progress import Event, last_output, progress
from tangram.queue import IteratorResult, Queue
from tangram.stop import Stop


async def utilities(response: Response, input: AsyncIterable[str]):
    queue = Queue(input)
    assert_type(await queue.next(), IteratorResult[str])
    assert_type(await anext(queue), str)
    events = progress(response, str)
    assert_type(events, AsyncIterator[Event[str]])
    assert_type(await last_output(events), str | None)
    stop: Awaitable[None] = Stop().promise
    assert_type(await stop, None)


async def errors(builder: ErrorBuilder):
    assert_type(builder.code("missing").message("failed"), ErrorBuilder)
    assert_type(await builder, Error)
    error = await builder
    assert_type(await error.message, str | None)


def metadata(module: Module, diagnostic: DiagnosticObject):
    assert_type(module.to_data(), ModuleDataObject)
    assert_type(Diagnostic.from_data(Diagnostic.to_data(diagnostic)), DiagnosticObject)
