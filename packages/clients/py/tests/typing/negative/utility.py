from tangram.error import ErrorBuilder
from tangram.queue import Queue


def invalid(builder: ErrorBuilder, queue: Queue[str]):
    builder.code(123)  # error: invalid-argument-type
    builder.kind("unknown")  # error: invalid-argument-type
    queue.next(42)  # error: invalid-argument-type
