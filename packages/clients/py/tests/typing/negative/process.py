"""These invalid operations must be rejected by the checker."""

from tangram.process import Process
from tangram.process.stdio import Reader, Writer


async def invalid(process: Process[str], reader: Reader, writer: Writer):
    await writer.write("text")  # error: invalid-argument-type
    await reader.read("length")  # error: invalid-argument-type
    await process.signal(15)  # error: invalid-argument-type
    process.run().cwd(123)  # error: invalid-argument-type
    process.run().connection("connect")  # error: invalid-argument-type
