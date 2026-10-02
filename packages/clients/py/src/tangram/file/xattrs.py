"""Read Tangram process attributes stored as values or numbered shards."""

from .. import host

ERROR_NAME = "user.tangram.error"
OUTCOME_NAME = "user.tangram.outcome"
OUTPUT_NAME = "user.tangram.output"


async def read_error(path: str) -> bytes | None:
    return await read_process_attribute(path, ERROR_NAME)


async def read_outcome(path: str) -> bytes | None:
    return await read_process_attribute(path, OUTCOME_NAME)


async def read_output(path: str) -> bytes | None:
    return await read_process_attribute(path, OUTPUT_NAME)


async def read_process_attribute(path: str, name: str) -> bytes | None:
    """An empty process attribute marks its contents as the serialized value."""
    value = await read_sharded(path, name)
    return (
        await host.read_file(path) if value is not None and len(value) == 0 else value
    )


async def read_sharded(path: str, name: str) -> bytes | None:
    """Read a Tangram attribute stored as a single value or numbered shards."""
    value = await host.getxattr(path, name)
    prefix = name + "."
    names = await host.listxattr(path)
    indices = {}
    for attribute_name in names:
        if not attribute_name.startswith(prefix):
            continue
        suffix = attribute_name[len(prefix) :]
        if (
            not suffix.isascii()
            or not suffix.isdecimal()
            or len(suffix) > 16
            or int(suffix) > 2**53 - 1
            or suffix != str(int(suffix))
        ):
            raise ValueError("invalid xattr shard name")
        indices[int(suffix)] = attribute_name
    if len(indices) == 0:
        return value
    if value is not None:
        raise ValueError("found both unsharded and sharded xattrs")

    # Read the numbered shards in order before decoding the value.
    shards = []
    size = 0
    entries = sorted(indices.items())
    for expected, (index, attribute_name) in enumerate(entries):
        if index != expected:
            raise ValueError("found a gap in the xattr shards")
        shard = await host.getxattr(path, attribute_name)
        if shard is None:
            raise ValueError("an xattr shard disappeared")
        shards.append(shard)
        size += len(shard)
    output = bytearray(size)
    offset = 0
    for shard in shards:
        output[offset : offset + len(shard)] = shard
        offset += len(shard)
    return bytes(output)


read_xattr = read_sharded
