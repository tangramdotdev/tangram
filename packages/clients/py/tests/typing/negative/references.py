from tangram.checksum import Checksum
from tangram.path import join
from tangram.referent import Referent


async def check() -> None:
    await Checksum.new("contents", "md5")  # error: invalid-argument-type


join(42)  # error: invalid-argument-type
bad_options = object()
Referent("id", bad_options)  # error: invalid-argument-type
