from typing import assert_type

from tangram.checksum import Algorithm, Checksum, checksum
from tangram.path import components, from_components, join, parent
from tangram.reference import Reference, ReferenceData
from tangram.referent import Referent, ReferentData


def decode_int(value: str) -> int:
    return int(value)


async def check() -> None:
    options: Referent.Options = {"path": "relative"}
    reference_options: Reference.Options = {"path": "relative"}
    Referent(42, options)
    Reference(42, reference_options)
    referent = Referent(42)
    assert_type(referent, Referent[int])
    assert_type(referent.to_data(), ReferentData[int])
    assert_type(referent.to_data(str), ReferentData[str])
    assert_type(referent.without_location_and_tokens(), Referent[int])
    assert_type(Referent.with_node_and_local_tokens(42, ["proof"]), Referent[int])
    data: ReferentData[str] = {"node": "42", "options": {"path": "relative"}}
    assert_type(Referent.from_data(data, decode_int), Referent[int])
    assert_type(Referent.from_data_string("42", int), Referent[int])
    reference = Reference(42)
    assert_type(reference.to_data(str), ReferenceData[str])
    reference_data: ReferenceData[str] = {"node": "42"}
    assert_type(Reference.from_data(reference_data, decode_int), Reference[int])
    assert_type(Reference.from_data_string("42", int), Reference[int])
    assert_type(Reference.without_tokens(reference_data), ReferenceData[str])
    assert_type(await checksum("contents", "sha256"), str)
    assert_type(Checksum.algorithm("sha256:abcd"), Algorithm)
    assert_type(components("/a/b"), list[str])
    assert_type(from_components(["/", "a", "b"]), str)
    assert_type(join(None, "/a", "b"), str)
    assert_type(parent("/a"), str | None)
