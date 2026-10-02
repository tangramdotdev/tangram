"""Consumer inference examples for resolution, builders, and async getters."""

from collections.abc import Awaitable
from typing import Self, assert_type

from tangram.async_property import async_property
from tangram.builder import Builder
from tangram.resolve import resolve


class Handle:
    __tangram_atomic__ = None


class NumberBuilder(Builder[int]):
    def increment(self) -> Self:
        return self

    async def _create(self) -> int:
        return 1


class Record:
    @async_property
    async def number(self, increment: int = 0) -> int:
        return 1 + increment


async def examples(number: int, pending: Awaitable[int], handle: Handle) -> None:
    assert_type(await resolve(number), int)
    assert_type(await resolve(pending), int)
    assert_type(await resolve(handle), Handle)
    assert_type(await resolve([pending]), list[int])
    assert_type(await resolve({"number": pending}), dict[str, int])
    assert_type(await NumberBuilder().increment(), int)
    record = Record()
    assert_type(await record.number, int)
    assert_type(await record.number(3), int)


async def value_examples(pending: Awaitable[int]) -> None:
    from tangram.mutation import Mutation, Unset
    from tangram.template import Template
    from tangram.value import Value, ValueData, ValueInput, ValueType

    nested: ValueInput = {"nested": [pending]}
    encoded: ValueData = Value.to_data(await resolve(nested))
    decoded: ValueType = Value.from_data(encoded)
    assert_type(decoded, ValueType)
    assert_type(await Mutation.set(nested), Mutation[ValueType])
    mutation = await Mutation.set(pending)
    assert_type(mutation, Mutation[int])
    assert_type(await mutation.apply(1), int | Unset)
    assert_type(await Mutation.append([pending]), Mutation[list[int]])
    assert_type(await Mutation.prefix("prefix"), Mutation[Template])


async def mutation_values() -> None:
    from tangram.mutation import Mutation
    from tangram.value import ValueType

    mutation = await Mutation.set(1)
    value: ValueType = mutation
    assert_type(mutation, Mutation[int])
    _ = value
