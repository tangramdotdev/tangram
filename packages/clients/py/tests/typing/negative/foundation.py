"""Invalid consumer calls must remain type errors."""

import tangram as tg
from tangram.resolve import resolve


class Record:
    @tg.property
    async def number(self, increment: int = 0) -> int:
        return 1 + increment


async def examples() -> None:
    await Record().number("bad")  # error: invalid-argument-type
    number: int = await resolve("bad")  # error: invalid-assignment
    print(number)


async def mutation_examples() -> None:
    from tangram.mutation import Mutation, mutation

    value = await Mutation.set(1)
    await value.apply("wrong")  # error: invalid-argument-type
    mutation({"kind": "prefix", "template": 123})  # error: invalid-argument-type
