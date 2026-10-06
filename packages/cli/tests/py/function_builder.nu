use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': '
        import asyncio

        async def cake(first, second):
            return await tg.file(f"{first} cake with {second} icing")

        async def default():
            async def flavor():
                await asyncio.sleep(0)
                return "chocolate"

            shared = flavor()
            resolved = await tg.resolve({"command": cake})
            assert isinstance(resolved["command"], tg.Command)
            return await tg.build(cake, shared).arg(shared)
    '
}
let output = tg build ($path | path join main.tg.py) | complete
success $output
assert equal (tg cat ($output.stdout | str trim)) 'chocolate cake with chocolate icing'
