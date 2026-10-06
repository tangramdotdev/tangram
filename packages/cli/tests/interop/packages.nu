use ../lib/test.nu *

let local = server spawn

# A JavaScript package adopts a dragon from a tagged Python package.
let dragons = artifact {
    'tangram.py': '
        from .hatchery import hatch as adopt
        __all__ = ["adopt"]
    '
    'hatchery.tg.py': '
        async def hatch(name: str):
            return await tg.file(f"{name} the Python dragon")
    '
}
tg tag put -p dragons/1.0.0 $dragons
rm --recursive $dragons
let keeper = artifact {
    'tangram.ts': '
        import { adopt } from "dragons/^1";
        export default async function () {
            const dragon = await adopt("Noodle");
            tg.assert(dragon instanceof tg.File);
            return dragon;
        }
    '
}
success (tg check $keeper | complete)
let checked = tg checkin $keeper
rm --recursive $keeper
success (tg check $checked | complete)
let output = tg run $checked | complete
success $output
assert equal (tg cat ($output.stdout | str trim)) 'Noodle the Python dragon'

# A Python package visits a tagged TypeScript planetarium.
let stars = artifact {
    'tangram.ts': 'export { default, chart as map } from "./stars.tg.ts";'
    'stars.tg.ts': '
        export default async function (name: string) { return tg.file(`Hello from ${name}`); }
        export const chart = (name: string) => ({ constellation: name, stars: [1, 2, 3] });
    '
    'comet.tg.js': 'export const visit = () => "a JavaScript comet";'
}
tg tag put -p stars/1.0.0 $stars
rm --recursive $stars
let astronomer = artifact {
    'tangram.py': '
        # /// script
        # [tool.tangram.imports.stars]
        # specifier = "stars/^1"
        # [tool.tangram.imports.comet]
        # specifier = "stars/^1"
        # attributes = { get = "comet.tg.js" }
        # ///
        import stars
        from comet import visit
        async def default():
            assert await stars.map("Orion") == {"constellation": "Orion", "stars": [1, 2, 3]}
            assert await visit() == "a JavaScript comet"
            greeting = await stars.default("Saturn")
            assert isinstance(greeting, tg.File)
            return greeting
    '
}
success (tg check $astronomer | complete)
success (tg py --export default ($astronomer | path join tangram.py) | complete)
let checked = tg checkin $astronomer
rm --recursive $astronomer
success (tg check $checked | complete)
let output = tg run $checked | complete
success $output
assert equal (tg cat ($output.stdout | str trim)) 'Hello from Saturn'
