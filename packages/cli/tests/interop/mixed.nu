use ../lib/test.nu *

let local = server spawn

# A TypeScript astronaut orders a Python cake with TypeScript icing.
# The Python baker imports the entry package again, creating a dependency cycle.
let path = artifact {
    'tangram.ts': '
        import { bake, bake as sameBake } from "./bakery.tg.py";
        export { decorate } from "./icing.tg.ts";
        export default async function () {
            tg.assert(bake === sameBake);
            const order = await tg.file("moon cake");
            const cake = await bake(order);
            tg.assert(cake instanceof tg.File);
            tg.assert(await cake.text === "moon cake with stardust icing");
            const another = await tg.command(bake).arg(order).build();
            tg.assert(another instanceof tg.File);
            tg.assert(await another.text === await cake.text);
            return cake;
        }
    '
    'bakery.tg.py': '
        # /// script
        # [tool.tangram.imports.icing]
        # specifier = "./tangram.ts"
        # ///
        from icing import decorate
        __all__ = ["bake"]
        async def bake(order: tg.File) -> tg.File:
            cake = await decorate(order, "stardust")
            assert isinstance(cake, tg.File)
            assert await cake.text == "moon cake with stardust icing"
            again = await tg.command(decorate).arg(order, "stardust").build()
            assert isinstance(again, tg.File)
            assert await again.text() == await cake.text
            return cake
    '
    'icing.tg.ts': '
        export const decorate = async (order: tg.File, flavor: string) => {
            return tg.file(`${await order.text} with ${flavor} icing`);
        };
    '
}
success (tg check $path | complete)
success (tg check ($path | path join bakery.tg.py) | complete)
let output = tg run $path | complete
success $output
assert equal (tg cat ($output.stdout | str trim)) 'moon cake with stardust icing'

# Execute the same mixed-language graph without any original files.
let checked = tg checkin $path
let python = tg checkin ($path | path join bakery.tg.py)
rm --recursive $path
success (tg check $checked | complete)
success (tg check $python | complete)
let output = tg run $checked | complete
success $output
assert equal (tg cat ($output.stdout | str trim)) 'moon cake with stardust icing'
