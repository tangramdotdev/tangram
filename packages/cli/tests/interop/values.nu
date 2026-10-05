use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.ts': '
        import lookingGlass, { echo, fail, explode, alias } from "./mirror.tg.py";
        export default async function () {
            tg.assert(await lookingGlass() === "a Python looking glass");
            tg.assert(await alias("-a") === "-a");
            tg.assert(await tg.command(alias).build().arg("-A") === "-A");
            const value = { count: 42, flag: true, list: ["comet", 1.5], nothing: null };
            tg.assert(tg.encoding.json.encode(await echo(value)) === tg.encoding.json.encode(value));
            const bytes = await echo(new Uint8Array([0, 42, 255]));
            tg.assert(bytes instanceof Uint8Array && bytes[2] === 255);
            const file = await tg.file("space dust");
            const directory = await echo(await tg.directory({ dust: file }));
            tg.assert(directory instanceof tg.Directory);
            const dust = await directory.get("dust");
            tg.assert(dust instanceof tg.File);
            tg.assert(await dust.text === "space dust");
            tg.assert(await tg.command(echo).arg(Promise.resolve("future")).build() === "future");
            for (const function_ of [fail, explode]) {
                let failed = false;
                try { await function_(); } catch { failed = true; }
                tg.assert(failed);
            }
            return "mirrors aligned";
        }
    '
    'mirror.tg.py': '
        # /// script
        # [tool.tangram.imports.mirror]
        # specifier = "./mirror.tg.ts"
        # ///
        import mirror
        async def echo(value):
            return await mirror.echo(value)
        alias = echo
        def default():
            return "a Python looking glass"
        async def fail():
            raise ValueError("the dragon ate the argument")
        async def explode():
            return await mirror.fail()
    '
    'mirror.tg.ts': '
        export const echo = (value: tg.Value) => value;
        export function fail() { throw new Error("the comet lost its tail"); }
    '
}
success (tg check $path | complete)
success (tg check ($path | path join mirror.tg.py) | complete)
let output = tg run $path | complete
success $output
assert ($output.stdout | str contains 'mirrors aligned')

# Cross-language results deliberately remain broad until semantic typing is implemented.
let bad = artifact {
    'tangram.ts': '
        import { echo } from "./mirror.tg.py";
        const value: string = await echo(42);
        await echo(() => 42);
    '
    'mirror.tg.py': 'def echo(value): return value'
}
let output = tg check $bad | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:')
let bad = artifact {
    'tangram.py': '
        # /// script
        # [tool.tangram.imports.mirror]
        # specifier = "./mirror.tg.ts"
        # ///
        import mirror
        async def default():
            value: str = await mirror.echo(42)
            await mirror.echo(object())
    '
    'mirror.tg.ts': 'export const echo = (value: tg.Value) => value;'
}
let output = tg check $bad | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:')
