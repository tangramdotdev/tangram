use ../../util/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.ts': '
        import * as functions from "./functions.tg.py";
        export default async function () {
            for (const name of ["default", "delete", "f0", "args", "tg"] as const) {
                const command = functions[name];
                tg.assert(await command.build("direct") === `${name}:direct`);
                tg.assert(await tg.build(command).arg(Promise.resolve("builder")) === `${name}:builder`);
            }
            return "javascript commands found the python exports";
        }
    '
    'functions.tg.py': '
        def default(value): return f"default:{value}"
        def delete(value): return f"delete:{value}"
        def f0(value): return f"f0:{value}"
        def args(value): return f"args:{value}"
        def tg(value): return f"tg:{value}"
    '
    'main.tg.py': '
        # /// script
        # [tool.tangram.dependencies.functions]
        # specifier = "./functions.tg.ts"
        # ///
        import functions
        async def default():
            for name in ["default", "args", "tg", "_tg_client", "_tg_args"]:
                command = getattr(functions, name)
                assert await command.build("direct") == f"{name}:direct"
                assert await tg.build(command).arg("builder") == f"{name}:builder"
            return "python commands found the javascript exports"
    '
    'functions.tg.ts': '
        export default (value: unknown) => `default:${value}`;
        export const args = (value: unknown) => `args:${value}`;
        export const tg = (value: unknown) => `tg:${value}`;
        export const _tg_client = (value: unknown) => `_tg_client:${value}`;
        export const _tg_args = (value: unknown) => `_tg_args:${value}`;
    '
}
for name in ['main.tg.ts', 'main.tg.py'] {
    let module = $path | path join $name
    success (tg check $module | complete)
    let checked = tg checkin $module
    let output = tg run $checked | complete
    success $output
    assert ($output.stdout | str contains 'commands found the')
}
