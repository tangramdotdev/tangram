use ../lib/test.nu *

let local = server spawn
let package = artifact {
    'tangram.py': 'base = 40'
    'task.tg.py': '
        from . import base
        from .helper import delta
        def default():
            return base + delta
    '
    'helper.tg.py': 'delta = 2'
    'unused.tg.py': 'unused = 1'
}
tg tag put -p tasks/1.0.0 $package
tg tag put -p alias/1.0.0 $package
let javascript = artifact {
    'tangram.ts': '
        import task from "tasks/^1" with { get: "task.tg.py" };
        export default async function () {
            const command = await tg.command(task);
            const module = (await command.args)[3].value as tg.Module;
            for (const key of ["id", "name", "path", "tag"] as const) {
                tg.assert(module.referent.options?.[key] == null);
            }
            return command;
        }
    '
}
let python = artifact {
    'tangram.py': '
        # /// script
        # [tool.tangram.imports.task]
        # specifier = "alias/^1"
        # attributes = { get = "task.tg.py" }
        # ///
        import task
        async def default():
            command = await tg.command(task.default)
            module = (await command.args)[3].value
            assert all(module.referent.options.get(key) is None for key in ("id", "name", "path", "tag"))
            return command
    '
}
let first = tg run $javascript | split row '?' | first
assert equal (tg run $python | split row '?' | first) $first
assert equal (tg run $first | str trim) '42'

# Retagging a package after an unrelated edit preserves its member command.
'unused = 2' | save --force ($package | path join unused.tg.py)
tg tag put -p tasks/1.0.1 $package
tg tag put -p alias/1.0.1 $package
tg update $javascript
tg update $python
assert equal (tg run $javascript | split row '?' | first) $first
assert equal (tg run $python | split row '?' | first) $first

# Required module changes alter both clients' command identities equally.
'delta = 3' | save --force ($package | path join helper.tg.py)
tg tag put -p tasks/1.0.2 $package
tg tag put -p alias/1.0.2 $package
tg update $javascript
tg update $python
let second = tg run $javascript | split row '?' | first
assert ($second != $first)
assert equal (tg run $python | split row '?' | first) $second
assert equal (tg run $second | str trim) '43'
