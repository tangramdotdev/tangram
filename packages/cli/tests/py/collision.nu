use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.ts': '
        export default async function () {
            const first = await tg.file("value = 1").module("py");
            const second = await tg.file("value = 2").module("py");
            const left = await tg.file("from .helper import value")
                .module("py").dependency("./helper.tg.py", { node: first });
            const right = await tg.file("from .helper import value")
                .module("py").dependency("./helper.tg.py", { node: second });
            return await tg.file("from . import left, right\ndef default():\n    return 0")
                .module("py")
                .dependency("./left.tg.py", { node: left })
                .dependency("./right.tg.py", { node: right });
        }
    '
}
let module = tg run $path
let output = tg run $module | complete
failure $output
assert ($output.stderr | str contains 'conflicting Python modules at the same path')
assert ($output.stderr | str contains '/tangram/helper.tg.py')
