use ../lib/test.nu *

let local = server spawn
for annotation in [int str] {
    let builder = artifact {
        'tangram.ts': ('
            export default async function () {
                const first = await tg.file("value: int = 42").module("py");
                const second = await tg.file("value: str = \"abc\"").module("py");
                const member = await tg.file("value: bool = False").module("py");
                const left = await tg.file("from . import helper\nvalue: ' + $annotation + ' = helper.value")
                    .module("py").dependency("./helper.tg.py", { node: first });
                const right = await tg.file("from . import helper\nvalue: str = helper.value")
                    .module("py").dependency("./helper.tg.py", { node: second });
                const root = await tg.file("from . import helper, left, right\ndef default() -> int:\n    assert helper.value is False\n    return left.value + len(right.value)").module("py");
                return await tg.directory({ "tangram.py": root, "left.tg.py": left, "right.tg.py": right, "helper.tg.py": member });
            }
        ')
    }
    let module = tg run $builder
    let output = tg check $module | complete
    if $annotation == int { success $output } else { failure $output }
    let output = tg run $module | complete
    success $output
    assert equal ($output.stdout | str trim) '45'
}
