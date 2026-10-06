use ../lib/test.nu *

let local = server spawn
for annotation in [int str] {
    let builder = artifact {
        'tangram.ts': ('
            export default async function () {
                const first = await tg.file("value: int = 42").module("py");
                const second = await tg.file("value: str = \"abc\"").module("py");
                const member = await tg.file("value: bool = False").module("py");
                const graph = await tg.graph({ nodes: [
                    { kind: "file", module: "py", contents: "from . import helper, left, right\ndef default() -> int:\n    assert helper.value is False\n    return left.value + len(right.value)", dependencies: { "./tangram.py": 0, "./left.tg.py": 1, "./right.tg.py": 2, "./helper.tg.py": member } },
                    { kind: "file", module: "py", contents: "from . import helper\nvalue: ' + $annotation + ' = helper.value", dependencies: { "./left.tg.py": 1, "./tangram.py": 0, "./helper.tg.py": first } },
                    { kind: "file", module: "py", contents: "from . import helper\nvalue: str = helper.value", dependencies: { "./right.tg.py": 2, "./tangram.py": 0, "./helper.tg.py": second } },
                ] });
                return await tg.directory({ "tangram.py": await graph.get(0) });
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
