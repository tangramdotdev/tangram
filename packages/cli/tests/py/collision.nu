use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.ts': '
        export default async function () {
            const first = await tg.file("value = 1").module("py");
            const second = await tg.file("value = 2").module("py");
            const source = `# /// script
# [tool.tangram.imports.helper]
# specifier = "./helper.tg.py"
# ///
from helper import value`;
            const left = await tg.file(source)
                .module("py").dependency("./helper.tg.py", { node: first });
            const right = await tg.file(source)
                .module("py").dependency("./helper.tg.py", { node: second });
            return await tg.file("from . import left, right\ndef default():\n    assert left.value == 1 and right.value == 2\n    return 0")
                .module("py")
                .dependency("./left.tg.py", { node: left })
                .dependency("./right.tg.py", { node: right });
        }
    '
}
let module = tg run $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '0'

# A recorded dependency can redirect a relative import away from the directory member.
let path = artifact {
    'tangram.ts': '
        export default async function () {
            const member = await tg.file("value = 1").module("py");
            const dependency = await tg.file("value = 2").module("py");
            const initializer = await tg.file("from .helper import value\ndef default():\n    assert value == 2\n    return value")
                .module("py").dependency("./helper.tg.py", { node: dependency });
            return await tg.directory({ "tangram.py": initializer, "helper.tg.py": member });
        }
    '
}
let module = tg run $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '2'

# The same file can have a distinct package context in a dependency referent.
let path = artifact {
    'tangram.ts': '
        export default async function () {
            const helper = await tg.file("from . import value").module("py");
            const root = await tg.file("value = 1\nfrom . import helper\ndef default():\n    return helper.value")
                .module("py")
                .dependency("./helper.tg.py", { node: helper, options: { path: "other/helper.tg.py" } });
            const other = await tg.file("value = 2").module("py");
            return await tg.directory({
                "tangram.py": root,
                "helper.tg.py": helper,
                other: { "tangram.py": other, "helper.tg.py": helper },
            });
        }
    '
}
let module = tg run $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '2'

# An unnamed file has the same package classification through either import route.
mut packages = []
for imports in ['import alias; from . import leaf' 'from . import leaf; import alias'] {
    let path = artifact {
        'tangram.ts': ('
            export default async function () {
                const leaf = await tg.file("package = __spec__.submodule_search_locations is not None").module("py");
                return await tg.file(`# /// script
# [tool.tangram.imports.alias]
# specifier = "./leaf.tg.py"
# ///
' + $imports + '
assert alias is leaf
def default():
    return leaf.package`)
                    .module("py").dependency("./leaf.tg.py", { node: leaf });
            }
        ')
    }
    let module = tg run $path
    let output = tg run $module | complete
    success $output
    $packages = $packages | append ($output.stdout | str trim)
}
assert equal $packages.0 $packages.1

# An unresolved dependency must not fall back to a directory member.
let path = artifact {
    'tangram.ts': '
        export default async function () {
            const helper = await tg.file("value = 1").module("py");
            const initializer = await tg.file("try:\n    from . import helper\nexcept tg.Error as error:\n    assert error.to_data().get(\"source\") is not None\nelse:\n    raise AssertionError(\"resolved an unresolved dependency\")\ndef default():\n    return 42")
                .module("py").dependency("./helper.tg.py", null);
            return await tg.directory({ "tangram.py": initializer, "helper.tg.py": helper });
        }
    '
}
let module = tg run $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '42'
