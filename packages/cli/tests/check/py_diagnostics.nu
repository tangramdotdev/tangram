use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.missing]
        # specifier = "./missing.tg.py"
        # ///
        import missing
        def accept(value: int) -> int:
            return value
        answer = accept("wrong")
        other: str = 42
    '
}
let source = open --raw ($path | path join main.tg.py)

# Use recorded unresolved edges so the checker's resolution fails after CLI root lookup.
for specifier in ['./missing.tg.py' 'missing-python-package/^1'] {
    let source = $source | str replace './missing.tg.py' $specifier
    let builder = artifact {
        'tangram.ts': ('export default async function () { return tg.directory({ "tangram.py": tg.file(' + ($source | to json -r) + ').module("py").dependency(' + ($specifier | to json -r) + ', null) }); }')
    }
    let module = tg run $builder
    let output = tg check $module | complete
    failure $output
    assert ($output.stderr | str contains 'tangram.py:5:') $output.stderr
    assert ($output.stderr | str contains 'tangram.py:8:') $output.stderr
    assert ($output.stderr | str contains 'tangram.py:9:') $output.stderr
    assert ($output.stderr | str contains 'Parameter declared here') $output.stderr
    assert ($output.stderr | str contains 'int') $output.stderr
    assert ($output.stderr | str contains 'str') $output.stderr
}

# Tangram diagnostic columns use UTF-8 byte offsets, including after non-ASCII text.
let path = artifact {'main.tg.py': 'é = "😀"; value: int = "wrong"'}
let output = tg check ($path | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'main.tg.py:1:27') $output.stderr
assert ($output.stderr | str contains 'wrong') $output.stderr

# Diagnostic labels use original source names without changing canonical module identity.
let path = artifact {
    'tangram.py': 'from . import helper
value = helper.missing'
    'helper.tg.py': 'value: int = 42'
}
for module in [$path (tg checkin $path)] {
    let output = tg check $module | complete
    failure $output
    assert ($output.stderr | str contains 'helper.tg.py` has no member `missing`') $output.stderr
    assert not ($output.stderr | str contains 'Module `m1`')
}
let path = artifact {
    'tangram.py': 'from . import namespace
value = namespace.missing'
    namespace: {'leaf.tg.py': 'value: int = 42'}
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'namespace` has no member `missing`') $output.stderr

# Modules without a source path are identified by their canonical referent.
let builder = artifact {
    'tangram.ts': 'export default async function () {
        const dep = await tg.file("value: int = 42").module("py");
        const source = "# /// script\n# [tool.tangram.imports.dep]\n# specifier = \"dep\"\n# ///\nimport dep\nvalue = dep.missing";
        return tg.directory({ "tangram.py": tg.file(source).module("py").dependency("dep", { node: dep, options: { id: dep.id } }) });
    }'
}
let module = tg run $builder
let output = tg check $module | complete
failure $output
assert ($output.stderr | str contains 'fil_') $output.stderr
assert ($output.stderr | str contains 'has no member `missing`') $output.stderr
assert not ($output.stderr | str contains 'Module `m1`')
