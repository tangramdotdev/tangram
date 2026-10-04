use ../lib/test.nu *

let local = server spawn
let source = '# /// script
# [tool.tangram.imports.tools]
# specifier = "tools/^1"
# attributes = { get = "debug/tangram.py" }
# ///

def default( ):
 return {"value":42}'
let expected = '# /// script
# [tool.tangram.imports.tools]
# specifier = "tools/^1"
# attributes = { get = "debug/tangram.py" }
# ///


def default():
    return {"value": 42}
'
let path = artifact {
    '.tangramignore': 'ignored'
    'tangram.py': $source
    'helper.tg.py': $source
    'plain.py': $source
    nested: {'tangram.py': $source}
    ignored: {'tangram.py': $source}
}

# Format Python modules while preserving metadata and respecting ignored paths.
tg format $path
for module in ['tangram.py' 'helper.tg.py' 'nested/tangram.py'] {
    assert equal (open --raw ($path | path join $module)) $expected
}
for module in ['plain.py' 'ignored/tangram.py'] {
    assert equal (open --raw ($path | path join $module)) $source
}

# Formatting an individual module is idempotent.
let module = $path | path join helper.tg.py
tg format $module
assert equal (open --raw $module) $expected
$source | save --force $module
tg format $module
assert equal (open --raw $module) $expected

# Invalid Python fails without overwriting the source.
let invalid = 'def broken('
$invalid | save --force $module
let output = tg format $module | complete
failure $output
assert ($output.stderr | str contains 'failed to format the module')
assert equal (open --raw $module) $invalid
