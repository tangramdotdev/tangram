use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.maths]
        # specifier = "./helper.tg.py"
        # ///
        import maths
        result: int = maths.double(21)
    '
    'helper.tg.py': '
        def double(value: int) -> int:
            return value * 2
    '
}
success (tg check ($path | path join main.tg.py) | complete)

# Imported annotations participate in checking the caller.
let source = open --raw ($path | path join main.tg.py)
$source | str replace 'double(21)' 'double("wrong")' | save --force ($path | path join main.tg.py)
let output = tg check ($path | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'int')
assert ($output.stderr | str contains 'main.tg.py:')

# Dependencies are checked too, even if the caller does not use the broken value.
$source | save --force ($path | path join main.tg.py)
'bad: int = "wrong"
def double(value: int) -> int:
    return value * 2
' | save --force ($path | path join helper.tg.py)
let output = tg check ($path | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'helper.tg.py:1:')
