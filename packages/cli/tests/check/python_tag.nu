use ../lib/test.nu *

let local = server spawn
let dependency = artifact {
    'tangram.py': 'from .helper import double'
    'helper.tg.py': '
        def double(value: int) -> int:
            return value * 2
    '
}
tg tag put -p python-check/1.0.0 $dependency
rm --recursive $dependency
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.maths]
        # specifier = "python-check/^1"
        # ///
        from maths import double
        result: int = double(21)
    '
}
success (tg check ($path | path join main.tg.py) | complete)
let source = open --raw ($path | path join main.tg.py)
$source | str replace 'double(21)' 'double("wrong")' | save --force ($path | path join main.tg.py)
let output = tg check ($path | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'int')

# The same graph can be loaded entirely from checked-in artifacts.
$source | save --force ($path | path join main.tg.py)
let checked = tg checkin ($path | path join main.tg.py)
rm --recursive $path
success (tg check $checked | complete)
