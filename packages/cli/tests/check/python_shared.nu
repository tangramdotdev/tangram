use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        from . import left, right
        first: int = left.value
        second: int = right.value
    '
    'left.tg.py': 'from .shared import value'
    'right.tg.py': 'from .shared import value'
    'shared.tg.py': 'value: int = "wrong"'
}
let output = tg check $path | complete
failure $output
assert equal ($output.stderr | lines | where {|line| $line | str contains 'shared.tg.py:1:'} | length) 1
