use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        from math import sqrt
        def square(value: int) -> int:
            return value * value
        answer: int = square(3)
        root: float = sqrt(answer)
    '
}
success (tg check $path | complete)

# Type errors use the original module and source location.
'answer: int = "wrong"' | save --force ($path | path join tangram.py)
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'int')
assert ($output.stderr | str contains 'tangram.py:1:')

# A fresh check observes updated source.
'answer: int = 42' | save --force ($path | path join tangram.py)
success (tg check $path | complete)
