use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        from . import left
        value: int = left.read()
    '
    'left.tg.py': '
        from . import right
        value: int = 1
        def read() -> int:
            return right.read()
    '
    'right.tg.py': '
        from . import left
        def read() -> int:
            return left.value
    '
}
success (tg check $path | complete)
