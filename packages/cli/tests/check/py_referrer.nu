use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        from . import left, right
        first: int = left.value
        second: str = right.value
    '
    'left.tg.py': '
        # /// script
        # [tool.tangram.imports.common]
        # specifier = "./integer.tg.py"
        # ///
        from common import value
    '
    'right.tg.py': '
        # /// script
        # [tool.tangram.imports.common]
        # specifier = "./string.tg.py"
        # ///
        from common import value
    '
    'integer.tg.py': 'value: int = 42'
    'string.tg.py': 'value: str = "hello"'
}
success (tg check $path | complete)
let source = open --raw ($path | path join tangram.py)
$source | str replace 'second: str' 'second: int' | save --force ($path | path join tangram.py)
failure (tg check $path | complete)
