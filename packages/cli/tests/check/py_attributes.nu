use ../lib/test.nu *

let local = server spawn
let dependency = artifact {
    'tangram.py': 'pass'
    'integer.tg.py': '
        class Value:
            pass
        value: int = 42
    '
    'string.tg.py': 'value: str = "hello"'
}
tg tag put -p py-attributes/1.0.0 $dependency
rm --recursive $dependency
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.first]
        # specifier = "py-attributes/^1"
        # attributes = { get = "integer.tg.py" }
        # [tool.tangram.imports.alias]
        # specifier = "py-attributes/^1"
        # attributes = { get = "integer.tg.py" }
        # [tool.tangram.imports.second]
        # specifier = "py-attributes/^1"
        # attributes = { get = "string.tg.py" }
        # ///
        import first
        import alias
        import second
        number: int = first.value
        text: str = second.value
        instance: first.Value = alias.Value()
    '
}
success (tg check ($path | path join main.tg.py) | complete)
let source = open --raw ($path | path join main.tg.py)
$source | str replace 'text: str' 'text: int' | save --force ($path | path join main.tg.py)
failure (tg check ($path | path join main.tg.py) | complete)
