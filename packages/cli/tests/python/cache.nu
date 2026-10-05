use ../lib/test.nu *

let local = server spawn
let package = artifact {
    'tangram.py': 'base = 40'
    'task.tg.py': '
        from . import base
        from .helper import delta
        def default():
            return base + delta
    '
    'helper.tg.py': 'delta = 2'
    'unused.tg.py': 'unused = 1'
    'README.md': 'first revision'
}
let task = $package | path join task.tg.py
let first = tg build --detach --verbose $task | from json
tg wait $first.process
let first_id = $first.process | split row '?' | first
let first_file = tg checkin $task | split row '?' | first

# A file command captures its initializer and imports, not the whole directory.
'next revision' | save --force ($package | path join README.md)
'unused = 2' | save --force ($package | path join unused.tg.py)
let second = tg build --detach --verbose $task | from json
tg wait $second.process
assert equal ($second.process | split row '?' | first) $first_id
assert equal (tg checkin $task | split row '?' | first) $first_file

# Required imports and initializers remain real dependencies.
'delta = 3' | save --force ($package | path join helper.tg.py)
let third = tg build --detach --verbose $task | from json
tg wait $third.process
assert (($third.process | split row '?' | first) != $first_id)
assert ((tg checkin $task | split row '?' | first) != $first_file)
'base = 50' | save --force ($package | path join tangram.py)
assert equal (tg build $task | str trim) '53'

# Checked-in files run and check after the source directory is removed.
let checked = tg checkin $task
rm --recursive $package
success (tg check $checked | complete)
assert equal (tg run $checked | str trim) '53'

# An absolute import of the same file does not change its package placement.
let package = artifact {
    'tangram.py': 'value = 42'
    'task.tg.py': ''
}
let task = $package | path join task.tg.py
('
# /// script
# [tool.tangram.imports.myself]
# specifier = ' + ($task | to json --raw) + '
# ///
from . import value
import myself
def default():
    assert myself.value == value
    return value
') | save --force $task
let checked = tg checkin $task
rm --recursive $package
success (tg check $checked | complete)
assert equal (tg run $checked | str trim) '42'
