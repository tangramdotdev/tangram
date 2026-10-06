use ../lib/test.nu *

let local = server spawn
for source in [
    'import missing_dependency'
    'def broken('
    'from .. import outside'
] {
    let path = artifact {'main.tg.py': $source}
    let output = tg check ($path | path join main.tg.py) | complete
    failure $output
    assert ($output.stderr | str contains 'main.tg.py:1:')
}
