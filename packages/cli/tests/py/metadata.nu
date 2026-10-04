use ../lib/test.nu *

let local = server spawn
for contents in [
    '# requires-python = "<3"'
    '# dependencies = ["requests"]'
    '# [tool.tangram.imports.bad]
# specifier = 1'
    '# [tool.tangram.imports.bad]
# specifier = "tools"
# attributes = { get = true }'
] {
    let source = '# /// script' + (char nl) + $contents + (char nl) + '# ///' + (char nl)
    let path = artifact {'main.tg.py': $source}
    let output = tg checkin ($path | path join main.tg.py) | complete
    failure $output
    assert ($output.stderr | str contains 'main.tg.py:')
    let output = tg py ($path | path join main.tg.py) | complete
    failure $output
    assert ($output.stderr | str contains 'main.tg.py')
}

let path = artifact {
    'main.tg.py': 'text = """
# /// script
# dependencies = ["ignored"]
# ///
"""
print("string contents ignored")
'
}
let output = tg py ($path | path join main.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) 'string contents ignored'
