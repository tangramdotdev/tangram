use ../lib/test.nu *

let local = server spawn
for source in ["# /// script\n# ///\n#" "# /// script\npass"] {
    let path = artifact {'main.tg.py': $source}
    let output = tg check ($path | path join main.tg.py) | complete
    success $output
    assert ($output.stderr | str contains 'warning the script metadata block is not closed and will be ignored')
    assert ($output.stderr | str contains 'main.tg.py:1:1')
}

let path = artifact {'main.tg.py': "# /// script\n# ///\n\n#"}
let output = tg check ($path | path join main.tg.py) | complete
success $output
assert equal $output.stderr ''
