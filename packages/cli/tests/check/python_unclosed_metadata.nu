use ../lib/test.nu *

let local = server spawn
for source in ["# /// script\n# ///\n#" "# /// script\n# ///\n#\npass"] {
    let path = artifact {'main.tg.py': $source}
    let output = tg check ($path | path join main.tg.py) | complete
    success $output
    snapshot --normalize --redact $path $output.stderr '
        warning the script metadata block is not closed and will be ignored
           ╭─[./main.tg.py:1:1]
         1 │ # /// script
           · ──────┬─────
           ·       ╰── the script metadata block is not closed and will be ignored
         2 │ # ///
           ╰────

    '
}

let path = artifact {'main.tg.py': "# /// script\n# ///\n\n#"}
let output = tg check ($path | path join main.tg.py) | complete
success $output
snapshot $output.stderr ''
