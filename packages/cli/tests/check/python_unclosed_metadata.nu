use ../lib/test.nu *

# A comment line directly after the closing `# ///` leaves the script block unclosed, so the block is ignored and the check reports only the unresolved import, with no diagnostic about the block.

let local = server spawn
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.helper]
        # specifier = "./helper.tg.py"
        # ///
        # This comment makes the block unclosed.
        import helper
    '
    'closed.tg.py': '
        # /// script
        # [tool.tangram.imports.helper]
        # specifier = "./helper.tg.py"
        # ///

        # This comment follows a blank line, so the block is closed.
        import helper
    '
    'helper.tg.py': 'value = 1'
}
success (tg check ($path | path join closed.tg.py) | complete)
let output = tg check ($path | path join main.tg.py) | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error Cannot resolve imported module `helper`
	info: Searched in the following paths during module resolution:
	info:   1. /library (extra search path specified on the CLI or in your config file)
	info:   2. vendored://stdlib (stdlib typeshed stubs vendored by ty)
	info: make sure your Python environment is properly configured: https://docs.astral.sh/ty/modules/#python-environment
	   ╭─[./main.tg.py:6:8]
	 5 │ # This comment makes the block unclosed.
	 6 │ import helper
	   ·        ───┬──
	   ·           ╰─┤ Cannot resolve imported module `helper`
	   ·             │ info: Searched in the following paths during module resolution:
	   ·             │ info:   1. /library (extra search path specified on the CLI or in your config file)
	   ·             │ info:   2. vendored://stdlib (stdlib typeshed stubs vendored by ty)
	   ·             │ info: make sure your Python environment is properly configured: https://docs.astral.sh/ty/modules/#python-environment
	   ╰────
	error an error occurred
	-> type checking failed

'
