use ../lib/test.nu *

# The check command warns about renamed and omitted exports throughout the dependency graph.

let local = server spawn
let path = artifact {
    'tangram.ts': '
        import * as tools from "./tools.tg.ts";
        export default tools.assert;
    '
    'tools.tg.ts': '
        export const assert = () => "assert";
        export const $ = () => "dollar";
        export const __all__ = () => "reserved";
        export const match = () => "match";
        export default () => "default";
    '
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.tools]
        # specifier = "./tools.tg.ts"
        # ///
        from tools import assert_

        async def default():
            return await assert_()
    '
}
let output = tg check $path | complete
success $output
snapshot --normalize --redact $path $output.stderr '
	warning python cannot bind the export $: the export name is not a python identifier
	   ╭─[<redacted>/tools.tg.ts:2:14]
	 1 │ export const assert = () => "assert";
	 2 │ export const $ = () => "dollar";
	   ·              ┬
	   ·              ╰── python cannot bind the export $: the export name is not a python identifier
	 3 │ export const __all__ = () => "reserved";
	   ╰────
	warning python cannot bind the export __all__: the export name __all__ is reserved by the python loader
	   ╭─[<redacted>/tools.tg.ts:3:14]
	 2 │ export const $ = () => "dollar";
	 3 │ export const __all__ = () => "reserved";
	   ·              ───┬───
	   ·                 ╰── python cannot bind the export __all__: the export name __all__ is reserved by the python loader
	 4 │ export const match = () => "match";
	   ╰────
	warning python names the export assert as assert_
	   ╭─[<redacted>/tools.tg.ts:1:14]
	 1 │ export const assert = () => "assert";
	   ·              ───┬──
	   ·                 ╰── python names the export assert as assert_
	 2 │ export const $ = () => "dollar";
	   ╰────

'

# A Python root reports warnings from its foreign dependencies as well.
let file = $path | path join main.tg.py
let output = tg check $file | complete
success $output
snapshot --normalize --redact $path $output.stderr '
	warning python cannot bind the export $: the export name is not a python identifier
	   ╭─[./tools.tg.ts:2:14]
	 1 │ export const assert = () => "assert";
	 2 │ export const $ = () => "dollar";
	   ·              ┬
	   ·              ╰── python cannot bind the export $: the export name is not a python identifier
	 3 │ export const __all__ = () => "reserved";
	   ╰────
	warning python cannot bind the export __all__: the export name __all__ is reserved by the python loader
	   ╭─[./tools.tg.ts:3:14]
	 2 │ export const $ = () => "dollar";
	 3 │ export const __all__ = () => "reserved";
	   ·              ───┬───
	   ·                 ╰── python cannot bind the export __all__: the export name __all__ is reserved by the python loader
	 4 │ export const match = () => "match";
	   ╰────
	warning python names the export assert as assert_
	   ╭─[./tools.tg.ts:1:14]
	 1 │ export const assert = () => "assert";
	   ·              ───┬──
	   ·                 ╰── python names the export assert as assert_
	 2 │ export const $ = () => "dollar";
	   ╰────

'

# Roots in both languages that reach the same module report its warnings once.
let output = tg check $path $file | complete
success $output
snapshot --normalize --redact $path $output.stderr '
	warning python cannot bind the export $: the export name is not a python identifier
	   ╭─[./tools.tg.ts:2:14]
	 1 │ export const assert = () => "assert";
	 2 │ export const $ = () => "dollar";
	   ·              ┬
	   ·              ╰── python cannot bind the export $: the export name is not a python identifier
	 3 │ export const __all__ = () => "reserved";
	   ╰────
	warning python cannot bind the export __all__: the export name __all__ is reserved by the python loader
	   ╭─[./tools.tg.ts:3:14]
	 2 │ export const $ = () => "dollar";
	 3 │ export const __all__ = () => "reserved";
	   ·              ───┬───
	   ·                 ╰── python cannot bind the export __all__: the export name __all__ is reserved by the python loader
	 4 │ export const match = () => "match";
	   ╰────
	warning python names the export assert as assert_
	   ╭─[./tools.tg.ts:1:14]
	 1 │ export const assert = () => "assert";
	   ·              ───┬──
	   ·                 ╰── python names the export assert as assert_
	 2 │ export const $ = () => "dollar";
	   ╰────

'
