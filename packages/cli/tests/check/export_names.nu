use ../lib/test.nu *

# The check command warns about renamed and unsupported exports throughout the dependency graph.

let local = server spawn
let path = artifact {
    'tangram.ts': '
        import * as tools from "./tools.tg.ts";
        export default tools.assert;
    '
    'tools.tg.ts': '
        import "./tangram.ts";
        export const assert = () => "assert";
        export const assert_ = () => "assert_";
        export const assert__ = () => "assert__";
        export const $ = () => "dollar";
        export const __all__ = () => "reserved";
        export const match = () => "match";
        export default () => "default";
    '
}
let output = tg check $path | complete
success $output
assert equal ($output.stderr | lines | where { $in | str contains 'warning python names the export assert as assert___' } | length) 1
assert ($output.stderr | str contains 'python cannot bind the export $') $output.stderr
assert ($output.stderr | str contains 'python cannot bind the export __all__') $output.stderr
assert not ($output.stderr | str contains 'export match') $output.stderr
assert not ($output.stderr | str contains 'export default') $output.stderr

# A Python root reports warnings from its foreign dependencies as well.
let path = artifact {
    'tools.tg.ts': 'export const assert = () => "assert";'
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
let output = tg check ($path | path join main.tg.py) | complete
success $output
assert ($output.stderr | str contains 'python names the export assert as assert_') $output.stderr
