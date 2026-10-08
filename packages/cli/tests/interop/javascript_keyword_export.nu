use ../lib/test.nu *

# JavaScript names a Python export that is named like a JavaScript keyword with its own name, because JavaScript writes it as a property and in a renamed import.

let local = server spawn
let path = artifact {
    'tools.tg.py': '
        def delete() -> str:
            return "delete"

        def new() -> str:
            return "new"
    '
    'tangram.ts': '
        import * as tools from "./tools.tg.py";
        import { delete as remove } from "./tools.tg.py";

        export default async () => [await tools.new(), await remove()];
    '
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | str trim) '["new","delete"]'
