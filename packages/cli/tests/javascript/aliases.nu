use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.ts': '
        export default async function () {
            const file = await tg.file("export default function () { console.log(\"javascript alias\"); }").module("typescript");
            return new tg.Module({ kind: "typescript", referent: { node: file } });
        }
    '
}
let module = tg run $path
for language in [javascript js] {
    let output = tg $language --export default $module | complete
    success $output
    assert equal ($output.stdout | str trim) 'javascript alias'
}
