use ../../test.nu *

# Documenting a module file path directly produces documentation for its exports.

let server = spawn

let dir = mktemp --directory
'export let x = 1;' | save ($dir | path join tangram.ts)

let output = tg document ($dir | path join tangram.ts) | complete
success $output

let json = $output.stdout | from json
assert ("x" in ($json.exports | columns)) "the documentation should include the export"
