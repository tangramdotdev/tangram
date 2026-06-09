use ../../test.nu *

# Touching a watch injects a synthetic file system event so a subsequent checkin reflects the modified file.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "one";'
}
let before = tg checkin $path --watch

'export default () => "two";' | save --force ($path | path join tangram.ts)
let output = tg watch touch $path ($path | path join tangram.ts) | complete
success $output

let after = tg checkin $path --watch
assert ($before != $after) "the checkin should produce a new id"
let object = tg get $after --blobs --depth=inf --pretty
assert ($object | str contains "two") "the checkin should reflect the modified contents"
