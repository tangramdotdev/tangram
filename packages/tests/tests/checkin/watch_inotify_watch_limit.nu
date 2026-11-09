use ../../util/test.nu *

# A watched checkin reflects an on-disk edit even when the server has no inotify watches left, such as when other watches have exhausted the user's limit.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --inotify-watches 0

# Create a package.
let path = artifact {
	tangram.ts: 'export default () => "one";',
}

# Check in the package with a watch.
let first = tg checkin --no-tokens $path --watch | referent node

# Edit the package's root module.
'export default () => "two";' | save --force ($path | path join tangram.ts)

# A checkin without a watch reflects the edit.
let plain = tg checkin --no-tokens $path --no-watch | referent node
assert ($plain != $first) "the edit should change the checked in package"

# A watched checkin reflects the edit.
wait_until { (tg checkin --no-tokens $path --watch | referent node) == $plain } "the watched checkin should reflect the on-disk edit"
