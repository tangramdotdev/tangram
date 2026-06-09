use ../../test.nu *

# Updating a package with nothing to update prints nothing and leaves the lockfile unchanged.

let server = spawn

let dep = artifact { tangram.ts: 'export default () => "dep";' }
tg tag dep/1.0.0 $dep

let root = artifact {
	tangram.ts: 'import dep from "dep/^1";'
}
tg checkin $root
let before = open ($root | path join tangram.lock)

let output = tg update $root | complete
success $output
assert equal $output.stdout "" "the update should print nothing"
assert equal (open ($root | path join tangram.lock)) $before "the lockfile should be unchanged"
