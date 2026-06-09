use ../../test.nu *

# Updating with a pattern argument bumps only the named dependency.

let server = spawn

let a1 = artifact { tangram.ts: 'export default () => "a1";' }
tg tag a/1.0.0 $a1
let b1 = artifact { tangram.ts: 'export default () => "b1";' }
tg tag b/1.0.0 $b1

let root = artifact {
	tangram.ts: '
		import a from "a/^1";
		import b from "b/^1";
	'
}
tg checkin $root

# Tag new versions of both dependencies.
let a2 = artifact { tangram.ts: 'export default () => "a2";' }
tg tag a/1.1.0 $a2
let b2 = artifact { tangram.ts: 'export default () => "b2";' }
tg tag b/1.1.0 $b2
tg index

let output = do --env { cd $root; tg update . a } | complete
success $output
snapshot ($output.stdout | str trim) '
	↑ updated a/1.0.0 to a/1.1.0, required by tangram.ts
'

let lock = open ($root | path join tangram.lock)
assert ($lock | str contains "a/1.1.0") "the named dependency should be updated"
assert ($lock | str contains "b/1.0.0") "the other dependency should keep its version"
assert (not ($lock | str contains "b/1.1.0")) "the other dependency should not be updated"
