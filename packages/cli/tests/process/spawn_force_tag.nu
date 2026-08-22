use ../../test.nu *

# Spawning with force-tag replaces an existing tag, while the default preserves it.

let server = spawn
let first_path = artifact {
	tangram.ts: 'export default async function () { return "first"; }'
}
let second_path = artifact {
	tangram.ts: 'export default async function () { return "second"; }'
}

for flag in [
	'--create-tag-ancestors'
	'--force-tag'
	'--no-create-tag-ancestors'
	'--no-pull-tag-ancestors'
	'--pull-tag-ancestors'
] {
	let output = tg process spawn --cached=false --sandbox $flag $first_path | complete
	failure $output $"($flag) should require --tag"
	assert ($output.stderr | str contains '--tag')
}

let first = tg process spawn --cached=false --create-tag-ancestors --sandbox --tag spawned/process $first_path | str trim
assert equal (tg tag get spawned/process | from json | get target.id) $first
success (tg group get spawned | complete) "create-tag-ancestors should create the parent group"

let output = tg process spawn --cached=false --sandbox --tag spawned/process $second_path | complete
failure $output "spawning should not replace an existing tag without force-tag"
assert equal (tg tag get spawned/process | from json | get target.id) $first

let second = tg process spawn --cached=false --force-tag --sandbox --tag spawned/process $second_path | str trim
assert not equal $first $second
assert equal (tg tag get spawned/process | from json | get target.id) $second
