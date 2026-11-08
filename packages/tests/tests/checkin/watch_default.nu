use ../../util/test.nu *

# Checkins watch ordinary sources by default and honor explicit watch preferences.

let local = server spawn
let cases = [
	{ args: [], watch: true }
	{ args: [--watch], watch: true }
	{ args: [--watch=true], watch: true }
	{ args: [--watch=false], watch: false }
	{ args: [--no-watch], watch: false }
	{ args: [--no-watch=false], watch: true }
	{ args: [--watch --no-watch], watch: false }
	{ args: [--no-watch --watch], watch: true }
]

for case in $cases {
	let path = artifact { file: 'one' }
	let before = tg checkin --no-tokens --no-lock --no-checkout-pointers ...$case.args $path | referent node
	'two' | save --force ($path | path join file)
	let touch = tg watch touch $path ($path | path join file) | complete
	if $case.watch {
		success $touch 'a watched source should accept an explicit file change notification'
	} else {
		failure $touch 'an unwatched source should not accept a file change notification'
	}
	let after = tg checkin --no-tokens --no-lock --no-checkout-pointers ...$case.args $path | referent node
	assert ($before != $after) 'the checkin should reflect the edited source'
	assert equal (tg read $'($after)?get=file') 'two'
}

# Implicit path checkins use the same default.
let path = artifact { file: 'before' }
let before = tg get --depth=0 --no-tokens $path | referent node
'after' | save --force ($path | path join file)
success (tg watch touch $path ($path | path join file) | complete)
let after = tg get --depth=0 --no-tokens $path | referent node
assert ($before != $after) 'implicit checkin should reflect the edited source'
assert equal (tg read $'($after)?get=file') 'after'
