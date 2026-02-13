use ../../test.nu *

let server = spawn

# Create and tag a module with a local dependency.
let path = artifact {
	a.tg.ts: '
		import "b" with { local: "./b.tg.ts" };
	'
	b.tg.ts: ''
	c.tg.ts: '
		import "a";
	'
}
tg tag a $'($path)/a.tg.ts'

# Check in a module that depends on a. This should fail because b is not tagged.
let output = tg checkin $'($path)/c.tg.ts' | complete
failure $output "the checkin should fail before b is tagged"
assert ($output.stderr | str contains "no matching tags were found")
assert ($output.stderr | str contains "pattern = b")

# Tag b and retry. This should now succeed.
tg tag b $'($path)/b.tg.ts'
let output = tg checkin $'($path)/c.tg.ts' | complete
success $output "the checkin should succeed after b is tagged"
