use ../../test.nu *

# Checking in a module that depends on a tag with an unsatisfied transitive source dependency fails, then succeeds once the transitive dependency is tagged.

let server = spawn

# Create and tag a module with a source dependency.
let path = artifact {
	a.tg.ts: '
		import "b" with { source: "./b.tg.ts" };
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
snapshot ($output.stderr | redact $path) '
	error an error occurred
	-> failed to solve dependencies
	-> no matching tags were found
	   pattern = b
	   referrer = a

'

# Tag b and retry. This should now succeed.
tg tag b $'($path)/b.tg.ts'
let output = tg checkin $'($path)/c.tg.ts' | complete
success $output "the checkin should succeed after b is tagged"
