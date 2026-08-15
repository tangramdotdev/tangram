use ../../test.nu *

# Incremental backtracking uses candidates for the conflicted dependency rather than candidates saved for another dependency.

let server = spawn

for version in [1.0.0 2.0.0] {
	let path = artifact { tangram.ts: $'// c ($version)' }
	tg tag -p $'c/($version)' $path
}
for version in [1.0.0 2.0.0] {
	let path = artifact { tangram.ts: $'// d ($version)' }
	tg tag -p $'d/($version)' $path
}

# Establish a watcher whose saved solution selects c/1.0.0.
let path = artifact {
	"z.tg.ts": 'import c from "c/^1";'
}
tg checkin $path --watch --no-checkout-pointers --no-lock | ignore

# Solve d before encountering a new c constraint that conflicts with the saved solution.
'import d from "d/*";' | save ($path | path join 'a.tg.ts')
'import c from "c/^2";' | save ($path | path join 'b.tg.ts')
tg watch touch $path $path

# The c constraints are incompatible, so the checkin must fail rather than resolving c/^2 with a d candidate.
let output = tg checkin $path --watch --no-checkout-pointers --no-lock | complete
failure $output "the incompatible c constraints should fail"
