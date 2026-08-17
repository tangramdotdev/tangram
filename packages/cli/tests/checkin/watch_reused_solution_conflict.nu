use ../../test.nu *

# An incremental solve handles a conflict after reusing a saved solution without panicking.

let server = spawn

let c1_path = artifact {
	tangram.ts: '// c 1.0.0'
}
tg tag -p c/1.0.0 $c1_path

let c2_path = artifact {
	tangram.ts: '// c 2.0.0'
}
tg tag -p c/2.0.0 $c2_path

# Establish a watcher whose saved solution selects c/1.0.0.
let path = artifact {
	"z.tg.ts": 'import c from "c/^1";'
}
tg checkin $path --watch --no-checkout-pointers --no-lock --unsolved-dependencies | ignore

# Add a compatible reference that reuses c/1.0.0 followed by an incompatible reference.
'import c from "c/*";' | save ($path | path join 'a.tg.ts')
'import c from "c/^2";' | save ($path | path join 'b.tg.ts')
tg watch touch $path $path

# The conflict is allowed to remain unsolved, but the incremental solver must not panic while attempting to backtrack through the reused solution.
let output = tg checkin $path --watch --no-checkout-pointers --no-lock --unsolved-dependencies | complete
success $output
