use ../../test.nu *

# Solving intersecting tag patterns backtracks from the newest incompatible candidate to an older compatible candidate.

let server = server spawn

let c1_path = artifact {
	tangram.ts: '// c 1.0.0'
}
tg tag -p c/1.0.0 $c1_path

let c2_path = artifact {
	tangram.ts: '// c 2.0.0'
}
tg tag -p c/2.0.0 $c2_path

let path = artifact {
	"a.tg.ts": 'import c from "c/*";'
	"b.tg.ts": 'import c from "c/^1";'
}

# The first pattern initially selects c/2.0.0, then the second pattern requires backtracking to c/1.0.0.
let id = tg checkin $path
let object = tg get $id --blobs --depth=inf --pretty
assert ($object | str contains '"tag": "c/1.0.0"') "the solver should select the jointly compatible version"
assert not ($object | str contains '"tag": "c/2.0.0"') "the solver should reject the incompatible newest version"
