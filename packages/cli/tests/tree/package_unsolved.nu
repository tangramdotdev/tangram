use ../../test.nu *

let server = spawn

let existing_path = artifact {
	tangram.ts: '// existing package'
}
tg tag existing/1.0.0 $existing_path

let root_path = artifact {
	tangram.ts: '
		import * as existing from "existing/^1";
		import * as missing from "missing/^1";
	'
}

let id = tg checkin --unsolved-dependencies $root_path
tg tag root $id

let output = tg tree root --kind=package
snapshot $output '
	root: dir_01vrn2rp9ps99rkhwey1ar780rhjjjq36g969vzxchya4tn1mektn0
	├╴existing/1.0.0: dir_01z93ad7h8392ahr5r8759xx19tqwtm0ss4x7vtf7ypb5yg9cvn7n0
	└╴missing/%5E1: null
'
