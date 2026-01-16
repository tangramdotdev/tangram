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
	root: dir_01w3wjyrh9ncmatjdx49jgvzge5j4a19wrkat31nhjx29geekxx4eg
	├╴existing/1.0.0: dir_01e033mjqybjapgqkfhrythpgm45wqy8d2pyb85syfw77yzxxvgwag
	└╴missing/^1: null
'
