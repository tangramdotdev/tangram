use ../../test.nu *

let server = spawn

let bottom_path = artifact {
	tangram.ts: '// bottom package'
}
tg tag bottom/1.0.0 $bottom_path

let left_path = artifact {
	tangram.ts: 'import * as bottom from "bottom/^1"'
}
tg tag left/1.0.0 $left_path

let right_path = artifact {
	tangram.ts: 'import * as bottom from "bottom/^1"'
}
tg tag right/2.0.0 $right_path

let root_path = artifact {
	tangram.ts: '
		import * as left from "left/^1";
		import * as right from "right/^2";
	'
}
tg tag root $root_path

let output = tg tree root --kind=package
snapshot $output '
	root: dir_01tmxzrckbg2q8rs7jgf3c3qx0466v88tbrha468ewmhstcc1yj4c0
	├╴bottom/1.0.0: dir_01dqhnepc8zbr7ehbbtqh7mr304rc7xq0n6ngqy4dy0v9p3vmac790
	├╴left/1.0.0: dir_01a42k1wbjw0mw8ntjc7m0fwfg2x035mtwbavp0vwsv5q4jxry99vg
	│ └╴bottom/1.0.0: dir_01dqhnepc8zbr7ehbbtqh7mr304rc7xq0n6ngqy4dy0v9p3vmac790
	└╴right/2.0.0: dir_01a42k1wbjw0mw8ntjc7m0fwfg2x035mtwbavp0vwsv5q4jxry99vg
'
