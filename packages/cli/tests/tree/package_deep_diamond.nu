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
	root: dir_01t2d0esw0vg2gkrn6wesz4j211z1wz0fymw98zg8sbrt7xd3cb85g
	├╴left/1.0.0: dir_01x9mxntyz9x0z76akrgz9cwt1w87f4k7tdegv789xfc8kmgay0d4g
	│ └╴bottom/1.0.0: dir_01dqhnepc8zbr7ehbbtqh7mr304rc7xq0n6ngqy4dy0v9p3vmac790
	└╴right/2.0.0: dir_01x9mxntyz9x0z76akrgz9cwt1w87f4k7tdegv789xfc8kmgay0d4g
'
