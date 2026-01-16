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
	root: dir_018myabm912k0ppwwbj5zr1rg6fz9ycz4pvjvkptn821rzxadggm40
	├╴left/1.0.0: dir_01vgq1hg1r5gtvf212kksncca926nvwf30khm2pnxzj9ptn5s7f270
	│ └╴bottom/1.0.0: dir_01sensxg2gzbz1fm34htcnxv0qv657bbg02w23ammhh0mzytbpvyrg
	└╴right/2.0.0: dir_01vgq1hg1r5gtvf212kksncca926nvwf30khm2pnxzj9ptn5s7f270
'
