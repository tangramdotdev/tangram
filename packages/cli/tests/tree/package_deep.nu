use ../../test.nu *

let server = spawn

let leaf_path = artifact {
	tangram.ts: '// leaf package'
}
tg tag leaf/1.0.0 $leaf_path

let middle_path = artifact {
	tangram.ts: 'import * as leaf from "leaf/^1"'
}
tg tag middle/2.0.0 $middle_path

let root_path = artifact {
	tangram.ts: 'import * as middle from "middle/^2"'
}
tg tag root $root_path

let output = tg tree root --kind=package
snapshot $output '
	root: dir_01ewzwcrbyx77ymgv6jkeewqmc2jb4yqctqm9r6jtyyptpa11037c0
	└╴middle/2.0.0: dir_019petez9rt6y5tkvh8zw21mre43zbtbsm64p1pf1935k2m9ye9x20
	  └╴leaf/1.0.0: dir_01ekh5fjdrn8d0b0qhtka34p23d3gab0qr932z4fjrmj65kgya6xp0
'
