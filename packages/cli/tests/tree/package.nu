use ../../test.nu *

let server = spawn

let foo_path = artifact {
	tangram.ts: '// foo'
}
tg tag foo $foo_path

let bar_path = artifact {
	tangram.ts: 'import * as foo from "foo"'
}
tg tag bar $bar_path

let path = artifact {
	tangram.ts: '
		import * as foo from "foo";
		import * as bar from "bar";
	'
}

tg tag root $path
let output = tg tree root --kind=package
snapshot $output '
	root: dir_01tjvqvxqgs8sdck7j4539gnqjc8fx1sngnsmjp3e31eq74z8738h0
	├╴bar: dir_01qnktgdfmzvp8zq4nc7a7dw9p31792v4246jvz5d5k0npw5jx8m50
	│ └╴foo: dir_01w9htt4zcpbb5d40p8p2jr3gejqww5ger62rgsgyndg7z78zxf5r0
	└╴foo: dir_01w9htt4zcpbb5d40p8p2jr3gejqww5ger62rgsgyndg7z78zxf5r0
'
