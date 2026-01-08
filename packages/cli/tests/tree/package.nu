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
	root: dir_019hj0yzc2k2aheg8714gcwatt3f0v9adse1gbe4fczkmhdebgdhkg
	├╴bar: dir_01kabb91xy8q9mhn2w11n6vqzvfveqsxm2e89gxhk9n51f84rrv040
	│ └╴foo: dir_01t8nppn4cd2e6myjy4dk9t5z01gp2sz6rk8k48npyqdv55g38nn70
	└╴foo: dir_01t8nppn4cd2e6myjy4dk9t5z01gp2sz6rk8k48npyqdv55g38nn70
'
