
use ../test.nu *
let server = spawn
let path = artifact {
	tangram.ts: r#'
		export const foo = () => "foo";
		export const bar = () => "bar";
		export default async () => {
			return [
				await tg.build(foo).named("foo"),
				await tg.build(bar).named("bar")
			]
		};
	'#
}
let output = tg build $"($path)#foo" | complete
success $output
snapshot $output.stdout '
	"foo"

'

let process = tg build -dv $path | from json
let output = tg wait $process.process
snapshot $output '{"exit":0,"output":["foo","bar"]}'

let children = tg process children $process.process | from json

let foo = $children | get 0 | update process 'PROCESS'
snapshot $foo '
	cached: true
	options: name: foo
	process: PROCESS

'

let bar = $children | get 1 | update process 'PROCESS'
snapshot $bar '
	options: name: bar
	process: PROCESS

'
