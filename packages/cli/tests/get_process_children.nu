
use ../test.nu *

# The children of a build process report their named options and cached status, and named child processes can be built individually.

let server = spawn
let path = artifact {
	tangram.ts: r#'
		export function foo() { return "foo"; }
		export function bar() { return "bar"; }
		export default async function () {
			return [
				await tg.build(foo).named("foo"),
				await tg.build(bar).named("bar")
			]
		}
	'#
}
let output = tg build $"($path)#foo" | complete
success $output
snapshot $output.stdout '
	"foo"

'

let process = tg build --detach --verbose $path | from json
let output = tg wait $process.process
snapshot $output '{"exit":0,"output":["foo","bar"]}'

let children = tg process children $process.process | from json

let foo = $children | get 0 | to json
snapshot --normalize-ids $foo '
	{
	  "cached": true,
	  "options": {
	    "name": "foo",
	    "token": "<token>"
	  },
	  "process": "pcs_0000000000000000000000000000"
	}
'

let bar = $children | get 1 | to json
snapshot --normalize-ids $bar '
	{
	  "options": {
	    "name": "bar",
	    "token": "<token>"
	  },
	  "process": "pcs_0000000000000000000000000000"
	}
'
