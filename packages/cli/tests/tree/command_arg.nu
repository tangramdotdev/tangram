use ../lib/test.nu *

let server = server spawn

let root = artifact {
	tangram.ts: r#'
		import { run } from "./a.tg.ts";
		export function target() { return "hello"; }
		export default async function () {
			return await tg.build(run, target);
		}
	'#
	"a.tg.ts": r#'
		export async function run(f) {
			return await tg.build(f);
		}
	'#
}

let process = tg build -dv $root | from json | get process
let output = tg wait $process 
snapshot $output '{"exit":0,"output":"hello"}'
let tree = tg view --mode inline --expand-processes $process | ansi strip | str replace --all --regex 'host: "[^"]+"' 'host: "<host>"'

snapshot --normalize-ids $tree '
	✓ fil_010000000000000000000000000000000000000000000000000000#default
	├╴output: "hello"
	├╴command: map
	│ ├╴args: array
	│ │ ├╴map
	│ │ │ ├╴kind: "string"
	│ │ │ └╴value: "js"
	│ │ ├╴map
	│ │ │ ├╴kind: "string"
	│ │ │ └╴value: "--export"
	│ │ ├╴map
	│ │ │ ├╴kind: "string"
	│ │ │ └╴value: "default"
	│ │ └╴map
	│ │   ├╴kind: "value"
	│ │   └╴value: map
	│ │     ├╴kind: "module"
	│ │     └╴value: map
	│ │       ├╴kind: "ts"
	│ │       └╴referent: map
	│ │         ├╴node: "fil_010000000000000000000000000000000000000000000000000000"
	│ │         └╴options: map
	│ │           ├╴location: "local"
	│ │           └╴tokens: map
	│ │             └╴local: array
	│ │               └╴"<token>"
	│ ├╴executable: map
	│ │ ├╴node: map
	│ │ │ └╴path: "tg"
	│ │ └╴options: map
	│ │   ├╴location: "local"
	│ │   └╴tokens: map
	│ │     └╴local: array
	│ │       └╴"<token>"
	│ └╴host: "<host>"
	└╴✓ a.tg.ts#run
	  ├╴output: "hello"
	  ├╴command: map
	  │ ├╴args: array
	  │ │ ├╴map
	  │ │ │ ├╴kind: "string"
	  │ │ │ └╴value: "js"
	  │ │ ├╴map
	  │ │ │ ├╴kind: "string"
	  │ │ │ └╴value: "--export"
	  │ │ ├╴map
	  │ │ │ ├╴kind: "string"
	  │ │ │ └╴value: "run"
	  │ │ ├╴map
	  │ │ │ ├╴kind: "value"
	  │ │ │ └╴value: map
	  │ │ │   ├╴kind: "module"
	  │ │ │   └╴value: map
	  │ │ │     ├╴kind: "ts"
	  │ │ │     └╴referent: map
	  │ │ │       ├╴node: "fil_011111111111111111111111111111111111111111111111111111"
	  │ │ │       └╴options: map
	  │ │ │         ├╴location: "local"
	  │ │ │         └╴tokens: map
	  │ │ │           └╴local: array
	  │ │ │             ├╴"<token>"
	  │ │ │             └╴"<token>"
	  │ │ ├╴map
	  │ │ │ ├╴kind: "string"
	  │ │ │ └╴value: "-A"
	  │ │ └╴map
	  │ │   ├╴kind: "value"
	  │ │   └╴value: map
	  │ │     ├╴kind: "object"
	  │ │     └╴value: "cmd_010000000000000000000000000000000000000000000000000000?location=local&tokens[local][0]=<token>&tokens[local][1]=<token>"
	  │ ├╴executable: map
	  │ │ ├╴node: map
	  │ │ │ └╴path: "tg"
	  │ │ └╴options: map
	  │ │   ├╴location: "local"
	  │ │   └╴tokens: map
	  │ │     └╴local: array
	  │ │       ├╴"<token>"
	  │ │       └╴"<token>"
	  │ └╴host: "<host>"
	  └╴✓ fil_010000000000000000000000000000000000000000000000000000#target
	    ├╴output: "hello"
	    └╴command: map
	      ├╴args: array
	      │ ├╴map
	      │ │ ├╴kind: "string"
	      │ │ └╴value: "js"
	      │ ├╴map
	      │ │ ├╴kind: "string"
	      │ │ └╴value: "--export"
	      │ ├╴map
	      │ │ ├╴kind: "string"
	      │ │ └╴value: "target"
	      │ └╴map
	      │   ├╴kind: "value"
	      │   └╴value: map
	      │     ├╴kind: "module"
	      │     └╴value: map
	      │       ├╴kind: "ts"
	      │       └╴referent: map
	      │         ├╴node: "fil_010000000000000000000000000000000000000000000000000000"
	      │         └╴options: map
	      │           ├╴location: "local"
	      │           └╴tokens: map
	      │             └╴local: array
	      │               ├╴"<token>"
	      │               ├╴"<token>"
	      │               └╴"<token>"
	      ├╴executable: map
	      │ ├╴node: map
	      │ │ └╴path: "tg"
	      │ └╴options: map
	      │   ├╴location: "local"
	      │   └╴tokens: map
	      │     └╴local: array
	      │       ├╴"<token>"
	      │       ├╴"<token>"
	      │       └╴"<token>"
	      └╴host: "<host>"
'
