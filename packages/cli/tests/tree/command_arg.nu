use ../../test.nu *

let server = spawn

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
let tree = (tg view --mode inline --expand-processes $process | ansi strip | normalize_ids)

snapshot $tree '
	✓ fil_010000000000000000000000000000000000000000000000000000#default
	├╴output: "hello"
	├╴command: cmd_010000000000000000000000000000000000000000000000000000
	└╴✓ a.tg.ts#run
	  ├╴output: "hello"
	  ├╴command: cmd_011111111111111111111111111111111111111111111111111111
	  └╴✓ fil_010000000000000000000000000000000000000000000000000000#target
	    ├╴output: "hello"
	    └╴command: cmd_012222222222222222222222222222222222222222222222222222
'
