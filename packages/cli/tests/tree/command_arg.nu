use ../../test.nu *

let server = spawn

let root = artifact {
	tangram.ts: r#'
		import { run } from "./a.tg.ts";
		export const target = () => "hello";
		export default async () => {
			return await tg.build(run, target);
		};
	'#
	"a.tg.ts": r#'
		export const run = async (f) => {
			return await tg.build(f);
		};
	'#
}

let process = tg build -dv $root | from json | get process
tg wait $process | ignore

let tree = (tg view --mode inline --expand-processes $process | ansi strip)
print $tree

snapshot $tree '
	✓ fil_01731x2q4mzw95rx8497dn61a8qfm6asqcnqrf2re4mf2w8v2r65a0#default
	├╴output: "hello"
	├╴command: cmd_015v744pd44cb2kbfjc1t65yk5ap54cc1hzx2qs28fbvh8j9hbcad0
	└╴✓ a.tg.ts#run
	  ├╴output: "hello"
	  ├╴command: cmd_011y00x9tt24h2amtj1n1v5wdxvt1w52k830eee75894664ra34kjg
	  └╴✓ fil_01731x2q4mzw95rx8497dn61a8qfm6asqcnqrf2re4mf2w8v2r65a0#target
	    ├╴output: "hello"
	    └╴command: cmd_01w9d9n93x61s5xgdjy7msen7zj2zget09xftz6bp7za7ga3tey3s0
'
