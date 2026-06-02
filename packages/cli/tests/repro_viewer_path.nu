use ../test.nu *

# The viewer renders process titles with the wrong module path: a process with a
# path-less referent inherits its tree parent's path, and process_title then
# appends a different module's export to it. Here `target` is defined in
# tangram.ts but is rendered under a.tg.ts (the module of the build that was
# handed `target` as an argument).

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

# `target` lives in tangram.ts, so it must not be rendered under a.tg.ts.
assert (not ($tree | str contains "a.tg.ts#target"))
