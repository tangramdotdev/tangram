use ../../test.nu *

# Two builds that depend on the same child reuse the cached child process, even when the modules
# form an import cycle.
#
# This is `cache_reuse_shared_child.nu` with one addition: a second module that imports the root,
# making the two modules a cycle. A cycle cannot be content addressed node by node, so the whole
# strongly-connected component becomes a single graph object and each module referent turns into a
# node index into it. The child command is assembled from literals and names no module, so making
# the parent a graph node must not change which process the child resolves to.

let server = spawn

let path = artifact {
	tangram.ts: '
		import * as b from "./b.tg.ts";
		function inner() { return tg.build({
			args: ["-c", "true"],
			executable: "sh",
			host: "not-a-real-host",
		}).named("inner"); }

		export async function first() {
			await inner();
			return b.value;
		}
		export async function second() {
			await inner();
			return "second";
		}
	',
	"b.tg.ts": '
		import * as root from "./tangram.ts";
		export const value = "first";
	',
}

let first = tg build --detach --verbose $"($path)#first" | from json
tg wait $first.process | complete
let first_shared = tg process children $first.process | from json | get 0.process

let second = tg build --detach --verbose $"($path)#second" | from json
tg wait $second.process | complete
let second_shared = tg process children $second.process | from json | get 0.process

let first_shared_id = $first_shared | split row '?' | first
let second_shared_id = $second_shared | split row '?' | first

# Compare the commands first, so a failure says whether the child command itself changed or whether
# an identical command failed to resolve to the cached process.
let first_command = tg process get $first_shared_id | from json | get command
let second_command = tg process get $second_shared_id | from json | get command
assert equal $first_command $second_command "the shared child's command should be identical"
assert equal $first_shared_id $second_shared_id "the shared child should be reused across the two builds"
