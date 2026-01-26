use ../../test.nu *

# Bug: Checking in from different entry points in a cycle produces different graph node orderings,
# which results in different IDs for the same packages.

let local = spawn -n local

# Create a cycle: A -> B -> C -> A
let root = artifact {
	packages: {
		a: {
			tangram.ts: '
				import b from "b" with { local: "../b" };
				export default "a";
			'
		}
		b: {
			tangram.ts: '
				import c from "c" with { local: "../c" };
				export default "b";
			'
		}
		c: {
			tangram.ts: '
				import a from "a" with { local: "../a" };
				export default "c";
			'
		}
	}
}

# Check in from A.
let a_from_a = tg checkin ($root | path join "packages/a")

# Check in from B.
let b_from_b = tg checkin ($root | path join "packages/b")

# Get the graph ID for each using tg children.
let a_graph = tg children $a_from_a | from json | get 0
let b_graph = tg children $b_from_b | from json | get 0

# The graphs should be the same since the cycle content is identical.
assert ($a_graph == $b_graph) $"Graphs differ based on entry point. From A: ($a_graph), from B: ($b_graph)"
