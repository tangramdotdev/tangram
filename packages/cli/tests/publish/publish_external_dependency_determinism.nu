use ../../test.nu *

# Test that graph IDs are deterministic when a package with cycles is entered via an external dependency.
#
# This reproduces the bug where publishing std directly vs via jq produced different IDs.
# The root cause was external pointers (references to nodes outside the SCC) using ephemeral
# checkin graph indices instead of resolved object IDs.

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a package "inner" where nodes in a cycle reference nodes outside the cycle.
# - a.tg.ts and b.tg.ts form a cycle AND both reference helper.tg.ts
# - helper.tg.ts is outside the cycle
# This means a-b SCC has external pointers to helper.
let root = artifact {
	packages: {
		inner: {
			"a.tg.ts": '
				import b from "./b.tg.ts";
				import helper from "./helper.tg.ts";
				export default {};
			'
			"b.tg.ts": '
				import a from "./a.tg.ts";
				import helper from "./helper.tg.ts";
				export default {};
			'
			"helper.tg.ts": '
				export default {};
			'
			tangram.ts: '
				import a from "./a.tg.ts";
				import b from "./b.tg.ts";
				export let metadata = { tag: "inner/0" };
			'
		}
		outer: {
			tangram.ts: '
				import inner from "inner" with { local: "../inner" };
				export let metadata = { tag: "outer/0" };
			'
		}
	}
}

# Publish inner directly (like publishing std).
tg publish ($root | path join "packages/inner")
let inner_from_inner = tg tag get inner/0 | from json | get item

# Publish outer which depends on inner (like publishing jq which depends on std).
tg publish ($root | path join "packages/outer")
let inner_from_outer = tg tag get inner/0 | from json | get item

# Inner should have the same ID regardless of entry point.
assert ($inner_from_inner == $inner_from_outer) $"inner has different IDs when published directly vs via outer. Direct: ($inner_from_inner), via outer: ($inner_from_outer)"
