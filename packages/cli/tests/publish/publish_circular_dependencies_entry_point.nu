use ../../test.nu *

# Bug: Publishing from different entry points in a cycle produces different IDs.

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a cycle: A -> B -> C -> A
let root = artifact {
	packages: {
		a: {
			tangram.ts: '
				import b from "b" with { local: "../b" };
				export let metadata = { tag: "a/0" };
			'
		}
		b: {
			tangram.ts: '
				import c from "c" with { local: "../c" };
				export let metadata = { tag: "b/0" };
			'
		}
		c: {
			tangram.ts: '
				import a from "a" with { local: "../a" };
				export let metadata = { tag: "c/0" };
			'
		}
	}
}

# Publish from A.
tg publish ($root | path join "packages/a")
let a_from_a = tg tag get a/0 | from json | get item

# Publish from B (without deleting tags).
tg publish ($root | path join "packages/b")
let a_from_b = tg tag get a/0 | from json | get item

# A should have the same ID regardless of entry point.
assert ($a_from_a == $a_from_b) $"A has different IDs when published from A vs B. From A: ($a_from_a), from B: ($a_from_b)"
