use ../../test.nu *

# Bug: When a transitive dependency is updated, publishing a top-level package
# should automatically republish intermediate dependencies in topological order.

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# 1. Create a monorepo with packages A, B, C where A -> B -> C using local dependencies.
let root = artifact {
	packages: {
		c: {
			tangram.ts: '
				export let metadata = { tag: "c/0" };
			'
		}
		b: {
			tangram.ts: '
				import c from "c" with { local: "../c" };
				export let metadata = { tag: "b/0" };
			'
		}
		a: {
			tangram.ts: '
				import b from "b" with { local: "../b" };
				export let metadata = { tag: "a/0" };
			'
		}
	}
}

# 2. Publish A (which should publish C, B, A in topological order).
tg publish ($root | path join "packages/a") 
let a_v1 = tg tag get a/0 | from json | get item

# 3. Update C by modifying its content.
"// v2\nexport let metadata = { tag: \"c/0\" };" | save --force ($root | path join "packages/c/tangram.ts")

# 4. Republish A. This should republish C with new content, then B with new C, then A with new B.
tg publish ($root | path join "packages/a")
let a_v2 = tg tag get a/0 | from json | get item

# 5. A should have a new ID because its transitive dependency C changed.
assert ($a_v1 != $a_v2) $"A should have new ID after C updated. Before: ($a_v1), after: ($a_v2)"
