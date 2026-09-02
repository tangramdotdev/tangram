use ../../test.nu *
use ../lib/checkin.nu checkin-output

# Reusing a tag solution observes the selected object with each referrer's location, independent of traversal order.

let remote = server spawn --name remote --config {
	checkin: {
		directory: {
			max_branch_children: 2
			max_leaf_entries: 2
		}
	}
	remotes: {}
}
let dependency_path = artifact {
	a: dependency
	b: dependency
	c: dependency
}
let dependency = tg --url $remote.url checkin --no-lock --root $dependency_path | str trim
tg --url $remote.url tag -p dependency/1.0.0 $dependency

let local = server spawn --name local --config {
	remotes: {
		a: { url: $remote.url }
		z: { trusted: true, url: $remote.url }
	}
}

let untrusted_first = artifact {
	a.tg.ts: 'import "dependency/^1?location=remote:a";'
	b.tg.ts: 'import "dependency/^1?location=remote:z";'
}
let output = checkin-output $local $untrusted_first
assert equal $output.permissions [object_subtree] "the later trusted referrer should prove subtree permission"

let trusted_first = artifact {
	a.tg.ts: 'import "dependency/^1?location=remote:z";'
	b.tg.ts: 'import "dependency/^1?location=remote:a";'
}
let output = checkin-output $local $trusted_first
assert equal $output.permissions [object_subtree] "the earlier trusted referrer should prove subtree permission"
