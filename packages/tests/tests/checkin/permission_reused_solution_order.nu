use ../../util/test.nu *

# Reusing a tag solution lets a trusted referrer make the reference grant another user read access to the dependency, independent of traversal order.

let remote = server spawn --name remote --config {
	remotes: {}
}

# Create a dependency with 1,025 entries, one more than fit in a directory leaf, so that it is a branch directory.
let entries = 0..<1_025 | each { |i| { name: ($i | into string), contents: 'dependency' } } | transpose --header-row --as-record
let dependency_path = artifact $entries
let dependency = tg --url $remote.url checkin --no-tokens --no-lock --root $dependency_path | referent node
tg --url $remote.url tag -p dependency/1.0.0 $dependency

let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	remotes: {
		a: { url: $remote.url }
		z: { trusted: true, url: $remote.url }
	}
}
let eve = tg --url $local.url login --verbose --name eve | from json

let untrusted_first = artifact {
	a.tg.ts: 'import "dependency/^1?location=remote:a";'
	b.tg.ts: 'import "dependency/^1?location=remote:z";'
}
let reference = tg --url $local.url --token $root_token checkin --no-lock --root $untrusted_first | str trim
tg --url $local.url --token $root_token get --depth=inf $reference | ignore
success (tg --url $local.url --token $eve.token get --depth=inf $reference | complete) "the later trusted referrer should make the reference grant the dependency"

let trusted_first = artifact {
	a.tg.ts: 'import "dependency/^1?location=remote:z";'
	b.tg.ts: 'import "dependency/^1?location=remote:a";'
}
let reference = tg --url $local.url --token $root_token checkin --no-lock --root $trusted_first | str trim
tg --url $local.url --token $root_token get --depth=inf $reference | ignore
success (tg --url $local.url --token $eve.token get --depth=inf $reference | complete) "the earlier trusted referrer should make the reference grant the dependency"
