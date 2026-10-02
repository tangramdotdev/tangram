use ../lib/test.nu *

# Every listing uses the same page, collection, and CLI output contract.
def check_pages [args: list<string>] {
	let expected = tg ...$args --limit 1000 --verbose | from json
	assert (($expected.data | length) > 1) "the fixture should span multiple pages"
	let first_output = tg --no-quiet ...$args --limit 1 --verbose | complete
	success $first_output
	assert equal $first_output.stderr ""
	let first = $first_output.stdout | from json
	assert equal $first.data ($expected.data | first 1)
	mut page = $first
	mut data = $page.data
	while ($page | get --optional cursor) != null {
		$page = ^tg ...$args --limit 1 --cursor $page.cursor --verbose | from json
		$data = $data | append $page.data
		assert (($data | length) <= ($expected.data | length)) "pagination should terminate"
	}
	assert equal $data $expected.data
	let all = tg --no-quiet ...$args --all --limit 1 --verbose | complete
	success $all
	assert equal $all.stderr ""
	assert equal ($all.stdout | from json) $expected
	let remaining = tg ...$args --all --limit 1 --cursor $first.cursor | from json
	assert equal $remaining ($expected.data | skip 1)
	let flat = tg --no-quiet ...$args --limit 1 | complete
	success $flat
	assert equal ($flat.stdout | from json) $first.data
	assert ($flat.stderr | str contains $first.cursor) "the info message should include the cursor"
	let all = tg --no-quiet ...$args --all --limit 1 | complete
	success $all
	assert equal $all.stderr ""
	assert equal ($all.stdout | from json) $expected.data
	for limit in ["0" "1001"] {
		failure (tg ...$args --limit $limit | complete)
	}
	for cursor in ["!" "bm90IGpzb24" "eyJ2ZXJzaW9uIjoiVjEifQ"] {
		failure (tg ...$args --cursor $cursor | complete)
	}
}

let local = server spawn --config {
	authentication: { root: { token: "root-token" }, users: { providers: { insecure: true } } },
	remotes: { "": { url: "http://localhost:9999" } },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json

let runners = 1..3 | each { tg --token $alice.token runner create --owner $alice.user.id | from json }
tg --token "root-token" runner create
check_pages [--token $alice.token runner list]
check_pages [--token "root-token" runner list --all-owners]
let first = tg --token "root-token" runner list --all-owners --limit 1 --verbose | from json
let scoped = tg --token $alice.token runner list --cursor $first.cursor --all | from json
assert ($scoped | all { $in.owner == $alice.user.id }) "a cursor should preserve owner scoping"

let runner = $runners.0.data.id
1..3 | each { tg --token $alice.token runner token create $runner } | ignore
check_pages [--token $alice.token runner token list $runner]

for kind in [group organization] {
	tg --token $alice.token $kind create $kind
	for member in [$bob.user.id $carol.user.id] {
		tg --token $alice.token $kind members add $kind $member
	}
	check_pages [--token $alice.token $kind members list $kind]
}

# Composite grant positions must include the creator to preserve tied subjects and resources.
tg --token $alice.token grant $carol.user.id admin group
tg --token $alice.token grant $bob.user.id read group
tg --token $carol.token grant $bob.user.id write group
check_pages [--token $alice.token grants list --resource group]
check_pages [--token $bob.token grants list --subject $bob.user.id]

for name in [zeta alpha beta] {
	tg --token "root-token" remote put $name http://localhost:9999
}
check_pages [--token "root-token" remote list]
let remotes = tg --token "root-token" remote list --all | from json
assert equal $remotes.name ["" alpha beta zeta]

let paths = 1..3 | each {
	let path = artifact { tangram.ts: 'export default () => 42;' }
	tg --token $alice.token checkin --no-tokens $path --watch | ignore
	$path
}
check_pages [--token $alice.token watch list]
let watches = tg --token $alice.token watch list --all | from json
assert equal $watches.path ($paths | sort)
let first = tg --token $alice.token watch list --limit 1 --verbose | from json
tg --token $alice.token watch delete $first.data.0.path
let remaining = tg --token $alice.token watch list --cursor $first.cursor --all --limit 1 | from json
assert equal $remaining.path ($paths | sort | skip 1)
let other = tg --token $bob.token watch list --cursor $first.cursor --all | from json
assert equal $other []
