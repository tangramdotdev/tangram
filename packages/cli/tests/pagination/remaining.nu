use ../lib/test.nu *

# Compare identities because authorization tokens can be refreshed between requests.
def identities [data: list, key: string] {
	$data | get $key
}

def check_pages [args: list<string>, key: string] {
	let expected = tg ...$args --limit 1000 --verbose | from json
	let first = tg ...$args --limit 17 --verbose | from json
	assert equal ($first.data | length) 17
	mut page = $first
	mut data = $page.data
	while ($page | get --optional cursor) != null {
		$page = ^tg ...$args --limit 17 --cursor $page.cursor --verbose | from json
		$data = $data | append $page.data
		assert (($data | length) <= ($expected.data | length)) "pagination should terminate"
	}
	assert equal (identities $data $key) (identities $expected.data $key)
	let all = tg ...$args --all --limit 17 --verbose | from json
	assert equal (identities $all.data $key) (identities $expected.data $key)
	assert equal ($all | get --optional cursor) null
	let remaining = tg ...$args --all --limit 23 --cursor $first.cursor --verbose | from json
	assert equal (identities $remaining.data $key) (identities ($expected.data | skip 17) $key)
	let flat = tg --no-quiet ...$args --limit 17 | complete
	success $flat
	assert equal ($flat.stdout | from json | length) 17
	assert ($flat.stderr | str contains $first.cursor) "the info message should include the cursor"
	let all = tg --no-quiet ...$args --all --limit 17 | complete
	success $all
	assert equal $all.stderr ""
	for limit in ["0" "1001"] {
		failure (tg ...$args --limit $limit | complete)
	}
	for cursor in ["!" "bm90IGpzb24" "eyJ2ZXJzaW9uIjoiVjEifQ"] {
		failure (tg ...$args --cursor $cursor | complete)
	}
}

let remote = server spawn --name remote
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }
tg --url $remote.url group create catalog | ignore
for name in (1..105 | each { into string } | append "01") {
	tg --url $remote.url group create $'catalog/($name)' | ignore
}

# A single source and a merged remote query both return all matches beyond page one.
check_pages [--url $remote.url list --local catalog] specifier
check_pages [--url $remote.url list --local --reverse catalog] specifier
check_pages [--url $local.url list --recursive] specifier
check_pages [--url $local.url match "catalog/*"] specifier
check_pages [--url $local.url match "catalog/*" --reverse] specifier
check_pages [--url $local.url children catalog] node
let first = tg --url $local.url children catalog --verbose | from json
assert equal ($first.data | length) 100
assert (($first.cursor | str length) > 0) "children should default to one page"
let children = tg --url $local.url children catalog --all | from json
assert equal ($children | length) 106

# Local entries retain precedence even when remote results span multiple pages.
let local_group = tg --url $local.url group create catalog | from json
tg --url $local.url group create catalog/01 | ignore
let output = tg --url $local.url list --recursive --all --limit 17 | from json
assert equal ($output | where specifier == catalog | get 0.node.node) $local_group.id
assert equal ($output.specifier | length) ($output.specifier | uniq | length)
let matches = tg --url $local.url match "catalog/*" --all --limit 17 | from json
assert equal ($matches | length) 106
