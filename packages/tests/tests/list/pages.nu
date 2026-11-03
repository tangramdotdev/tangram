use ../../util/test.nu *

# tg list pages from the key of the last item, in version order, with each subtree together and each specifier listed once across locations.

let remote = server spawn --name remote
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }

let local_file = tg --url $local.url put --no-tokens 'tg.file("local")' | referent node
let remote_file = tg --url $remote.url put --no-tokens 'tg.file("remote")' | referent node

# Sibling versions sort by value.
tg --url $local.url group create versions
for version in [10 2 1] {
	tg --url $local.url tag put $"versions/($version)" $local_file
}
let specifiers = tg --url $local.url list --local --all versions | from json | get specifier
assert equal $specifiers [versions/1 versions/2 versions/10]

# A cursor continues after its item when an earlier item is deleted.
tg --url $local.url group create letters
for letter in [a b c] {
	tg --url $local.url tag put $"letters/($letter)" $local_file
}
let first = tg --url $local.url list --local --limit 2 --verbose letters | from json
tg --url $local.url tag delete letters/a
let second = tg --url $local.url list --local --cursor $first.cursor letters | from json | get specifier
assert equal $second [letters/c] "deleting an earlier item should not skip a later one"

# A recursive list keeps each subtree together.
tg --url $local.url group create a
tg --url $local.url tag put a/c $local_file
tg --url $local.url tag put ab $local_file
let specifiers = tg --url $local.url list --local --all --recursive | from json | get specifier | where { |specifier| $specifier in [a a/c ab] }
assert equal $specifiers [a a/c ab]

# A list across locations pages through each specifier once.
tg --url $local.url tag put shared $local_file
tg --url $remote.url tag put shared $remote_file
tg --url $remote.url tag put zeta $remote_file
mut specifiers: list<string> = []
mut cursor: any = null
loop {
	let cursor_args = if $cursor == null { [] } else { [--cursor $cursor] }
	let page = tg --url $local.url list --verbose --location 'local,remote:default' --limit 1 ...$cursor_args | from json
	$specifiers = $specifiers | append ($page.data | get specifier)
	$cursor = $page.cursor? | default null
	if $cursor == null {
		break
	}
}
assert equal ($specifiers | length) ($specifiers | uniq | length) "no specifier should be listed twice"
assert ("shared" in $specifiers) "the shared specifier should be listed"
assert ("zeta" in $specifiers) "the remote specifier should be listed"
