use ../lib/test.nu *

let remote = server spawn --name remote
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }
let local_ids = 1..2 | each { tg --url $local.url sandbox create --no-tokens --local | referent node }
let remote_ids = 1..2 | each { tg --url $remote.url sandbox create --no-tokens --local | referent node }
tg --url $local.url index
tg --url $remote.url index
let expected = $local_ids | append $remote_ids | sort
let all = tg --url $local.url sandbox list --all --limit 1 --verbose | from json
assert equal $all.data.id $expected
assert equal ($all | get --optional cursor) null
let first = tg --url $local.url sandbox list --limit 1 --verbose | from json
assert equal $first.data.id ($expected | first 1)
let remaining = tg --url $local.url sandbox list --cursor $first.cursor --limit 2 --all | from json
assert equal $remaining.id ($expected | skip 1)
let flat = tg --no-quiet --url $local.url sandbox list --limit 1 | complete
success $flat
assert equal ($flat.stdout | from json | get id) ($expected | first 1)
assert ($flat.stderr | str contains $first.cursor) "the info message should include the cursor"
let scoped = tg --url $local.url sandbox list --local --all --limit 1 | from json
assert equal $scoped.id ($local_ids | sort)
for cursor in ["!" "eyJ2ZXJzaW9uIjoiVjEifQ"] {
	failure (tg --url $local.url sandbox list --cursor $cursor | complete)
}
for limit in ["0" "1001"] {
	failure (tg --url $local.url sandbox list --limit $limit | complete)
}
for id in $local_ids { tg --url $local.url sandbox destroy $id }
for id in $remote_ids { tg --url $remote.url sandbox destroy $id }
