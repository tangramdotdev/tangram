use ../lib/test.nu *

# Internal remote discovery follows every page even though the CLI defaults to one page.
let empty = server spawn --name empty
let target = server spawn --name target
let remotes = 1..100 | each {|index| { name: $'remote-($index)', value: { url: $empty.url } } } | transpose --header-row --as-record
let remotes = $remotes | insert zeta { url: $target.url }
let local = server spawn --name local --config { remotes: $remotes }
let expected = tg --url $target.url group create found | from json
let first = tg --url $local.url remote list --verbose | from json
assert equal ($first.data | length) 100
assert (($first.cursor | str length) > 0) "the default page should have a cursor"
let all = tg --url $local.url remote list --all --limit 17 | from json
assert equal ($all | length) 101
let output = tg --url $local.url get found | from json
assert equal $output.id $expected.id

# Forwarding retains the opaque cursor and page size.
let members = [alpha beta gamma] | each {|name| tg --url $target.url group create $name | from json }
for member in $members {
	tg --url $target.url group members add found $member.id
}
let first = tg --url $local.url group members list found --remote=zeta --limit 1 --verbose | from json
assert equal ($first.data | length) 1
let remaining = tg --url $local.url group members list found --remote=zeta --limit 1 --cursor $first.cursor --all | from json
assert equal ($first.data | append $remaining) ($members.id | sort)
