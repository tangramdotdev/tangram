use ../lib/test.nu *

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let created = 1..5 | each { tg --token $alice.token user token create | from json }

# Follow individual pages and compare them with the collecting method.
let first_output = tg --no-quiet --token $alice.token user token list --limit 2 --verbose | complete
success $first_output
assert equal $first_output.stderr ""
let first = $first_output.stdout | from json
assert equal ($first.data | length) 2
assert (($first.cursor | str length) > 0) "the first page should have a cursor"
let second = tg --no-quiet --token $alice.token user token list --limit 2 --cursor $first.cursor --verbose | from json
let third = tg --no-quiet --token $alice.token user token list --limit 2 --cursor $second.cursor --verbose | from json
assert equal ($second.data | length) 2
assert equal ($third.data | length) 2
assert equal ($third | get --optional cursor) null
let expected = $first.data | append $second.data | append $third.data
assert equal $expected.id ($expected.id | sort | uniq)
assert not ("token" in ($expected | columns)) "listing should not expose token secrets"

let all_output = tg --no-quiet --token $alice.token user token list --all --limit 2 --verbose | complete
success $all_output
assert equal $all_output.stderr ""
let all = $all_output.stdout | from json
assert equal $all.data $expected
assert equal ($all | get --optional cursor) null
let remaining = tg --no-quiet --token $alice.token user token list --all --limit 1 --cursor $first.cursor --verbose | from json
assert equal $remaining.data ($expected | skip 2)
assert equal ($remaining | get --optional cursor) null

# Print flat data and report a continuation using the normal info mechanism.
let output = tg --no-quiet --token $alice.token user token list --limit 2 | complete
success $output
assert equal ($output.stdout | from json) $first.data
assert ($output.stderr | str contains $first.cursor) "the info message should include the cursor"
assert ($output.stderr | str contains "info") "the cursor should use the info printer"
let output = tg --no-quiet --token $alice.token user token list --limit 2 --cursor $second.cursor | complete
success $output
assert equal $output.stderr ""
assert equal ($output.stdout | from json) $third.data
let output = tg --no-quiet --token $alice.token user token list --all --limit 2 | complete
success $output
assert equal $output.stderr ""
assert equal ($output.stdout | from json) $expected

# Reject invalid limits, malformed encodings, and unknown cursor versions.
for limit in ["0" "1001" "18446744073709551615"] {
	failure (tg --no-quiet --token $alice.token user token list --limit ($limit | into string) | complete)
}
for cursor in ["!" "bm90IGpzb24" "eyJ2ZXJzaW9uIjoiVjEifQ"] {
	failure (tg --no-quiet --token $alice.token user token list --cursor $cursor | complete)
}

# A cursor cannot bypass the authenticated user's scope.
let bob = tg login --verbose --name bob | from json
let bob_tokens = tg --token $bob.token user token list --cursor $first.cursor --all --limit 1 | from json
assert ($bob_tokens | all {|token| $token.id not-in $expected.id }) "the cursor should not expose another user's tokens"

# Continuation survives deletion of the row that established its position.
let boundary = $first.data.1.id
let authentication = $created | where id != $boundary | first | get token
tg --token $authentication user token delete $boundary
let remaining = tg --token $authentication user token list --cursor $first.cursor --all --limit 1 --verbose | from json
assert equal $remaining.data ($expected | skip 2)
