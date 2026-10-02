use ../lib/test.nu *

# Pulling an object by id fetches it from the remote and makes it present locally.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

let id = tg --url $remote.url put --no-tokens 'tg.file("pulled object")' | referent node

# The object is absent locally before the pull.
let before = tg object get --local $id | complete
failure $before "the object should be absent locally before the pull"

let output = tg --no-quiet pull $id | complete
success $output
assert equal ($output.stdout | referent node) $id
let tokens = $output.stdout | referent tokens local
assert not ($tokens | is-empty)
assert ($tokens | any {|token| ($token | token body | get permissions) == [object_subtree] })
for token in $tokens {
	let body = $token | token body
	if $body.permissions == [object_subtree] {
		assert equal $body.resource $id
	}
}
assert ($output.stderr | lines | any {|line| ($line starts-with $'info ($id)') and ($line | str contains 'tokens[local]') })

# The object is present locally after the pull.
let after = tg object get --local $id | complete
success $after "the object should be present locally after the pull"

# A second pull takes the local fast path and still returns a signed capability.
let local_output = tg --no-quiet pull $id | complete
success $local_output
assert equal ($local_output.stdout | referent node) $id
assert not ($local_output.stderr | str contains 'tokens[')
let tokens = $local_output.stdout | referent tokens local
assert equal ($tokens | length) 1
assert equal ($tokens.0 | token body | get permissions) [object_subtree]

let without_tokens = tg pull --no-tokens $id | complete
success $without_tokens
assert equal ($without_tokens.stdout | str trim) $id
