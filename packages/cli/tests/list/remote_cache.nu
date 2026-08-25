use ../../test.nu *

# The remote cache stores filtered list responses and honors TTL overrides.

skip_if_no_tokens

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let node = tg --url $remote.url put 'tg.file("contents")' | str trim
tg --url $remote.url tag -p foo/a $node

let initial = tg --url $local.url list --no-groups foo | from json
assert equal ($initial | get specifier) [foo/a]
assert (($initial.0 | get --optional node.options.tokens.remote) != null) "remote list should return a token"

tg --url $remote.url tag foo/b $node

let cached = tg --url $local.url list --no-groups foo | from json
assert equal ($cached | get specifier) [foo/a]
assert (($cached.0 | get --optional node.options.tokens.remote) != null) "the cached response should preserve the token"

let fresh = tg --url $local.url list --no-groups --ttl 0 foo | from json
assert equal ($fresh | get specifier) [foo/a foo/b]
