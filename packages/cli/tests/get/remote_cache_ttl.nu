use ../../test.nu *

# Remote cache reads use the configured default TTL and honor finite and infinite overrides.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remote_cache: { time_to_live: 0 }
	remotes: { default: { url: $remote.url } }
}

let first = tg --url $remote.url put 'tg.file("first")' | str trim
let second = tg --url $remote.url put 'tg.file("second")' | str trim
let third = tg --url $remote.url put 'tg.file("third")' | str trim

tg --url $remote.url tag foo $first
assert equal (tg --url $local.url tag get foo | from json | get target.id) $first

# The configured default of zero bypasses the cache.
tg --url $remote.url tag --force foo $second
assert equal (tg --url $local.url tag get foo | from json | get target.id) $second

# A finite override accepts the cached response.
tg --url $remote.url tag --force foo $third
assert equal (tg --url $local.url tag get --ttl 1h foo | from json | get target.id) $second

# An infinite override also accepts the cached response.
assert equal (tg --url $local.url tag get --no-ttl foo | from json | get target.id) $second

# An explicit zero bypasses the cache.
assert equal (tg --url $local.url tag get --ttl 0 foo | from json | get target.id) $third
