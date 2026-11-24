use ../../util/test.nu *

# A pull by a caller with no access to an object does not keep the object alive past its ttl.

let root_token = random chars
let local = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	indexer: { cleaning: {} },
	object: { ttl: 10, ttt: 0 },
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice stores an object, and a second later a sentinel whose removal shows that the cleaner has processed the object.
let id = tg --token $alice.token put --no-tokens 'tg.file("private")' | referent node
advance_time $local 1sec
let sentinel = tg --token $alice.token put --no-tokens 'tg.file("sentinel")' | referent node
tg --token $root_token index
let delete = checkpoint watch cleaning.object.delete --params { object: $sentinel } --token $root_token

# Eve pulls the object by its id regularly, which fails because she has no access to it.
for _ in 1..2 {
	advance_time $local 4sec
	failure (tg --token $eve.token pull $id | complete) "the pull should be unauthorized"
	tg --token $root_token index
}

# Advance the clock past the ttl of both objects, and wait for the cleaner to remove the sentinel.
advance_time $local 4sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# The object is removed, so Alice can no longer get it.
failure (tg --token $alice.token object get $id | complete) "the object should be removed"
