use ../../util/test.nu *

# A tag get of a missing tag answers while usage updates are pending.

let root_token = random chars
let local = server spawn --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	usage: true,
}
tg login --name alice | ignore

# Hold the usage updates.
let usage = tg --token $root_token checkpoint watch indexer.update.usage.batch | from json | get watch
tg --token $root_token checkpoint wait --timeout 30s indexer.update.usage.batch $usage 0 | ignore

# Tag an object in alice's namespace, and wait for the capture of the tag's permissions, which enqueues a usage update for alice's account.
let object = tg put --no-tokens 'tg.file("hello")' | referent node
let params = { resource: $object } | to json --raw
let written = tg --token $root_token checkpoint watch permission_capture.written --params $params | from json | get watch
tg tag put alice/hello $object
tg --token $root_token checkpoint wait --timeout 30s permission_capture.written $written 0 | ignore
tg --token $root_token checkpoint continue permission_capture.written $written 0
tg --token $root_token checkpoint unwatch permission_capture.written $written

# Get a tag that does not exist while the usage update is pending.
let output = timeout 10s tg tag get missing | complete
assert ($output.exit_code != 124) "the tag get must not wait for the pending usage updates"
failure $output "the tag get should fail when the tag does not exist"

# Release the usage updates.
tg --token $root_token checkpoint continue indexer.update.usage.batch $usage 0
tg --token $root_token checkpoint unwatch indexer.update.usage.batch $usage
