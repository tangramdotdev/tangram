use ../../util/test.nu *

# A tag get of a missing tag answers while the indexer's index queue is behind.

let root_token = random chars
let local = server spawn --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	indexer: { id: 'idx_0000000000000000000000000000' },
}

# Hold the indexer's index queue.
let watch = tg --token $root_token checkpoint watch index.write | from json | get watch

# Put an object, whose index write stays in the indexer's index queue while the queue is held.
tg --token $root_token put --no-tokens 'tg.file("hello")' | ignore
let output = tg --token $root_token checkpoint wait --timeout 30s index.write $watch 0 | complete
success $output "the index write must reach the held index queue"

# Get a tag that does not exist while the index write is pending.
let output = timeout 10s tg --token $root_token tag get missing | complete
assert ($output.exit_code != 124) "the tag get must not wait for the indexer's index queue"
failure $output "the tag get should fail when the tag does not exist"
assert ($output.stderr | str contains "failed to find the tag") $output.stderr

# Release the index queue.
tg --token $root_token checkpoint continue index.write $watch 0
tg --token $root_token checkpoint unwatch index.write $watch
