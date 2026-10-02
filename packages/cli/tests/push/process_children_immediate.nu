use ../lib/test.nu *

# A push makes the authoritative process children list visible before its final index batch runs.

let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
}
let local_source = server spawn --name local-source --config {
	remotes: { default: { url: $remote.url } },
}

let path = artifact {
	tangram.ts: 'export default () => "hello"',
}
let process = tg --url $local_source.url build --no-tokens --detach $path | referent node
tg --url $local_source.url wait $process
let source_process = tg --url $local_source.url process get --local $process | from json
assert equal $source_process.children [] "the source leaf process should have an empty children list"

# Hold the asynchronous final index batch so only the awaited sync store write is visible.
tg --url $remote.url index
let watch = (
	tg --url $remote.url checkpoint watch index.batch
	| from json
	| get watch
)
tg --url $local_source.url push $process
tg --url $remote.url checkpoint wait index.batch $watch 0 | ignore

let output = tg --url $remote.url process get --local $process | complete
tg --url $remote.url checkpoint continue index.batch $watch 0
tg --url $remote.url checkpoint unwatch index.batch $watch
success $output
let remote_process = $output.stdout | from json
assert equal $remote_process.children [] "the pushed leaf process should immediately have an empty children list"
