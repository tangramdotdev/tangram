use ../lib/test.nu *

# A pull that receives a blob before the file that uses it leaves the blob and the file readable without the remote.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	remotes: { default: { url: $remote.url } },
}

let contents = 'shared command input and executable contents'
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg --url $remote.url put --no-tokens $blob_value | referent node
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg --url $remote.url put --no-tokens $file_value | referent node
let default_file_value = ['tg.file({"contents":' $blob '})'] | str join
let default_file = tg --url $remote.url put --no-tokens $default_file_value | referent node
let command_value = (
	[
		'tg.command({"env":{"BLOB":{"kind":"value","value":'
		$blob
		'}},"executable":{"artifact":'
		$file
		'},"host":"aarch64-darwin"})'
	]
	| str join
)
let command = tg --url $remote.url put --no-tokens $command_value | referent node
let file_watch = (
	tg --url $local.url checkpoint watch sync.get.input.object --params ({ id: $file } | to json)
	| from json
	| get watch
)
let publish_watch = (
	tg --url $local.url checkpoint watch sync.get.checkout.publish --params ({ artifact: $default_file } | to json)
	| from json
	| get watch
)

let pull = job spawn {
	let job_id = job id
	let output = tg --url $local.url pull $command | complete
	$output | job send --tag $job_id 0
}

tg --url $local.url checkpoint wait sync.get.input.object $file_watch 0 | ignore

# Publish the blob's default checkout before the file arrives.
tg --url $local.url checkpoint wait sync.get.checkout.publish $publish_watch 0 | ignore
tg --url $local.url checkpoint continue sync.get.checkout.publish $publish_watch 0
tg --url $local.url checkpoint unwatch sync.get.checkout.publish $publish_watch
tg --url $local.url checkpoint continue sync.get.input.object $file_watch 0
tg --url $local.url checkpoint unwatch sync.get.input.object $file_watch

success (job recv --tag $pull --timeout 10sec)

# The blob and the file are readable without the remote.
server stop $remote
assert equal (tg --url $local.url read $blob) $contents
assert equal (tg --url $local.url read $file) $contents
