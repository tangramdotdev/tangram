use ../../test.nu *

# A blob received before a file that uses it is published through a default checkout and reused for the file.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	remotes: { default: { url: $remote.url } },
}

let contents = 'shared command input and executable contents'
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg --url $remote.url put $blob_value | str trim
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg --url $remote.url put $file_value | str trim
let default_file_value = ['tg.file({"contents":' $blob '})'] | str join
let default_file = tg --url $remote.url put $default_file_value | str trim
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
let command = tg --url $remote.url put $command_value | str trim
let file_watch = (
	tg --url $local.url checkpoint watch sync.get.input.object --params ({ id: $file } | to json)
	| from json
	| get watch
)

let pull = job spawn {
	let job_id = job id
	let output = tg --url $local.url pull $command | complete
	$output | job send --tag $job_id 0
}

tg --url $local.url checkpoint wait sync.get.input.object $file_watch 0 | ignore

let default_path = $local.checkout_directory | path join $default_file
let file_path = $local.checkout_directory | path join $file
wait_until {
	$default_path | path exists
} 'expected the blob to be published through its default checkout before the file arrived'
tg --url $local.url checkpoint continue sync.get.input.object $file_watch 0
tg --url $local.url checkpoint unwatch sync.get.input.object $file_watch

success (job recv --tag $pull --timeout 10sec)
assert equal (open --raw $default_path) $contents
assert equal (open --raw $file_path) $contents

server stop $remote
rm $default_path
rm $file_path
let output = tg --url $local.url get --bytes $blob | complete
failure $output 'expected the leaf bytes to require the default checkout'
