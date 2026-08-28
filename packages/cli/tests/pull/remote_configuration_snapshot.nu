use ../../test.nu *

# A pull uses the remote session it resolved even if the remote is replaced while the pull is in progress.

let source = server spawn --name source
let replacement = server spawn --name replacement
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
}

let object = tg --url $source.url put 'tg.file("contents")' | str trim
tg --url $local.url remote put default $source.url --trusted

let watch = (
	tg --url $local.url checkpoint watch push.source.remote.resolved
	| from json
	| get watch
)
let pull = job spawn {
	let job_id = job id
	let output = tg --url $local.url pull $object | complete
	$output | job send --tag $job_id 0
}

let output = timeout 30s tg --url $local.url checkpoint wait push.source.remote.resolved $watch 0 | complete
success $output "the pull should resolve the source remote"

tg --url $local.url remote put default $replacement.url
tg --url $local.url checkpoint continue push.source.remote.resolved $watch 0
tg --url $local.url checkpoint unwatch push.source.remote.resolved $watch

let output = try { job recv --tag $pull --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the pull did not complete" }
}
success $output "the pull should use the original remote session"

let output = tg --url $local.url object get --bytes --local $object | complete
success $output "the object should be stored locally"
