use ../../test.nu *

# Pulling a process includes its error by default.

let remote = spawn --cloud --name remote
let source = spawn --name source --config {
	remotes: { default: { url: $remote.url } },
}
let local = spawn --name local
tg remote put default $remote.url

let path = artifact {
	tangram.ts: r#'
		export default function () {
			throw tg.error.sync("whoops");
		}
	'#
}
let process = tg --url $source.url build --detach $path | str trim
tg --url $source.url wait $process
tg --url $source.url push $process
tg --url $remote.url wait $process
let error = tg --url $remote.url get $process | from json | get error

tg pull $process

let local_error = tg object get --local $error | complete
success $local_error "the process error should be present locally after the pull"
