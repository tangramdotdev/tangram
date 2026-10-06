use ../lib/test.nu *

# A sandbox can read an artifact returned by a remote process cache lookup without network access.

let remote = server spawn --name remote
let remote_primary = server spawn --name remote-primary
tg remote put default $remote.url

let shared = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("shared result");
		}
	'
}

let process = tg build --no-tokens --detach $shared | referent node
tg wait $process
tg push --eager --process-output-objects $process

let wrapper_typescript = [
	$'import shared from "shared" with { source: "($shared)" };'
	'export default async function () {'
	'	const file = await tg.build(shared).then(tg.File.expect);'
	'	return file.text;'
	'}'
] | str join "\n"
let wrapper = artifact { tangram.ts: $wrapper_typescript }

let local_fresh = server spawn --name local-fresh
tg remote put default $remote.url

let output = tg build $wrapper | from json
assert equal $output "shared result"
