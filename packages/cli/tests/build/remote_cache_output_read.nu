use ../../test.nu *

# A sandbox can read an artifact returned by a remote process cache lookup without network access.

let remote = server spawn --name remote
let primary = server spawn --name primary
tg remote put default $remote.url

let shared = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("shared result");
		}
	'
}

let process = tg build --detach $shared | str trim
tg wait $process
tg push --eager --process-outputs $process

let wrapper_ts = [
	$'import shared from "shared" with { source: "($shared)" };'
	'export default async function () {'
	'	const file = await tg.build(shared).then(tg.File.expect);'
	'	return file.text;'
	'}'
] | str join "\n"
let wrapper = artifact { tangram.ts: $wrapper_ts }

let fresh = server spawn --name fresh
tg remote put default $remote.url

let output = tg build $wrapper | from json
assert equal $output "shared result"
