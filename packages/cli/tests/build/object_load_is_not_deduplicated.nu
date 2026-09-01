use ../../test.nu *

# Concurrent loads through one handle should share an in-flight object GET.

let server = server spawn --config {
	tracing: {
		filter: 'tangram=info,tangram_http::layer::tracing=trace'
		stderr_format: 'json'
	}
}

let path = artifact {
	tangram.ts: '
		export default async () => {
			const concurrency = 8;
			const directory = await tg.directory({ file: "contents" });
			const id = await directory.store();
			const handle = tg.Directory.withId(id);
			await Promise.all(Array.from({ length: concurrency }, () => handle.load()));
			return id;
		};
	'
}

let id = tg build $path | from json
server stop $server

# The tracing layer debug formats the path, so the recorded value carries literal quotes.
let gets = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { from json }
	| where $it.fields.message? == 'request'
	| where ($it.fields.method? | default '') == 'GET'
	| each { |event| $event.fields.path? | default '' | str trim --char '"' }
	| where $it == $'/objects/($id)'
	| length

assert equal $gets 1 'concurrent loads through one handle should share one object GET'
