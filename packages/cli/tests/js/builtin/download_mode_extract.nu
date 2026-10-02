use ../../lib/test.nu *
use ../../lib/http.nu *

# tg.download with the "extract" mode unpacks the downloaded archive, returning a directory artifact.

let local = server spawn
let source = artifact { 'file.txt': 'hello, world!' }
let archive = mktemp
tar -czf $archive -C $source file.txt
let http = spawn_http_server { '/archive.tar.gz': { file: $archive } }

let module = '
	export default async function (url: string) {
		let result = await tg.download(url, undefined, { mode: "extract" });
		tg.assert(result instanceof tg.Directory);
		const file = tg.File.expect(await result.get("file.txt"));
		return await file.text;
	}
'

let path = artifact {
	tangram.ts: $module
}

let output = tg build $path $'($http.url)/archive.tar.gz' | from json
assert equal $output 'hello, world!'
