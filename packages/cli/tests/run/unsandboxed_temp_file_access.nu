use ../../test.nu *

# An unsandboxed process can read an arbitrary host temp file whose path is passed as an argument.

let server = spawn

let temp_path = mktemp --tmpdir tangram_temp.XXXXXX
"hello from temp file\n" | save --force $temp_path

let source = '
	export default async function (path) {
		return await tg.run`cat ${path}`;
	}
'

let path = artifact {
	tangram.ts: $source,
}

let output = tg run $path --arg-string $temp_path
snapshot $output '
	hello from temp file
'
