use ../../test.nu *

let server = spawn

# Build a target that returns a directory.
let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"greeting.txt": "Hello, World!",
				"data": tg.directory({
					"info.txt": "some data",
				}),
				"link": tg.symlink("greeting.txt"),
			});
		};
	'
}

let id = tg build $path | str trim

# Archive the build output and extract it, then verify the roundtrip.
let blob_id = tg archive --format tar --compression gz $id | str trim
let extracted_id = tg extract $blob_id | str trim
assert ($extracted_id == $id) "roundtrip failed for build output"
