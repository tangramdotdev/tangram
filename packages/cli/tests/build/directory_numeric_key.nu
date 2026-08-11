use ../../test.nu *

# A directory can be created with a numeric string key and produces the expected directory identifier.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return tg.directory({
				"0": "hello",
			});
		}
	'
}

# Build.
let output = tg build $path
snapshot $output 'dir_01yxf9ewzxy8jra954962jp96j0qrkg4hnp1h3pj6j6wvr5cjy3dc0'
