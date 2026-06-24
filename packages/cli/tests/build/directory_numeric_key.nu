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
snapshot $output 'dir_017yd51z6sgrxvdnaf7hsjhg8186p707y4536v1b0js196cxhs58w0'
