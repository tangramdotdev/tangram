use ../../test.nu *

# When --build is set, `tg run` should build the target first to produce an object, then run that object.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export default () => {
			return tg.directory({
				"tangram.ts": tg.file('export default () => "from built module";'),
			});
		};
	'#
}

let sandbox_output = tg run --build $path --sandbox
snapshot $sandbox_output '"from built module"'

let output = tg run --build $path
assert equal $output $sandbox_output
