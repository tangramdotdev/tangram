use ../../test.nu *

# When --executable-path is set, `tg run` should resolve the executable from the directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.directory({
			"main.tg.ts": tg.file("export default () => \"from executable path\";"),
		});
	'
}

let sandbox_output = tg run --executable-path main.tg.ts --build $path
snapshot $sandbox_output '"from executable path"'
