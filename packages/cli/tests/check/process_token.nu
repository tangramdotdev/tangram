use ../../test.nu *

# A process may check a module from its sandbox.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			const source = tg.directory({
				"tangram.ts": tg.file(`export default "x";`),
			});
			return tg.run`tg check ${source} > "$TANGRAM_OUTPUT"`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}

success (tg build $path | complete)
