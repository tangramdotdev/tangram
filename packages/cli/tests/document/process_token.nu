use ../../test.nu *

# A process may document a module from its sandbox.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			const source = tg.directory({
				"tangram.ts": tg.file(`export default function () { return "x"; }`),
			});
			return tg.run`tg document ${source} > "$TANGRAM_OUTPUT"`
				.sandbox()
				.then(tg.File.expect);
		}
	'
}

success (tg build $path | complete)
