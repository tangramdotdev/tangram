use ../lib/test.nu *

# A process may check a module from its sandbox.

let local = server spawn --config {compiler: {check_backend: tsrs, typescript_executable: /__missing_tsrs_test__/tsc}}

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
