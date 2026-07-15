use ../../../test.nu *

# tg.Error children enumerate module objects reachable through the error's location, stack, and diagnostics, not only its source.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let range = {
				start: { line: 0, character: 0 },
				end: { line: 0, character: 0 },
			};
			let makeModule = (item) => ({ kind: "js", referent: { item, options: {} } });

			// Each file is reachable only through a distinct error field.
			let locationFile = await tg.file("location");
			let diagnosticFile = await tg.file("diagnostic");
			let stackFile = await tg.file("stack");

			let error = tg.error.sync("boom", {
				location: {
					file: { kind: "module", value: makeModule(locationFile) },
					range,
				},
				diagnostics: [
					{
						location: { module: makeModule(diagnosticFile), range },
						message: "m",
						severity: "error",
					},
				],
				stack: [
					{ file: { kind: "module", value: makeModule(stackFile) }, range },
				],
			});

			let children = await error.children;
			return (
				children.includes(locationFile) &&
				children.includes(diagnosticFile) &&
				children.includes(stackFile)
			);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
