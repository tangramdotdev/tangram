use ../../../test.nu *

# tg.Value.print renders an error whose diagnostic has no location without crashing.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let error = tg.error.sync("boom", {
				diagnostics: [{ message: "diagmsg", severity: "error" }],
			});
			let output = tg.Value.print(error);
			return output.includes("diagmsg") && output.includes(`"diagnostics":`);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
