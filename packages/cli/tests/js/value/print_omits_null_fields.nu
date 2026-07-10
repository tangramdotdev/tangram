use ../../../test.nu *

# tg.Value.print omits absent optional error and executable fields rather than rendering them as null.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let errorPrint = tg.Value.print(tg.error("boom"));
			let errorOmitsNull =
				!errorPrint.includes(`"code":`) &&
				!errorPrint.includes(`"diagnostics":`) &&
				!errorPrint.includes(`"location":`) &&
				!errorPrint.includes(`"source":`);
			let executable = await tg.file("run");
			let command = await tg.command({
				executable: { artifact: executable },
				host: "builtin",
			});
			let commandPrint = tg.Value.print(command);
			let executableOmitsNull = !commandPrint.includes(`"path":`);
			return errorOmitsNull && executableOmitsNull;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
