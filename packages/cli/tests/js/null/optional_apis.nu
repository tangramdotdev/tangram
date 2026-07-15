use ../../../test.nu *

# Public APIs accept null for optional values and interpret it as None.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ file: "contents" });
			let archive = await tg.archive(directory, "tar", null);
			let prefix = await tg.Mutation.prefix("prefix", null);
			let suffix = await tg.Mutation.suffix("suffix", null);
			let error = tg.error.sync({ values: null });
			let printed = tg.Value.print(null, { color: null });
			return [
				archive instanceof tg.Blob,
				prefix.inner.separator === null,
				suffix.inner.separator === null,
				Object.keys(await error.values).length === 0,
				printed === "null",
			];
		}
	'
}

let output = tg build $path
snapshot $output '[true,true,true,true,true]'
