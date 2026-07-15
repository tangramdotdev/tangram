use ../../../test.nu *

# tg.Value.print renders an error whose source is another error, recursing into the nested error without crashing.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let error = tg.error.sync("outer", {
				source: { item: tg.error.sync("inner"), options: {} },
			});
			let output = tg.Value.print(error);
			return output.includes(`"source":`) && output.includes("inner");
		}
	'
}

let output = tg build $path
snapshot $output 'true'
