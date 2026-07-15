use ../../../test.nu *

# tg.error.sync populates the code and values getters from an argument object.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let error = tg.error.sync("boom", { code: "E1", values: { a: "b" } });
			return { message: await error.message, code: await error.code, values: await error.values };
		}
	'
}

let output = tg build $path
snapshot $output '{"code":"E1","message":"boom","values":{"a":"b"}}'
