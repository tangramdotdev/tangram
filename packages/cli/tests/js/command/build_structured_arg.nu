use ../../../test.nu *

# A command created from a function delivers a structured argument to that function
# unchanged, whether it is invoked through the builder or through the awaited command.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export function report(arg) {
			return typeof arg;
		}

		export default async function () {
			let viaBuild = await tg.build(report, { a: 1 });
			let viaBuilder = await tg.command(report).build({ a: 1 });
			let viaCommand = await (await tg.command(report)).build({ a: 1 });
			return { viaBuild, viaBuilder, viaCommand };
		}
	'
}

let output = tg build $path
snapshot $output '{"viaBuild":"object","viaBuilder":"object","viaCommand":"object"}'
