use ../../../util/test.nu *

# A command builder for an export of a module runs the export with its args.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			const command = tg.Command.Builder.withModule(import.meta.module, "greet", ["World", 1]);
			return await command.build();
		};
		export const greet = (name: string, count: number) => `Hello, ${name}! ${count}`;
	'
}
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World! 1'
