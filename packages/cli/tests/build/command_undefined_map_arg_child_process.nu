use ../../test.nu *

# A command argument map with an undefined entry must round-trip into a child process without crashing when the process deserializes its arguments.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export const inner = (arg: { a?: string }) => {
			return "ok";
		};
		export default async () => {
			return await tg.build(inner, { a: undefined });
		};
	'#
}

success (tg build $path | complete) "a map arg with an undefined entry must round-trip into a child process"
