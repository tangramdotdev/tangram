use ../../test.nu *

# Reproduces a hang where a parent JS process never receives notification
# that a child system process finished. Matches the pattern from the std
# build: default -> tg.build(inner) -> inner calls tg.run with network/sandbox
# which creates a separate sandbox with ttl=0 for the child.

let server = spawn

let path = artifact {
	tangram.ts: '
		export const inner = async (...args) => {
			let id = args[0]?.id ?? "0";
			return tg.run`echo "done ${id}" > ${tg.output}`.network(true);
		};
		export default async () => {
			return Promise.all(
				Array.from({ length: 4 }, (_, i) => tg.build(inner, { id: String(i) }))
			);
		};
	'
}

let output = tg build $path | complete
success $output

