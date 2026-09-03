use ../../test.nu *

# Checking in a process output preserves authorization for dependencies inherited from an input file.

let server = server spawn --config {
	authorization: {
		initial: { ancestor: { max_edges: 3 }, descendant: { max_edges: 3 } }
		final: { ancestor: { max_edges: 3 }, descendant: { max_edges: 3 } }
	}
}

let program = '
	export default async function () {
		const dependency = await tg.file("dependency");
		const input = await tg.file({
			contents: tg.blob("input"),
			dependencies: { [dependency.id]: dependency },
		});
		// Give checkout the input token directly so only output checkin must resolve the dependency.
		await input.store();
		const reference = tg.Referent.toDataString(
			tg.Object.toReferent(input),
			id => id,
		);
		return tg.command({
			args: ["checkout", "--dependencies=false", "--path", tg.output, reference],
			env: { INPUT: input },
			executable: "tg",
			host: tg.host.current,
		});
	}
'
let path = artifact {
	tangram.ts: $program
}

let command = tg build $path | str trim
let output = tg build $command | complete
success $output "the process should check in its authorized materialized dependency"
let object = tg get --pretty ($output.stdout | str trim)
assert ($object | str contains 'dependencies') "the output should preserve the dependency xattr"
