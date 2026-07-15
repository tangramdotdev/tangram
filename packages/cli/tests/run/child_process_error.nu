use ../../test.nu *

# An error thrown in a child process spawned via tg.run propagates to the parent, causing the run to fail with the expected diagnostic on stderr.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(foo);
		}

		export function foo() {
			throw tg.error.sync("error");
		}
	',
}

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr
let stderr = $stderr | redact | normalize_ids
snapshot $stderr
