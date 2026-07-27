use ../../test.nu *

# A builtin's sandbox must not be enqueued with the command's host, "builtin", because a runner
# advertises `tg::host::current()`, which is only ever a machine host, so no runner could ever
# satisfy it and the sandbox would stay queued forever. A builtin reaches the scheduler only when the
# allocation shortcut in `try_spawn_process_task` fails, so the sleeping child occupies the spare cpu
# until the concurrent builtins are enqueued, then releases enough capacity to place them.

let server = spawn --config { runner: { cpus: 2 } }

let path = artifact {
	tangram.ts: '
		export async function hold() {
			await tg.sleep(2);
		}

		export default async function () {
			let artifact = await tg.directory({ "hello.txt": "contents" });
			let other = await tg.directory({ "goodbye.txt": "contents" });
			await Promise.all([
				tg.build(hold),
				tg.archive(artifact, "tar"),
				tg.archive(artifact, "zip"),
				tg.archive(other, "tar"),
			]);
		}
	'
}

tg build $path
