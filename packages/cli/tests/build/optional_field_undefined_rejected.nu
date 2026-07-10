use ../../test.nu *

# Optional fields may be omitted or set to null, but they may not be set to undefined.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const omitted: tg.Command.Arg.Object = {};
			const cleared: tg.Command.Arg.Object = { cwd: null };
			const hostArg: tg.Host.SpawnArg = {
				args: [],
				cwd: null,
				env: {},
				executable: "true",
				stderr: "null",
				stdin: "null",
				stdout: "null",
			};
			const reader = new tg.Process.Stdio.Reader({
				fd: null,
				stream: "stdout",
			});
			const writer = new tg.Process.Stdio.Writer({
				fd: null,
				stream: "stdin",
			});
			if (false) {
				// @ts-expect-error An optional field may not be set to undefined.
				const invalid: tg.Command.Arg.Object = { cwd: undefined };
				// @ts-expect-error The Rust Option field is always present as a value or null.
				const missingCwd: tg.Host.SpawnArg = {
					args: [],
					env: {},
					executable: "true",
					stderr: "null",
					stdin: "null",
					stdout: "null",
				};
				// @ts-expect-error An optional field may not be set to undefined.
				new tg.Process.Stdio.Reader({ fd: undefined, stream: "stdout" });
				await tg.archive({} as tg.Artifact, "tar", null);
				await tg.download("https://example.com", null, null);
				await tg.Mutation.prefix("value", null);
				await tg.Mutation.suffix("value", null);
				await tg.Process.prepareUnsandboxedCommand(
					{} as tg.Process.Spawn.Arg,
					null,
				);
				tg.Value.print(null, { color: null });
				tg.error({ values: null });
				void invalid;
				void missingCwd;
			}
			return (
				omitted.cwd === undefined &&
				cleared.cwd === null &&
				hostArg.cwd === null &&
				reader instanceof tg.Process.Stdio.Reader &&
				writer instanceof tg.Process.Stdio.Writer
			);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
