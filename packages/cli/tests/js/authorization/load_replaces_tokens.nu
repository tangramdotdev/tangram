use ../../../test.nu *

# Loading state replaces inherited tokens with the non-empty tokens returned by the server.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const inherited = { local: "inherited", remote: "remote" };
			const returned = { local: "returned" };
			const commandId = "cmd_010000000000000000000000000000000000000000000000000000" as tg.Command.Id;
			const objectId = "blb_010000000000000000000000000000000000000000000000000000" as tg.Blob.Id;
			const processId = "pcs_010000000000000000000000000000000000000000000000000000" as tg.Process.Id;
			const sandboxId = "sbx_010000000000000000000000000000000000000000000000000000" as tg.Sandbox.Id;

			const getObject = tg.client.getObject;
			const getProcess = tg.client.getProcess;
			const getSandbox = tg.client.getSandbox;
			try {
				tg.client.getObject = async () => ({
					data: { kind: "blob", value: { bytes: "" } },
					tokens: returned,
				});
				tg.client.getProcess = async () => ({
					data: {
						command: commandId,
						created_at: 0,
						host: "test",
						sandbox: sandboxId,
						status: "started",
					},
					id: processId,
					tokens: returned,
				});
				tg.client.getSandbox = async () => ({
					id: sandboxId,
					status: "started",
					tokens: returned,
				});

				const object = tg.Blob.withId(objectId);
				object.state.tokens = { ...inherited };
				await object.state.load();

				const process = new tg.Process({
					id: processId,
					stderr: new tg.Process.Stdio.Reader({ stream: "stderr" }),
					stdin: new tg.Process.Stdio.Writer({ stream: "stdin" }),
					stdout: new tg.Process.Stdio.Reader({ stream: "stdout" }),
					tokens: { ...inherited },
				});
				await process.load();

				const sandbox = new tg.Sandbox({
					id: sandboxId,
					tokens: { ...inherited },
				});
				await sandbox.load();
				returned.local = "mutated";

				return {
					object: object.state.tokens,
					process: process.tokens,
					sandbox: sandbox.tokens,
				};
			} finally {
				tg.client.getObject = getObject;
				tg.client.getProcess = getProcess;
				tg.client.getSandbox = getSandbox;
			}
		}
	'
}

let output = tg build $path | from json
let expected = {
	object: { local: returned }
	process: { local: returned }
	sandbox: { local: returned }
}
assert equal $output $expected
