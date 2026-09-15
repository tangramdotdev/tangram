use ../../../test.nu *

# Loading state merges the returned tokens without dropping existing tokens or locations.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const inherited = { local: { authorization: ["inherited"] }, remote: { authorization: ["remote"] } };
			const returned = { local: { authorization: ["returned"] } };
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
				object.state.finishStore({ node: objectId, options: { tokens: returned } });

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
				returned.local.authorization[0] = "mutated";
				inherited.local.authorization[0] = "mutated";
				inherited.remote.authorization[0] = "mutated";

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
	object: { local: { authorization: [returned inherited] }, remote: { authorization: [remote] } }
	process: { local: { authorization: [returned inherited] }, remote: { authorization: [remote] } }
	sandbox: { local: { authorization: [returned inherited] }, remote: { authorization: [remote] } }
}
assert equal $output $expected
