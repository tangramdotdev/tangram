use ../../lib/test.nu *

# Assignment, inheritance, loading, storing, and collection use the same coverage rules.
let server = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			const id = "dir_010000000000000000000000000000000000000000000000000000" as tg.Directory.Id;
			const other = tg.Directory.withObject({ entries: {} }).id;
			const encode = (value) => tg.encoding.base64.encode(tg.encoding.utf8.encode(JSON.stringify(value)));
			const proof = (resource, expires_at) => `0.${encode({ expires_at, permissions: ["object_subtree"], resource })}.${encode({ algorithm: "ed25519", key: "test" })}.`;
			const direct = proof(id, 20);
			const inherited = proof(other, 20);
			for (const authorization of [[direct, inherited], [inherited, direct]]) {
				const object = tg.Directory.withId(id);
				for (const token of authorization) {
					object.state.inheritTokens({ local: { authorization: [token] } });
				}
				tg.assert(JSON.stringify(object.state.tokens.local.authorization) === JSON.stringify([direct]));
				object.state.tokens = { local: { authorization } };
				tg.assert(JSON.stringify(object.state.tokens.local.authorization) === JSON.stringify([direct]));
			}
			const commandId = "cmd_010000000000000000000000000000000000000000000000000000";
			const commandToken = proof(commandId, 20);
			const state = { command: { node: commandId, options: { tokens: { local: { authorization: [commandToken] } } } }, error: null, log: null } as tg.Process.State;
			tg.Process.State.inheritTokens(state, { local: { authorization: [inherited] } });
			tg.assert(JSON.stringify(state.command.options.tokens.local.authorization) === JSON.stringify([commandToken]));
			const getObject = tg.client.getObject;
			try {
				tg.client.getObject = async () => ({ data: { kind: "directory", value: { entries: {} } }, tokens: { local: { authorization: [direct] } } });
				const object = tg.Directory.withId(id);
				object.state.tokens = { local: { authorization: [inherited] } };
				await object.state.load();
				tg.assert(JSON.stringify(object.state.tokens.local.authorization) === JSON.stringify([direct]));
				object.state.tokens = { local: { authorization: [inherited] } };
				object.state.finishStore({ node: id, options: { tokens: { local: { authorization: [direct] } } } });
				tg.assert(JSON.stringify(object.state.tokens.local.authorization) === JSON.stringify([direct]));
			} finally {
				tg.client.getObject = getObject;
			}
			const child = tg.Directory.withId(other);
			const parent = tg.Directory.withObject({ entries: { child } });
			const parentToken = proof(parent.id, 120);
			parent.state.tokens = { local: { authorization: [parentToken] } };
			for (const expiration of [60, 120, 121, 179, 180, 181, 240]) {
				const childToken = proof(child.id, expiration);
				child.state.tokens = { local: { authorization: [childToken], sync: ["sync"] }, remote: { authorization: [childToken] } };
				const tokens = tg.Object.toReferent(parent).options.tokens;
				tg.assert(tokens.local.authorization.includes(parentToken));
				tg.assert(tokens.local.authorization.includes(childToken) === (expiration > 180));
				tg.assert(tokens.local.sync[0] === "sync");
				tg.assert(tokens.remote.authorization[0] === childToken);
				tg.assert(child.state.tokens.local.authorization[0] === childToken);
			}
			child.state.object = { kind: "directory", value: { entries: { parent } } };
			const cyclic = tg.Object.toReferent(parent).options.tokens;
			tg.assert(cyclic.local.authorization.length === 2);
			tg.assert(cyclic.local.sync.length === 1);
			child.state.object = null;
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
