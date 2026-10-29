use ../../../util/test.nu *

# Assignment, inheritance, loading, storing, and collection use the same coverage rules.
let local = server spawn

# Get the IDs of the directories that the test creates.
let ids = tg build (artifact {
	tangram.ts: '
		export default () => {
			const other = tg.Directory.withObject({ entries: {} }).id;
			const child = tg.Directory.withId(other);
			const parent = tg.Directory.withObject({ entries: { child } });
			return { other, parent: parent.id };
		};
	'
}) | from json

# Encode the tokens that the test uses.
let id = 'dir_010000000000000000000000000000000000000000000000000000'
let specs = [
	{ resource: $id, expires_at: 20, permissions: [object_subtree] }
	{ resource: $ids.other, expires_at: 20, permissions: [object_subtree] }
	{ resource: 'cmd_010000000000000000000000000000000000000000000000000000', expires_at: 20, permissions: [object_subtree] }
	{ resource: 'syn_0000000000000000000000000000', expires_at: 120, permissions: [sync_read] }
	{ resource: $ids.parent, expires_at: 120, permissions: [object_subtree] }
] | append ([60 120 121 179 180 181 240] | each { |expires_at| { resource: $ids.other, expires_at: $expires_at, permissions: [object_subtree] } })
let path = artifact {
	authorization.tg.ts: (token module $specs)
	tangram.ts: '
		import { token } from "./authorization.tg.ts";
		export default async function () {
			const id = "dir_010000000000000000000000000000000000000000000000000000" as tg.Directory.Id;
			const other = tg.Directory.withObject({ entries: {} }).id;
			const proof = (resource, expires_at) => token(resource, expires_at, ["object_subtree"]);
			const direct = proof(id, 20);
			const inherited = proof(other, 20);
			for (const authorization of [[direct, inherited], [inherited, direct]]) {
				const object = tg.Directory.withId(id);
				for (const token of authorization) {
					object.state.inheritTokens({ local: [token] });
				}
				tg.assert(JSON.stringify(object.state.tokens.local) === JSON.stringify([direct]));
				object.state.tokens = { local: authorization };
				tg.assert(JSON.stringify(object.state.tokens.local) === JSON.stringify([direct]));
			}
			const commandId = "cmd_010000000000000000000000000000000000000000000000000000";
			const commandToken = proof(commandId, 20);
			const state = { command: { node: commandId, options: { tokens: { local: [commandToken] } } }, error: null, log: null } as tg.Process.State;
			tg.Process.State.inheritTokens(state, { local: [inherited] });
			tg.assert(JSON.stringify(state.command.options.tokens.local) === JSON.stringify([commandToken]));
			const getObject = tg.client.getObject;
			try {
				tg.client.getObject = async () => ({ data: { kind: "directory", value: { entries: {} } }, tokens: { local: [direct] } });
				const object = tg.Directory.withId(id);
				object.state.tokens = { local: [inherited] };
				await object.state.load();
				tg.assert(JSON.stringify(object.state.tokens.local) === JSON.stringify([direct]));
				object.state.tokens = { local: [inherited] };
				const normalize = tg.Authorization.Tokens.normalize;
				let normalizations = 0;
				tg.Authorization.Tokens.normalize = (...args) => { normalizations++; return normalize(...args); };
				try {
					object.state.finishStore({ node: id, options: { tokens: { local: [direct] } } });
				} finally {
					tg.Authorization.Tokens.normalize = normalize;
				}
				tg.assert(normalizations === 1);
				tg.assert(JSON.stringify(object.state.tokens.local) === JSON.stringify([direct]));
			} finally {
				tg.client.getObject = getObject;
			}
			const sync = token("syn_0000000000000000000000000000", 120, ["sync_read"]);
			const child = tg.Directory.withId(other);
			const parent = tg.Directory.withObject({ entries: { child } });
			const parentToken = proof(parent.id, 120);
			parent.state.tokens = { local: [parentToken] };
			for (const expiration of [60, 120, 121, 179, 180, 181, 240]) {
				const childToken = proof(child.id, expiration);
				child.state.tokens = { local: [childToken, sync], remote: [childToken] };
				const tokens = tg.Object.toReferent(parent).options.tokens;
				tg.assert(tokens.local.includes(parentToken));
				tg.assert(!tokens.local.includes(childToken));
				tg.assert(tokens.remote[0] === childToken);
				tg.assert(child.state.tokens.local.includes(childToken));
				tg.assert(tokens.local.includes(sync));
			}
			child.state.object = { kind: "directory", value: { entries: { parent } } };
			const cyclic = tg.Object.toReferent(parent).options.tokens;
			tg.assert(cyclic.local.length === 2);
			child.state.object = null;
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
