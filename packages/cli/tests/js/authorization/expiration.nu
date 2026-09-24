use ../../lib/test.nu *

# Token pruning ignores expiration and preserves the merge laws.
let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () {
			const id = "fil_010000000000000000000000000000000000000000000000000000";
			const other = "dir_010000000000000000000000000000000000000000000000000000";
			const encode = (value) => tg.encoding.base64.encode(tg.encoding.utf8.encode(JSON.stringify(value)));
			const proof = (resource, expires_at, permission) => `0.${encode({ expires_at, permissions: [permission], resource })}.${encode({ algorithm: "ed25519", key: "test" })}.`;
			const inputs = [id, other].flatMap((resource) => [120, 179, 180, 181].flatMap((expiration) => ["object_node", "object_subtree"].map((permission) => ({ local: [proof(resource, expiration, permission)] }))));
			const merge = (a, b, resource) => {
				const output = tg.Authorization.Tokens.clone(a);
				tg.Authorization.Tokens.inherit(output, b, resource);
				return output;
			};
			const equal = (a, b) => tg.assert(JSON.stringify(a.local.toSorted()) === JSON.stringify(b.local.toSorted()));
			for (const resource of [undefined, id]) {
				for (const a of inputs) {
					equal(merge(a, a, resource), a);
					for (const b of inputs) {
						const ab = merge(a, b, resource);
						equal(ab, merge(b, a, resource));
						equal(ab, merge(ab, ab, resource));
						for (const c of inputs) equal(merge(ab, c, resource), merge(a, merge(b, c, resource), resource));
					}
				}
			}
			const processId = "pcs_010000000000000000000000000000000000000000000000000000";
			for (const [resource, granted, needed, unrelated] of [[id, "object_subtree", "object_node", "process_node"], [processId, "process_subtree", "process_node", "process_node_output"]]) {
				const token = proof(resource, 120, granted);
				tg.assert(tg.Authorization.Token.grants(token, resource, granted));
				tg.assert(tg.Authorization.Token.grants(token, resource, needed));
				tg.assert(!tg.Authorization.Token.grants(token, other, needed));
				tg.assert(!tg.Authorization.Token.grants(token, resource, unrelated));
				tg.assert(tg.Authorization.Token.grantsObjectSubtree(token, resource) === (resource === id));
			}
			const chain = [proof(processId, 120, "process_parent"), proof(processId, 179, "process_subtree"), proof(processId, 238, "process_node")];
			for (const order of [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]]) {
				const tokens = { local: order.map((index) => chain[index]) };
				tg.Authorization.Tokens.normalize(tokens);
				tg.assert(JSON.stringify(tokens.local) === JSON.stringify([chain[0]]));
				const sequential = {};
				for (const index of order) tg.Authorization.Tokens.inherit(sequential, { local: [chain[index]] });
				equal(sequential, tokens);
			}
			const earlier = { local: [proof(id, 120, "object_subtree")] };
			const later = { local: [proof(id, 121, "object_subtree")] };
			const expected = { local: [earlier.local[0], later.local[0]].sort().slice(0, 1) };
			equal(merge(earlier, later, id), expected);
			equal(merge(later, earlier, id), expected);
			const leaf = tg.Directory.withId(other);
			const middle = tg.Directory.withObject({ entries: { leaf } });
			const root = tg.Directory.withObject({ entries: { middle } });
			const rootProof = proof(root.id, 120, "object_subtree");
			const leafProof = proof(leaf.id, 238, "object_subtree");
			root.state.tokens = { local: [rootProof] };
			middle.state.tokens = { local: [proof(middle.id, 179, "object_subtree")] };
			leaf.state.tokens = { local: [leafProof] };
			equal(tg.Object.toReferent(root).options.tokens, { local: [rootProof] });
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
