use ../../lib/test.nu *

# Expiration coverage uses a direct threshold and preserves pairwise merge laws.
let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () {
			const id = "fil_010000000000000000000000000000000000000000000000000000";
			const other = "dir_010000000000000000000000000000000000000000000000000000";
			const encode = (value) => tg.encoding.base64.encode(tg.encoding.utf8.encode(JSON.stringify(value)));
			const proof = (resource, expires_at, permission) => `0.${encode({ expires_at, permissions: [permission], resource })}.${encode({ algorithm: "ed25519", key: "test" })}.`;
			for (const [a, b, expected] of [[120n, 121n, true], [120n, 179n, true], [120n, 180n, true], [120n, 181n, false], [179n, 180n, true], [181n, 120n, true], [179n, 238n, true], [120n, 238n, false], [-60n, -1n, true], [-1n, 0n, true], [-60n, 1n, false], [(1n << 63n) - 61n, (1n << 63n) - 1n, true], [(1n << 63n) - 62n, (1n << 63n) - 1n, false], [-(1n << 63n), (1n << 63n) - 1n, false]]) {
				tg.assert(tg.Authorization.Token.coversExpiration(a, b) === expected);
			}
			const inputs = [id, other].flatMap((resource) => [120, 179, 180, 181].flatMap((expiration) => ["object_node", "object_subtree"].map((permission) => ({ local: { authorization: [proof(resource, expiration, permission)] } }))));
			const merge = (a, b, resource) => {
				const output = tg.Tokens.clone(a);
				tg.Tokens.inherit(output, b, resource);
				return output;
			};
			const equal = (a, b) => tg.assert(JSON.stringify(a.local.authorization.toSorted()) === JSON.stringify(b.local.authorization.toSorted()));
			for (const resource of [undefined, id]) {
				for (const a of inputs) {
					equal(merge(a, a, resource), a);
					for (const b of inputs) {
						const ab = merge(a, b, resource);
						equal(ab, merge(b, a, resource));
						equal(ab, merge(ab, ab, resource));
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
				const tokens = { local: { authorization: order.map((index) => chain[index]) } };
				tg.Tokens.normalize(tokens);
				tg.assert(JSON.stringify(tokens.local.authorization) === JSON.stringify([chain[0]]));
			}
			const earlier = { local: { authorization: [proof(id, 120, "object_subtree")] } };
			const later = { local: { authorization: [proof(id, 121, "object_subtree")] } };
			equal(merge(earlier, later, id), later);
			equal(merge(later, earlier, id), later);
			const leaf = tg.Directory.withId(other);
			const middle = tg.Directory.withObject({ entries: { leaf } });
			const root = tg.Directory.withObject({ entries: { middle } });
			const rootProof = proof(root.id, 120, "object_subtree");
			const leafProof = proof(leaf.id, 238, "object_subtree");
			root.state.tokens = { local: { authorization: [rootProof] } };
			middle.state.tokens = { local: { authorization: [proof(middle.id, 179, "object_subtree")] } };
			leaf.state.tokens = { local: { authorization: [leafProof] } };
			equal(tg.Object.toReferent(root).options.tokens, { local: { authorization: [rootProof, leafProof] } });
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
