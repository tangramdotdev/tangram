use ../../../test.nu *

# Clients retain complementary proofs and prune only covered permissions and lifetimes.

let server = server spawn
let path = artifact {
	tangram.ts: r#'
		export default async function () {
			const id = "pcs_010000000000000000000000000000000000000000000000000000";
			const encode = (value) => tg.encoding.base64.encode(tg.encoding.utf8.encode(JSON.stringify(value)));
			const token = (permissions, expires_at, resource = id, key = "default") =>
				`0.${encode({ expires_at, permissions, resource })}.${encode({ algorithm: "ed25519", key })}.`;
			const node = token(["process_node"], 30);
			const output = token(["process_node_output"], 20);
			const log = token(["process_node_log"], 20);
			const broad = token(["process_subtree", "process_subtree_output", "process_subtree_log"], 20);
			const parent = token(["process_parent"], 30);
			const tokens = { local: [node, output] };
			tg.Authorization.Tokens.inherit(tokens, { local: [log, output] });
			tg.assert(tokens.local.length === 3);
			tg.Authorization.Tokens.inherit(tokens, { local: [broad] });
			tg.assert(tokens.local.length === 2 && tokens.local.includes(node) && tokens.local.includes(broad));
			tg.Authorization.Tokens.inherit(tokens, { local: [parent], remote: [output, log] });
			tg.assert(tokens.local.length === 1 && tokens.local[0] === parent);
			tg.assert(tokens.remote.length === 2);
			const differentKey = token(["process_parent"], 40, id, "other");
			tg.Authorization.Tokens.inherit(tokens, { local: [differentKey] });
			tg.assert(tokens.local.length === 2);
			const permanentBody = '{"expires_at":9223372036854775807,"permissions":["process_parent"],"resource":"' + id + '"}';
			const permanent = "0." + tg.encoding.base64.encode(tg.encoding.utf8.encode(permanentBody)) + "." + encode({ algorithm: "ed25519", key: "default" }) + ".";
			tg.Authorization.Tokens.inherit(tokens, { local: [permanent] });
			tg.assert(tokens.local.length === 2 && tokens.local.includes(permanent) && !tokens.local.includes(parent));
			tg.assert(!tg.Authorization.Token.covers(token(["process_node"], 40), output));
			for (const aspect of ["", "_command", "_error", "_log", "_output"]) {
				const node = token(["process_node" + aspect], 20);
				const subtree = token(["process_subtree" + aspect], 20);
				tg.assert(tg.Authorization.Token.covers(subtree, node));
				tg.assert(!tg.Authorization.Token.covers(node, subtree));
			}
			const referent = { node: id, options: { tokens } };
			const string = tg.Referent.toDataString(referent, (id) => id);
			const restored = tg.Referent.fromDataString(string, (id) => id);
			tg.assert(JSON.stringify(restored.options.tokens) === JSON.stringify(tokens));
			const reference = tg.Reference.toDataString(referent, (id) => id);
			tg.assert(JSON.stringify(tg.Reference.fromDataString(reference, (id) => id).options.tokens) === JSON.stringify(tokens));
			return true;
		}
	'#
}
assert equal (tg build $path | from json) true
