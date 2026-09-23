use ../../lib/test.nu *

# Clients retain complementary proofs and prune covered permissions regardless of expiration.

let server = server spawn
let path = artifact {
	tangram.ts: r#'
		export default async function () {
			const id = "pcs_010000000000000000000000000000000000000000000000000000";
			const encode = (value) => tg.encoding.base64.encode(tg.encoding.utf8.encode(JSON.stringify(value)));
			const token = (permissions, expires_at, resource = id, key = "default", algorithm = "ed25519") =>
				`0.${encode({ expires_at: expires_at * 60, permissions, resource })}.${encode({ algorithm, key })}.`;
			const node = token(["process_node"], 30);
			const output = token(["process_node_output"], 20);
			const log = token(["process_node_log"], 20);
			const broad = token(["process_subtree", "process_subtree_output", "process_subtree_log"], 20);
			const parent = token(["process_parent"], 30);
			const tokens = { local: { authorization: [node, output] } };
			tg.Tokens.inherit(tokens, { local: { authorization: [log, output] } });
			tg.assert(tokens.local.authorization.length === 3);
			tg.Tokens.inherit(tokens, { local: { authorization: [broad] } });
			tg.assert(tokens.local.authorization.length === 1 && tokens.local.authorization[0] === broad);
			tg.Tokens.inherit(tokens, { local: { authorization: [parent] }, remote: { authorization: [output, log] } });
			tg.assert(tokens.local.authorization.length === 1 && tokens.local.authorization[0] === parent);
			tg.assert(tokens.remote.authorization.length === 2);
			const differentKey = token(["process_parent"], 40, id, "other");
			tg.Tokens.inherit(tokens, { local: { authorization: [differentKey] } });
			tg.assert(tokens.local.authorization.length === 1 && tokens.local.authorization[0] === [parent, differentKey].sort()[0]);
			const differentAlgorithm = token(["process_parent"], 40, id, "other", "other");
			tg.assert(tg.Authorization.Token.covers(differentAlgorithm, differentKey));
			tg.assert(tg.Authorization.Token.covers(differentKey, differentAlgorithm));
			tg.Tokens.inherit(tokens, { local: { authorization: [differentAlgorithm] } });
			tg.assert(tokens.local.authorization.length === 1);
			const fileId = "fil_010000000000000000000000000000000000000000000000000000";
			const fileToken = token(["object_subtree"], 40, fileId, "file", "other");
			const file = tg.File.withId(fileId);
			file.state.tokens = { local: { authorization: [fileToken] } };
			file.state.inheritTokens({ local: { authorization: [parent] }, remote: { authorization: [parent] } });
			tg.assert(file.state.tokens.local.authorization.length === 1 && file.state.tokens.local.authorization[0] === fileToken);
			tg.assert(file.state.tokens.remote.authorization[0] === parent);
			const longer = token(["process_parent"], 50);
			file.state.inheritTokens({ local: { authorization: [longer] } });
			tg.assert(file.state.tokens.local.authorization.length === 1 && file.state.tokens.local.authorization[0] === fileToken);
			const permanentBody = '{"expires_at":9223372036854775807,"permissions":["process_parent"],"resource":"' + id + '"}';
			const permanent = "0." + tg.encoding.base64.encode(tg.encoding.utf8.encode(permanentBody)) + "." + encode({ algorithm: "ed25519", key: "default" }) + ".";
			tg.Tokens.inherit(tokens, { local: { authorization: [permanent] } });
			tg.assert(tokens.local.authorization.length === 1 && tokens.local.authorization[0] === [parent, differentKey, differentAlgorithm, permanent].sort()[0]);
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
