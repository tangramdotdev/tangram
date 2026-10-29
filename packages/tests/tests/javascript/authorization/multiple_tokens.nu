use ../../../util/test.nu *

# Clients retain complementary proofs, prune covered permissions regardless of expiration, and keep the latest of equivalent proofs.

let local = server spawn

# Encode the tokens that the test uses.
let id = 'pcs_010000000000000000000000000000000000000000000000000000'
let file = 'fil_010000000000000000000000000000000000000000000000000000'
let aspects = ['' _command_objects _error_objects _log_objects _output_objects]
let specs = [
	{ resource: $id, expires_at: 1800, permissions: [process_node] }
	{ resource: $id, expires_at: 1200, permissions: [process_node_output_objects] }
	{ resource: $id, expires_at: 1200, permissions: [process_node_log_objects] }
	{ resource: $id, expires_at: 1200, permissions: [process_subtree process_subtree_output_objects process_subtree_log_objects] }
	{ resource: $id, expires_at: 1800, permissions: [process_parent] }
	{ resource: $id, expires_at: 2400, permissions: [process_parent], secret_id: 1 }
	{ resource: $file, expires_at: 2400, permissions: [object_subtree], secret_id: 2 }
	{ resource: $id, expires_at: 3000, permissions: [process_parent] }
	{ resource: $id, expires_at: 9223372036854775807, permissions: [process_parent] }
	{ resource: $id, expires_at: 2400, permissions: [process_node] }
] | append ($aspects | each { |aspect|
	[
		{ resource: $id, expires_at: 1200, permissions: [$'process_node($aspect)'] }
		{ resource: $id, expires_at: 1200, permissions: [$'process_subtree($aspect)'] }
	]
} | flatten)
let path = artifact {
	authorization.tg.ts: (token module $specs)
	tangram.ts: r#'
		import { token } from "./authorization.tg.ts";
		export default async function () {
			const id = "pcs_010000000000000000000000000000000000000000000000000000";
			const proof = (permissions, expires_at, resource = id, secretId = 0) => token(resource, expires_at * 60, permissions, secretId);
			const node = proof(["process_node"], 30);
			const output = proof(["process_node_output_objects"], 20);
			const log = proof(["process_node_log_objects"], 20);
			const broad = proof(["process_subtree", "process_subtree_output_objects", "process_subtree_log_objects"], 20);
			const parent = proof(["process_parent"], 30);
			const tokens = { local: [node, output] };
			tg.Authorization.Tokens.inherit(tokens, { local: [log, output] });
			tg.assert(tokens.local.length === 3);
			tg.Authorization.Tokens.inherit(tokens, { local: [broad] });
			tg.assert(tokens.local.length === 1 && tokens.local[0] === broad);
			tg.Authorization.Tokens.inherit(tokens, { local: [parent], remote: [output, log] });
			tg.assert(tokens.local.length === 1 && tokens.local[0] === parent);
			tg.assert(tokens.remote.length === 2);
			const differentSecret = proof(["process_parent"], 40, id, 1);
			tg.Authorization.Tokens.inherit(tokens, { local: [differentSecret] });
			tg.assert(tokens.local.length === 1 && tokens.local[0] === differentSecret);
			const fileId = "fil_010000000000000000000000000000000000000000000000000000";
			const fileToken = proof(["object_subtree"], 40, fileId, 2);
			const file = tg.File.withId(fileId);
			file.state.tokens = { local: [fileToken] };
			file.state.inheritTokens({ local: [parent], remote: [parent] });
			tg.assert(file.state.tokens.local.length === 1 && file.state.tokens.local[0] === fileToken);
			tg.assert(file.state.tokens.remote[0] === parent);
			const longer = proof(["process_parent"], 50);
			file.state.inheritTokens({ local: [longer] });
			tg.assert(file.state.tokens.local.length === 1 && file.state.tokens.local[0] === fileToken);
			const permanent = token(id, 9223372036854775807n, ["process_parent"]);
			tg.Authorization.Tokens.inherit(tokens, { local: [permanent] });
			tg.assert(tokens.local.length === 1 && tokens.local[0] === permanent);
			tg.assert(!tg.Authorization.Token.covers(proof(["process_node"], 40), output));
			for (const aspect of ["", "_command_objects", "_error_objects", "_log_objects", "_output_objects"]) {
				const node = proof(["process_node" + aspect], 20);
				const subtree = proof(["process_subtree" + aspect], 20);
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
