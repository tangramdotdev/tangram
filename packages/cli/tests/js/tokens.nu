use ../../test.nu *

# Cached descendants inherit sync context even when they already carry authorization.
let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const child = tg.Directory.withObject({ entries: {} });
			child.state.tokens = { local: { authorization: ["child-authorization"] } };
			const parent = tg.Directory.withObject({ entries: { a: child } });
			parent.state.tokens = { local: { authorization: ["parent-authorization"], sync: "incoming-sync" } };
			const result = await parent.get("a");
			tg.assert(JSON.stringify(result.state.tokens.local?.authorization) === JSON.stringify(["child-authorization", "parent-authorization"]));
			tg.assert(result.state.tokens.local?.sync === "incoming-sync");
			const copy = result.state.tokens;
			copy.local!.sync = "changed-sync";
			tg.assert(result.state.tokens.local?.sync === "incoming-sync");
			const inherited = result.state.tokens;
			tg.Tokens.inherit(inherited, { remote: { sync: "remote-sync" } });
			tg.assert(parent.state.tokens.remote === undefined);
			inherited.local.authorization[0] = "mutated";
			tg.assert(parent.state.tokens.local.authorization[0] === "parent-authorization");
			tg.assert(result.state.tokens.local.authorization[0] === "child-authorization");
			inherited.local.authorization[0] = "child-authorization";
			inherited.local.authorization.push("another-authorization");
			const referent = { node: "a", options: { tokens: inherited } };
			const encoded = tg.Referent.toDataString(referent, (value) => value);
			const decoded = tg.Referent.fromDataString(encoded, (value) => value);
			tg.assert(JSON.stringify(decoded.options?.tokens?.local?.authorization) === JSON.stringify(inherited.local.authorization));
			tg.assert(decoded.options?.tokens?.local?.sync === "incoming-sync");
			tg.assert(decoded.options?.tokens?.remote?.sync === "remote-sync");
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
