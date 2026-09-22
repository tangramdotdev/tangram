use ../lib/test.nu *

# Cached descendants inherit sync context even when they already carry authorization.
let server = server spawn

let path = artifact {
	tangram.ts: '
		export function identity(arg: tg.Value) {
			return arg;
		}
		export default async function () {
			const child = tg.Directory.withObject({ entries: {} });
			child.state.tokens = { local: { authorization: ["child-authorization"] } };
			const parent = tg.Directory.withObject({ entries: { a: child } });
			parent.state.tokens = { local: { authorization: ["parent-authorization"], sync: ["incoming-sync"] } };
			const result = await parent.get("a");
			tg.assert(JSON.stringify(result.state.tokens.local?.authorization) === JSON.stringify(["child-authorization", "parent-authorization"]));
			tg.assert(result.state.tokens.local?.sync?.[0] === "incoming-sync");
			const copy = result.state.tokens;
			copy.local!.sync![0] = "changed-sync";
			tg.assert(result.state.tokens.local?.sync?.[0] === "incoming-sync");
			const inherited = result.state.tokens;
			tg.Tokens.inherit(inherited, { remote: { sync: ["remote-sync"] } });
			tg.assert(parent.state.tokens.remote === undefined);
			inherited.local.authorization[0] = "mutated";
			tg.assert(parent.state.tokens.local.authorization[0] === "parent-authorization");
			tg.assert(result.state.tokens.local.authorization[0] === "child-authorization");
			inherited.local.authorization[0] = "child-authorization";
			inherited.local.authorization.push("another-authorization");
			tg.Tokens.normalize(inherited);
			const referent = { node: "a", options: { tokens: inherited } };
			const encoded = tg.Referent.toDataString(referent, (value) => value);
			const decoded = tg.Referent.fromDataString(encoded, (value) => value);
			tg.assert(JSON.stringify(decoded.options?.tokens?.local?.authorization) === JSON.stringify(inherited.local.authorization));
			tg.assert(decoded.options?.tokens?.local?.sync?.[0] === "incoming-sync");
			tg.assert(decoded.options?.tokens?.remote?.sync?.[0] === "remote-sync");
			const argument = tg.Directory.withObject({ entries: {} });
			argument.state.tokens = inherited;
			const command = await tg.command(identity, argument);
			tg.assert(tg.Tokens.isEmpty(command.state.tokens));
			tg.assert(JSON.stringify(argument.state.tokens) === JSON.stringify(inherited));
			const data = tg.Object.Object.toData(command.state.object!);
			tg.Object.Data.withoutLocationAndTokens(data);
			tg.assert(JSON.stringify(argument.state.tokens) === JSON.stringify(inherited));
			const first = tg.Directory.withObject({ entries: {} });
			const second = tg.Directory.withObject({ entries: {} });
			first.state.tokens = { local: { authorization: ["first-authorization"], sync: ["first-sync"] } };
			second.state.tokens = { local: { authorization: ["second-authorization"], sync: ["second-sync"] } };
			const inputs = tg.Directory.withObject({ entries: { first, second, shared: first } });
			const wrapper = tg.Directory.withObject({ entries: { inputs } });
			const lifted = tg.Object.toReferent(wrapper).options!.tokens!.local!;
			tg.assert(JSON.stringify(lifted.sync!.toSorted()) === JSON.stringify(["first-sync", "second-sync"]));
			tg.assert(JSON.stringify(lifted.authorization!.toSorted()) === JSON.stringify(["first-authorization", "second-authorization"]));
			tg.assert(tg.Tokens.isEmpty(wrapper.state.tokens));
			tg.assert(tg.Tokens.isEmpty(inputs.state.tokens));
			tg.assert(first.state.tokens.local.sync!.length === 1);
			tg.assert(second.state.tokens.local.sync!.length === 1);
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
