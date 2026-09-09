import { expect, spyOn, test } from "bun:test";
import * as tg from "../src/index.ts";

const id =
	"gph_010000000000000000000000000000000000000000000000000000" as tg.Graph.Id;
const process = "pcs_010000000000000000000000000000000000000000000000000000";
const token = (resource: string, permission: string, expires_at: number) =>
	`0.${Buffer.from(JSON.stringify({ resource, permissions: [permission], expires_at })).toString("base64")}.metadata.signature`;
const inherited = { local: "inherited", "remote=test": "remote" };

test("inheritance preserves existing process authorization", () => {
	for (const [permission, expiration] of [
		["process_node_output", 0],
		["process_subtree_output", Number.MAX_SAFE_INTEGER],
	] as const) {
		const existing = token(process, permission, Number.MAX_SAFE_INTEGER);
		const state = new tg.Object.State({
			id,
			stored: true,
			tokens: { local: existing },
		});
		state.inheritTokens({ local: token(id, "object_node", expiration) });
		expect(state.tokens).toEqual({ local: existing });
	}
});

test("inheritance does not compute the object ID", () => {
	const state = new tg.Object.State({
		object: { kind: "graph", value: { nodes: [] } },
		stored: false,
	});
	const objectId = spyOn(tg.client, "objectId").mockReturnValue(id);
	try {
		state.inheritTokens({});
		state.inheritTokens(inherited);
		expect(objectId).not.toHaveBeenCalled();
		expect(state.tokens).toEqual(inherited);
	} finally {
		objectId.mockRestore();
	}
});

test("store refreshes returned tokens and preserves other locations", () => {
	for (const local of ["returned", undefined]) {
		const state = new tg.Object.State({ id, stored: false, tokens: inherited });
		const returned: tg.Authorization.Tokens =
			local === undefined ? {} : { local };
		state.finishStore({ node: id, options: { tokens: returned } });
		returned.local = "mutated";
		expect(state.tokens).toEqual({
			local: local ?? "inherited",
			"remote=test": "remote",
		});
		expect(state.stored).toBe(true);
	}
});

test("load refreshes returned tokens and preserves concurrent inheritance", async () => {
	const original = tg.client.getObject;
	try {
		for (const returned of [{ local: "returned" }, {}, null, undefined]) {
			const state = new tg.Object.State({
				id,
				stored: true,
				tokens: inherited,
			});
			tg.client.getObject = async (_id, arg) => {
				expect(arg?.tokens).toEqual(inherited);
				state.inheritTokens({ "remote=late": "late" });
				return {
					data: { kind: "graph", value: { nodes: [] } },
					...(returned === undefined ? {} : { tokens: returned }),
				};
			};
			await state.load();
			const local = returned?.local ?? "inherited";
			if (returned !== undefined && returned !== null)
				returned.local = "mutated";
			expect(state.tokens).toEqual({
				local,
				"remote=late": "late",
				"remote=test": "remote",
			});
		}
	} finally {
		tg.client.getObject = original;
	}
});
