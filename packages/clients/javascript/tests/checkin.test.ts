import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Checkin } from "../src/client/checkin.ts";

test("checkin preserves omitted and explicit watch preferences", () => {
	for (const watch of [undefined, false, true]) {
		const output = Checkin.Arg.toJson({
			options: { watch },
			path: "/source",
			updates: [],
		});
		assert.deepEqual(output, {
			options: watch === undefined ? {} : { watch },
			path: "/source",
		});
	}
});
