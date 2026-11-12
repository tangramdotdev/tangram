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

test("checkin lifetime aliases serialize with the canonical name", () => {
	for (let value of [0, 1.5, null]) {
		for (let name of ["timeToLive", "ttl"]) {
			assert.deepEqual(
				Checkin.Arg.toJson({
					options: { [name]: value },
					path: "/source",
					updates: [],
				}),
				{
					options: {
						tag_time_to_live: value === null ? "infinite" : `${value}s`,
					},
					path: "/source",
				},
			);
		}
	}
	assert.throws(
		() =>
			Checkin.Arg.toJson({
				options: { timeToLive: 1, ttl: 2 },
				path: "/source",
				updates: [],
			}),
		/may not both be set/,
	);
});
