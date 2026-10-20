import { strict as assert } from "node:assert";
import { test } from "node:test";
import * as tg from "../src/index.ts";

test("withEntries creates a leaf when the entries fit", async () => {
	let directory = tg.Directory.withEntries(entries(names(1024)));
	assert.ok("entries" in object(directory));
});

test("withEntries splits entries into leaves like checkin", async () => {
	let sorted = names(1025).sort();
	let directory = tg.Directory.withEntries(entries(sorted));
	assert.deepEqual(shape(directory), [
		[1024, sorted[1023]],
		[1, sorted[1024]],
	]);
});

test("withEntries nests branches until the children fit", async () => {
	let sorted = names(1024 * 128 + 1).sort();
	let directory = tg.Directory.withEntries(entries(sorted));
	assert.deepEqual(shape(directory), [
		[1024 * 128, sorted[1024 * 128 - 1]],
		[1, sorted[1024 * 128]],
	]);
	let root = object(directory);
	assert.ok("children" in root);
	let child = root.children[0]!.directory;
	assert.ok(child instanceof tg.Directory);
	assert.equal(shape(child).length, 128);
});

test("withEntries orders entries by code point", async () => {
	let directory = tg.Directory.withEntries(
		entries([...names(1023), "\u{1f600}", "\u{ff61}"]),
	);
	assert.deepEqual(
		shape(directory).map(([, last]) => last),
		["\u{ff61}", "\u{1f600}"],
	);
});

test("new nests each full chunk of blob children in a branch", async () => {
	let children = Array.from({ length: 1025 }, (_, index) => {
		let bytes = new Uint8Array([index % 256]);
		return { blob: tg.Blob.withObject({ bytes }), length: 1 };
	});
	let blob = await tg.Blob.new({ children });
	let value = blob.state.object;
	assert.ok(
		value !== null && value.kind === "blob" && "children" in value.value,
	);
	assert.deepEqual(
		value.value.children.map((child) => child.length),
		[1024, 1],
	);
});

let names = (count: number): Array<string> =>
	Array.from({ length: count }, (_, index) => String(index));

let entries = (names: Array<string>): { [key: string]: number } =>
	Object.fromEntries(names.map((name) => [name, 0]));

/** Get a directory's object without loading it. */
let object = (directory: tg.Directory): tg.Directory.Object => {
	let object = directory.state.object;
	assert.ok(object !== null && object.kind === "directory");
	return object.value;
};

/** Get the count and last name of each child of a branch. */
let shape = (directory: tg.Directory): Array<[number, string]> => {
	let branch = object(directory);
	assert.ok("children" in branch);
	return branch.children.map((child) => [child.count, child.last]);
};
