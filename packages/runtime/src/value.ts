import * as tg from "./index.ts";

export type Value =
	| undefined
	| boolean
	| number
	| string
	| Array<Value>
	| { [key: string]: Value }
	| tg.Object
	| Uint8Array
	| tg.Mutation
	| tg.Template;

export namespace Value {
	export let toData = (value: Value): Data => {
		if (
			typeof value === "undefined" ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string"
		) {
			return value;
		} else if (value instanceof Array) {
			return value.map(toData);
		} else if (tg.Object.is(value)) {
			return { kind: "object", value: value.id };
		} else if (value instanceof Uint8Array) {
			return { kind: "bytes", value: value };
		} else if (value instanceof tg.Mutation) {
			return { kind: "mutation", value: tg.Mutation.toData(value) };
		} else if (value instanceof tg.Template) {
			return { kind: "template", value: tg.Template.toData(value) };
		} else if (typeof value === "object") {
			return {
				kind: "map",
				value: Object.fromEntries(
					Object.entries(value).map(([key, value]) => [key, toData(value)]),
				),
			};
		} else {
			throw new Error("invalid value");
		}
	};

	export let fromData = (data: tg.Value.Data): tg.Value => {
		if (
			typeof data === "undefined" ||
			typeof data === "boolean" ||
			typeof data === "number" ||
			typeof data === "string"
		) {
			return data;
		} else if (data instanceof Array) {
			return data.map(fromData);
		} else if (data.kind === "map") {
			return Object.fromEntries(
				Object.entries(data.value).map(([key, value]) => [
					key,
					fromData(value),
				]),
			);
		} else if (data.kind === "object") {
			return tg.Object.withId(data.value);
		} else if (data.kind === "bytes") {
			return data.value;
		} else if (data.kind === "mutation") {
			return tg.Mutation.fromData(data.value);
		} else if (data.kind === "template") {
			return tg.Template.fromData(data.value);
		} else {
			throw new Error("unknown value data");
		}
	};

	export let is = (value: unknown): value is Value => {
		return (
			value === undefined ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string" ||
			tg.Value.isArray(value) ||
			tg.Value.isMap(value) ||
			tg.Object.is(value) ||
			value instanceof Uint8Array ||
			value instanceof tg.Mutation ||
			value instanceof tg.Template
		);
	};

	export let expect = (value: unknown): Value => {
		tg.assert(is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Value => {
		tg.assert(is(value));
	};

	export let isArray = (value: unknown): value is Array<Value> => {
		if (!(value instanceof Array)) {
			return false;
		}
		return value.every((value) => Value.is(value));
	};

	export let isMap = (value: unknown): value is { [key: string]: Value } => {
		if (
			!(typeof value === "object" && value !== null) ||
			value instanceof Array ||
			value instanceof Uint8Array ||
			value instanceof tg.Mutation ||
			value instanceof tg.Template ||
			tg.Object.is(value)
		) {
			return false;
		}
		return Object.entries(value).every(([_, value]) => Value.is(value));
	};

	export let objects = (value: tg.Value): Array<tg.Object> => {
		if (value instanceof Array) {
			return value.flatMap(objects);
		} else if (tg.Value.isMap(value)) {
			return globalThis.Object.values(value).flatMap(objects);
		} else if (tg.Object.is(value)) {
			return [value];
		} else if (value instanceof tg.Mutation) {
			return value.children();
		} else if (value instanceof tg.Template) {
			return value.children();
		} else {
			return [];
		}
	};

	export let store = async (value: tg.Value): Promise<void> => {
		// Collect all unstored objects in reverse topological order.
		let unstored = [];
		let stack = tg.Value.objects(value).filter((object) => {
			return !object.state.stored;
		});
		while (stack.length > 0) {
			let object = stack.pop()!;
			unstored.push(object);
			if (object.state.object === undefined) {
				continue;
			}
			let kind = tg.Object.kind(object);
			let children = tg.Object.Object.children({
				kind,
				value: object.state.object!,
			} as tg.Object.Object);
			stack.push(...children.filter((object) => !object.state.stored));
		}
		unstored.reverse();
		if (unstored.length === 0) {
			return;
		}

		// Import.
		let items = [];
		for (let object of unstored) {
			if (object.state.object === undefined) {
				continue;
			}
			let kind = tg.Object.kind(object);
			let data = tg.Object.Object.toData({
				kind,
				value: object.state.object,
			} as tg.Object.Object);
			let id = syscall("object_id", data);
			object.state.id = id;
			items.push({ id, data });
		}
		await syscall("import", items);

		// Mark all objects stored.
		for (let object of unstored) {
			object.state.stored = true;
		}
	};

	export type Data =
		| undefined
		| boolean
		| number
		| string
		| Array<tg.Value.Data>
		| { kind: "map"; value: { [key: string]: tg.Value.Data } }
		| { kind: "object"; value: tg.Object.Id }
		| { kind: "bytes"; value: Uint8Array }
		| { kind: "mutation"; value: tg.Mutation.Data }
		| { kind: "template"; value: tg.Template.Data };
}
