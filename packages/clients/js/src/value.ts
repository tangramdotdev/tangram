import * as tg from "./index.ts";
import {
	Printer as Printer_,
	type Options as PrintOptions_,
} from "./value/print.ts";

/** The union of all types that can be used as the input or output of Tangram commands. */
export type Value =
	| null
	| boolean
	| number
	| string
	| Array<Value>
	| { [key: string]: Value }
	| tg.Object
	| Uint8Array
	| tg.Mutation
	| tg.Module
	| tg.Template
	| tg.Placeholder;

export namespace Value {
	/** Parse TGON to a value. */
	export let parse = (value: string): tg.Value => {
		return fromData(tg.client.parseValue(value));
	};

	/** Serialize a value to TGON. */
	export let stringify = (value: tg.Value): string => {
		return tg.client.stringifyValue(tg.Value.toData(value));
	};

	export type PrintOptions = PrintOptions_;

	export let print = (
		value: tg.Value,
		options?: PrintOptions | null,
	): string => {
		return new Printer_(options ?? {}).print(value);
	};

	export let toData = (value: Value): Data => {
		if (value === null) {
			return null;
		} else if (
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string"
		) {
			return value;
		} else if (value instanceof Array) {
			return value.map(toData);
		} else if (tg.Object.is(value)) {
			let referent = tg.Object.toReferent(value);
			let value_ = tg.Referent.toDataString(referent, (id) => id);
			return { kind: "object", value: value_ };
		} else if (value instanceof Uint8Array) {
			return { kind: "bytes", value: tg.encoding.base64.encode(value) };
		} else if (value instanceof tg.Mutation) {
			return { kind: "mutation", value: tg.Mutation.toData(value) };
		} else if (value instanceof tg.Module) {
			return { kind: "module", value: tg.Module.toData(value) };
		} else if (value instanceof tg.Template) {
			return { kind: "template", value: tg.Template.toData(value) };
		} else if (value instanceof tg.Placeholder) {
			return { kind: "placeholder", value: tg.Placeholder.toData(value) };
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
		if (data === null) {
			return null;
		} else if (
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
			let referent = tg.Referent.fromDataString(
				data.value,
				(id) => id as tg.Object.Id,
			);
			return tg.Object.withReferent(referent);
		} else if (data.kind === "bytes") {
			return tg.encoding.base64.decode(data.value);
		} else if (data.kind === "mutation") {
			return tg.Mutation.fromData(data.value);
		} else if (data.kind === "module") {
			return tg.Module.fromData(data.value);
		} else if (data.kind === "template") {
			return tg.Template.fromData(data.value);
		} else if (data.kind === "placeholder") {
			return tg.Placeholder.fromData(data.value);
		} else {
			throw new Error("unknown value data");
		}
	};

	/** Check if a value is a `tg.Value`. */
	export let is = (value: unknown): value is Value => {
		return (
			value === null ||
			typeof value === "boolean" ||
			typeof value === "number" ||
			typeof value === "string" ||
			tg.Value.isArray(value) ||
			tg.Value.isMap(value) ||
			tg.Object.is(value) ||
			value instanceof Uint8Array ||
			value instanceof tg.Mutation ||
			value instanceof tg.Module ||
			value instanceof tg.Template ||
			value instanceof tg.Placeholder
		);
	};

	/** Expect that a value is a `tg.Value`. */
	export let expect = (value: unknown): Value => {
		tg.assert(is(value));
		return value;
	};

	/** Assert that a value is a `tg.Value`. */
	export let assert = (value: unknown): asserts value is Value => {
		tg.assert(is(value));
	};

	export let isArray = (value: unknown): value is Array<Value> => {
		if (!(value instanceof Array)) {
			return false;
		}
		return value.every((value) => Value.is(value));
	};

	/** Assert that a value is a valid map. */
	export let isMap = (value: unknown): value is { [key: string]: Value } => {
		if (
			!(typeof value === "object" && value !== null) ||
			value instanceof Array ||
			value instanceof Uint8Array ||
			value instanceof tg.Mutation ||
			value instanceof tg.Module ||
			value instanceof tg.Template ||
			value instanceof tg.Placeholder ||
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
			return value.objects();
		} else if (value instanceof tg.Module) {
			return tg.Module.children(value);
		} else if (value instanceof tg.Template) {
			return value.objects();
		} else if (value instanceof tg.Placeholder) {
			return [];
		} else {
			return [];
		}
	};

	export let inheritLocation = (
		value: tg.Value,
		location: tg.Location | null,
	): void => {
		for (let object of tg.Value.objects(value)) {
			tg.Object.inheritLocation(object, location);
		}
	};

	export let inheritTokens = (
		value: tg.Value,
		tokens: tg.Authorization.Tokens,
	): void => {
		for (let object of tg.Value.objects(value)) {
			tg.Object.inheritTokens(object, tokens);
		}
	};

	export let store = async (value: tg.Value): Promise<void> => {
		while (true) {
			// Collect all unstored states with children before parents.
			let pending = new Set<Promise<void>>();
			let states: Array<tg.Object.State> = [];
			let stack = tg.Value.objects(value).map((object) => ({
				expanded: false,
				object,
			}));
			let visited = new Set<tg.Object.State>();
			while (stack.length > 0) {
				let { expanded, object } = stack.pop()!;
				let state = object.state;
				if (expanded) {
					states.push(state);
					continue;
				}
				if (visited.has(state)) {
					continue;
				}
				visited.add(state);
				if (state.stored) {
					continue;
				}
				if (state.storePromise !== null) {
					pending.add(state.storePromise);
					continue;
				}
				stack.push({ expanded: true, object });
				if (state.object !== null) {
					stack.push(
						...tg.Object.Object.children(state.object).map((object) => ({
							expanded: false,
							object,
						})),
					);
				}
			}

			// Wait for overlapping store promises and plan the batch again.
			if (pending.size > 0) {
				await Promise.all(pending);
				continue;
			}
			if (states.length === 0) {
				return;
			}

			// Claim the states and start the store promise.
			let promise = Promise.resolve().then(() => storeStates(states));
			for (let state of states) {
				state.startStorePromise(promise);
			}
			promise.then(
				() => states.forEach((state) => state.clearStorePromise(promise)),
				() => states.forEach((state) => state.clearStorePromise(promise)),
			);

			await promise;
			return;
		}
	};

	let storeStates = async (states: Array<tg.Object.State>): Promise<void> => {
		// Create the batch.
		let objects: Array<tg.Object.Batch.Object> = [];
		let stateGroupIndices = new Map<tg.Object.Id, number>();
		let stateGroups: Array<Array<tg.Object.State>> = [];
		for (let state of states) {
			if (state.object === null) {
				throw new Error("expected the object to be loaded");
			}
			let data = tg.Object.Data.withoutLocationAndTokens(
				tg.Object.Object.toData(state.object),
			);
			let id = tg.client.objectId(data);
			state.id = id;
			let children = tg.Object.Object.children(state.object).map(
				tg.Object.toReferent,
			);
			let object = { children, data, id };
			let stateGroupIndex = stateGroupIndices.get(id);
			if (stateGroupIndex === undefined) {
				stateGroupIndex = stateGroups.length;
				objects.push(object);
				stateGroupIndices.set(id, stateGroupIndex);
				stateGroups.push([]);
			} else {
				objects[stateGroupIndex] = object;
			}
			stateGroups[stateGroupIndex]!.push(state);
		}

		// Store the batch.
		let output = await tg.client.postObjectBatch({ objects });

		// Update the states.
		applyObjectBatchOutput(stateGroups, output);
	};

	let applyObjectBatchOutput = (
		stateGroups: Array<Array<tg.Object.State>>,
		output: tg.Object.Batch.Output,
	) => {
		if (stateGroups.length !== output.objects.length) {
			throw new Error("invalid object batch output");
		}
		for (let [index, node] of output.objects.entries()) {
			let states = stateGroups[index]!;
			if (states.length === 0) {
				throw new Error("invalid object batch output");
			}
			for (let state of states) {
				if (state.id !== node.node) {
					throw new Error("invalid object batch output");
				}
			}
		}
		for (let [index, node] of output.objects.entries()) {
			for (let state of stateGroups[index]!) {
				state.finishStore(node);
			}
		}
	};

	export type Data =
		| null
		| boolean
		| number
		| string
		| Array<tg.Value.Data>
		| { kind: "map"; value: { [key: string]: tg.Value.Data } }
		| { kind: "object"; value: string }
		| { kind: "bytes"; value: string }
		| { kind: "mutation"; value: tg.Mutation.Data }
		| { kind: "module"; value: tg.Module.Data }
		| { kind: "template"; value: tg.Template.Data }
		| { kind: "placeholder"; value: tg.Placeholder.Data };

	export namespace Data {
		export let children = (data: tg.Value.Data): Array<tg.Object.Id> => {
			if (
				data === null ||
				typeof data === "boolean" ||
				typeof data === "number" ||
				typeof data === "string"
			) {
				return [];
			} else if (data instanceof Array) {
				return data.flatMap(children);
			} else if (data.kind === "map") {
				return globalThis.Object.values(data.value).flatMap(children);
			} else if (data.kind === "object") {
				let referent = tg.Referent.fromDataString(
					data.value,
					(id) => id as tg.Object.Id,
				);
				return [referent.node];
			} else if (data.kind === "mutation") {
				return tg.Mutation.Data.children(data.value);
			} else if (data.kind === "module") {
				return tg.Module.Data.children(data.value);
			} else if (data.kind === "template") {
				return tg.Template.Data.children(data.value);
			} else {
				return [];
			}
		};

		export let withoutLocationAndTokens = (
			data: tg.Value.Data,
		): tg.Value.Data => {
			if (data instanceof Array) {
				return data.map(withoutLocationAndTokens);
			} else if (typeof data === "object" && data !== null) {
				if (data.kind === "map") {
					return {
						...data,
						value: globalThis.Object.fromEntries(
							globalThis.Object.entries(data.value).map(([key, value]) => [
								key,
								withoutLocationAndTokens(value),
							]),
						),
					};
				} else if (data.kind === "object") {
					let referent = tg.Referent.fromDataString(
						data.value,
						(id) => id as tg.Object.Id,
					);
					return {
						...data,
						value: tg.Referent.toDataString(
							tg.Referent.withoutLocationAndTokens(referent),
							(id) => id,
						),
					};
				} else if (data.kind === "mutation") {
					return {
						...data,
						value: tg.Mutation.Data.withoutLocationAndTokens(data.value),
					};
				} else if (data.kind === "module") {
					return {
						...data,
						value: tg.Module.Data.withoutLocationAndTokens(data.value),
					};
				} else if (data.kind === "template") {
					return {
						...data,
						value: tg.Template.Data.withoutLocationAndTokens(data.value),
					};
				}
				return { ...data };
			}
			return data;
		};
	}
}
