import * as tg from "./index.ts";

/** Create a mutation. */
export function mutation<T extends tg.Value = tg.Value>(
	arg: tg.Unresolved<tg.Mutation.Arg<T>>,
): tg.Mutation.Builder<T> {
	return new tg.Mutation.Builder(arg);
}

export class Mutation<T extends tg.Value = tg.Value> {
	#inner: tg.Mutation.Inner<T>;

	constructor(inner: tg.Mutation.Inner<T>) {
		this.#inner = inner;
	}

	/** Create a mutation. */
	static async new<T extends tg.Value = tg.Value>(
		arg_: tg.Unresolved<tg.Mutation.Arg<T>>,
	): Promise<tg.Mutation<T>> {
		let arg = await tg.resolve(arg_);
		if (arg.kind === "set") {
			return new tg.Mutation({
				kind: "set",
				value: arg.value,
			}) as tg.Mutation<T>;
		} else if (arg.kind === "unset") {
			return new tg.Mutation({ kind: "unset" });
		} else if (arg.kind === "set_if_unset") {
			return new tg.Mutation({
				kind: "set_if_unset",
				value: arg.value,
			}) as tg.Mutation<T>;
		} else if (arg.kind === "prepend" || arg.kind === "append") {
			return new tg.Mutation({
				kind: arg.kind,
				values: arg.values as Array<tg.Value>,
			}) as tg.Mutation<T>;
		} else if (arg.kind === "prefix" || arg.kind === "suffix") {
			return new tg.Mutation({
				kind: arg.kind,
				template: await tg.template(arg.template),
				separator: arg.separator,
			}) as tg.Mutation<T>;
		} else if (arg.kind === "merge") {
			return new tg.Mutation({
				kind: arg.kind,
				value: arg.value as { [key: string]: tg.Value },
			}) as tg.Mutation<T>;
		} else {
			return tg.unreachable("invalid kind");
		}
	}

	/** Create a set mutation. */
	static async set<T extends tg.Unresolved<tg.Value>>(
		value: T,
	): Promise<tg.Mutation<tg.Resolved<T>>> {
		return new tg.Mutation<tg.Resolved<T>>({
			kind: "set",
			value: await tg.resolve(value),
		});
	}

	/** Create an unset mutation. */
	static unset<T extends tg.Value = tg.Value>(): tg.Mutation<T> {
		return new tg.Mutation({ kind: "unset" }) as tg.Mutation<T>;
	}

	/** Create a set if unset mutation. */
	static async setIfUnset<T extends tg.Unresolved<tg.Value>>(
		value: T,
	): Promise<tg.Mutation<tg.Resolved<T>>> {
		return new tg.Mutation<tg.Resolved<T>>({
			kind: "set_if_unset",
			value: await tg.resolve(value),
		});
	}

	/** Create an prepend mutation. */
	static async prepend<T extends Array<tg.Value> = Array<tg.Value>>(
		values: tg.Unresolved<T>,
	): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "prepend",
			values: (await tg.resolve(values)) as Array<tg.Value>,
		}) as tg.Mutation<T>;
	}

	/** Create an append mutation. */
	static async append<T extends Array<tg.Value> = Array<tg.Value>>(
		values: tg.Unresolved<T>,
	): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "append",
			values: (await tg.resolve(values)) as Array<tg.Value>,
		}) as tg.Mutation<T>;
	}

	/** Create a prefix mutation. */
	static async prefix(
		template: tg.Unresolved<tg.Template.Arg>,
		separator?: string | undefined,
	): Promise<tg.Mutation<tg.Template>> {
		return new tg.Mutation({
			kind: "prefix",
			template: await tg.template(template),
			separator,
		});
	}

	/** Create a suffix mutation. */
	static async suffix(
		template: tg.Unresolved<tg.Template.Arg>,
		separator?: string | undefined,
	): Promise<tg.Mutation<tg.Template>> {
		return new tg.Mutation({
			kind: "suffix",
			template: await tg.template(template),
			separator,
		});
	}

	/** Create a merge mutation. */
	static async merge<
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	>(value: tg.Unresolved<T>): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "merge",
			value: (await tg.resolve(value)) as { [key: string]: tg.Value },
		}) as tg.Mutation<T>;
	}

	static expect(value: unknown): tg.Mutation {
		tg.assert(value instanceof tg.Mutation);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Mutation {
		tg.assert(value instanceof tg.Mutation);
	}

	static toData<T extends tg.Value = tg.Value>(
		value: tg.Mutation<T>,
	): tg.Mutation.Data {
		if (value.inner.kind === "unset") {
			return { kind: "unset" };
		} else if (value.inner.kind === "set") {
			return {
				kind: "set",
				value: tg.Value.toData(value.inner.value),
			};
		} else if (value.inner.kind === "set_if_unset") {
			return {
				kind: "set_if_unset",
				value: tg.Value.toData(value.inner.value),
			};
		} else if (
			value.inner.kind === "prepend" ||
			value.inner.kind === "append"
		) {
			return {
				kind: value.inner.kind,
				values: value.inner.values.map(tg.Value.toData),
			};
		} else if (value.inner.kind === "prefix" || value.inner.kind === "suffix") {
			let output: {
				kind: "prefix" | "suffix";
				template: tg.Template.Data;
				separator?: string;
			} = {
				kind: value.inner.kind,
				template: tg.Template.toData(value.inner.template),
			};
			if (value.inner.separator !== undefined) {
				output.separator = value.inner.separator;
			}
			return output;
		} else if (value.inner.kind === "merge") {
			return {
				kind: "merge",
				value: Object.fromEntries(
					Object.entries(value.inner.value).map(([key, value]) => [
						key,
						tg.Value.toData(value),
					]),
				),
			};
		} else {
			return tg.unreachable("invalid kind");
		}
	}

	static fromData<T extends tg.Value = tg.Value>(
		data: tg.Mutation.Data,
	): tg.Mutation<T> {
		if (data.kind === "unset") {
			return new tg.Mutation({ kind: "unset" }) as tg.Mutation<T>;
		} else if (data.kind === "set") {
			return new tg.Mutation({
				kind: "set",
				value: tg.Value.fromData(data.value),
			}) as tg.Mutation<T>;
		} else if (data.kind === "set_if_unset") {
			return new tg.Mutation({
				kind: "set_if_unset",
				value: tg.Value.fromData(data.value),
			}) as tg.Mutation<T>;
		} else if (data.kind === "prepend" || data.kind === "append") {
			return new tg.Mutation({
				kind: data.kind,
				values: data.values.map(tg.Value.fromData),
			}) as tg.Mutation<T>;
		} else if (data.kind === "prefix" || data.kind === "suffix") {
			return new tg.Mutation({
				kind: data.kind,
				template: tg.Template.fromData(data.template),
				separator: data.separator,
			}) as tg.Mutation<T>;
		} else if (data.kind === "merge") {
			return new tg.Mutation({
				kind: "merge",
				value: Object.fromEntries(
					Object.entries(data.value).map(([key, value]) => [
						key,
						tg.Value.fromData(value),
					]),
				),
			}) as tg.Mutation<T>;
		} else {
			return tg.unreachable("invalid kind");
		}
	}

	get inner() {
		return this.#inner;
	}

	objects(): Array<tg.Object> {
		if (this.#inner.kind === "set") {
			return tg.Value.objects(this.#inner.value);
		} else if (this.#inner.kind === "set_if_unset") {
			return tg.Value.objects(this.#inner.value);
		} else if (
			this.#inner.kind === "prepend" ||
			this.#inner.kind === "append"
		) {
			return tg.Value.objects(this.#inner.values);
		} else if (this.#inner.kind === "prefix" || this.#inner.kind === "suffix") {
			return this.#inner.template.objects();
		} else {
			return [];
		}
	}

	async apply(map: { [key: string]: tg.Value }, key: string): Promise<void> {
		if (this.#inner.kind === "unset") {
			delete map[key];
		} else if (this.#inner.kind === "set") {
			map[key] = this.#inner.value;
		} else if (this.#inner.kind === "set_if_unset") {
			if (!(key in map)) {
				map[key] = this.#inner.value;
			}
		} else if (this.#inner.kind === "prepend") {
			if (!(key in map) || map[key] === undefined) {
				map[key] = [];
			}
			let array = map[key];
			tg.assert(array instanceof Array);
			map[key] = [...this.#inner.values, ...array];
		} else if (this.#inner.kind === "append") {
			if (!(key in map) || map[key] === undefined) {
				map[key] = [];
			}
			let array = map[key];
			tg.assert(array instanceof Array);
			map[key] = [...array, ...this.#inner.values];
		} else if (this.#inner.kind === "prefix") {
			if (!(key in map)) {
				map[key] = await tg.template();
			}
			let value = map[key];
			tg.assert(
				value === undefined ||
					typeof value === "string" ||
					tg.Artifact.is(value) ||
					value instanceof tg.Template,
			);
			map[key] = await tg.Template.join(
				this.#inner.separator,
				this.#inner.template,
				value,
			);
		} else if (this.#inner.kind === "suffix") {
			if (!(key in map)) {
				map[key] = await tg.template();
			}
			let value = map[key];
			tg.assert(
				value === undefined ||
					typeof value === "string" ||
					tg.Artifact.is(value) ||
					value instanceof tg.Template,
			);
			map[key] = await tg.Template.join(
				this.#inner.separator,
				value,
				this.#inner.template,
			);
		} else if (this.#inner.kind === "merge") {
			if (!(key in map) || map[key] === undefined) {
				map[key] = {};
			}
			let target = map[key];
			tg.assert(tg.Value.isMap(target));
			let inner = this.#inner.value;
			for (let innerKey in inner) {
				let mutation = inner[innerKey];
				if (!(mutation instanceof tg.Mutation)) {
					target[innerKey] = mutation;
				} else {
					await mutation.apply(target, innerKey);
				}
			}
		}
	}
}

export namespace Mutation {
	export class Builder<T extends tg.Value = tg.Value> {
		#arg: tg.Unresolved<tg.Mutation.Arg<T>>;

		constructor(arg: tg.Unresolved<tg.Mutation.Arg<T>>) {
			this.#arg = arg;
		}

		then<TResult1 = tg.Mutation<T>, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Mutation<T>) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return tg.Mutation.new(this.#arg).then(onfulfilled, onrejected);
		}
	}

	export type Kind =
		| "set"
		| "unset"
		| "set_if_unset"
		| "prepend"
		| "append"
		| "prefix"
		| "suffix"
		| "merge";

	export type Arg<T extends tg.Value = tg.Value> =
		| { kind: "unset" }
		| { kind: "set"; value: T }
		| { kind: "set_if_unset"; value: T }
		| {
				kind: "prepend";
				values: T extends Array<infer _U> ? T : never;
		  }
		| {
				kind: "append";
				values: T extends Array<infer _U> ? T : never;
		  }
		| {
				kind: "prefix";
				template: T extends tg.Template ? T : never;
				separator?: string | undefined;
		  }
		| {
				kind: "suffix";
				template: T extends tg.Template ? T : never;
				separator?: string | undefined;
		  }
		| {
				kind: "merge";
				value: T extends { [key: string]: tg.Value } ? T : never;
		  };

	export type Inner<T extends tg.Value = tg.Value> =
		| { kind: "unset" }
		| { kind: "set"; value: T }
		| { kind: "set_if_unset"; value: T }
		| {
				kind: "prepend";
				values: T extends Array<infer _U> ? T : never;
		  }
		| {
				kind: "append";
				values: T extends Array<infer _U> ? T : never;
		  }
		| {
				kind: "prefix";
				template: T extends tg.Template ? T : never;
				separator: string | undefined;
		  }
		| {
				kind: "suffix";
				template: T extends tg.Template ? T : never;
				separator: string | undefined;
		  }
		| {
				kind: "merge";
				value: T extends { [key: string]: tg.Value } ? T : never;
		  };

	export type Data =
		| { kind: "unset" }
		| { kind: "set"; value: tg.Value.Data }
		| { kind: "set_if_unset"; value: tg.Value.Data }
		| {
				kind: "prepend";
				values: Array<tg.Value.Data>;
		  }
		| {
				kind: "append";
				values: Array<tg.Value.Data>;
		  }
		| {
				kind: "prefix";
				template: tg.Template.Data;
				separator?: string;
		  }
		| {
				kind: "suffix";
				template: tg.Template.Data;
				separator?: string;
		  }
		| {
				kind: "merge";
				value: { [key: string]: tg.Value.Data };
		  };

	export namespace Data {
		export let children = (data: tg.Mutation.Data): Array<tg.Object.Id> => {
			switch (data.kind) {
				case "unset": {
					return [];
				}
				case "set":
				case "set_if_unset": {
					return tg.Value.Data.children(data.value);
				}
				case "prepend":
				case "append": {
					return data.values.flatMap(tg.Value.Data.children);
				}
				case "prefix":
				case "suffix": {
					return tg.Template.Data.children(data.template);
				}
				case "merge": {
					return globalThis.Object.values(data.value).flatMap(
						tg.Value.Data.children,
					);
				}
			}
		};
	}
}
