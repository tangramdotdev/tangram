import * as tg from "./index.ts";

export async function mutation<T extends tg.Value = tg.Value>(
	arg: tg.Unresolved<Mutation.Arg<T>>,
): Promise<Mutation<T>> {
	return await Mutation.new(arg);
}

export class Mutation<T extends tg.Value = tg.Value> {
	#inner: Mutation.Inner<T>;

	constructor(inner: tg.Mutation.Inner<T>) {
		this.#inner = inner;
	}

	static async new<T extends tg.Value = tg.Value>(
		arg_: tg.Unresolved<tg.Mutation.Arg<T>>,
	): Promise<Mutation<T>> {
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

	static async set<T extends tg.Value = tg.Value>(
		value: tg.Unresolved<T>,
	): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "set",
			value: await tg.resolve(value),
		}) as tg.Mutation<T>;
	}

	static unset(): tg.Mutation {
		return new tg.Mutation({ kind: "unset" });
	}

	static async setIfUnset<T extends tg.Value = tg.Value>(
		value: tg.Unresolved<T>,
	): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "set_if_unset",
			value: await tg.resolve(value),
		}) as tg.Mutation<T>;
	}

	static async prepend<T extends Array<tg.Value> = Array<tg.Value>>(
		values: tg.Unresolved<T>,
	): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "prepend",
			values: (await tg.resolve(values)) as Array<tg.Value>,
		}) as tg.Mutation<T>;
	}

	static async append<T extends Array<tg.Value> = Array<tg.Value>>(
		values: tg.Unresolved<T>,
	): Promise<tg.Mutation<T>> {
		return new tg.Mutation({
			kind: "append",
			values: (await tg.resolve(values)) as Array<tg.Value>,
		}) as tg.Mutation<T>;
	}

	static async prefix<T extends tg.Template.Arg = tg.Template.Arg>(
		template: tg.Unresolved<T>,
		separator?: string | undefined,
	): Promise<tg.Mutation<tg.Template>> {
		return new tg.Mutation({
			kind: "prefix",
			template: await tg.template(template),
			separator,
		});
	}

	static async suffix<T extends tg.Template.Arg = tg.Template.Arg>(
		template: tg.Unresolved<T>,
		separator?: string | undefined,
	): Promise<tg.Mutation<tg.Template>> {
		return new tg.Mutation({
			kind: "suffix",
			template: await tg.template(template),
			separator,
		});
	}

	static async merge<
		T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
	>(value: tg.Unresolved<T>): Promise<Mutation<T>> {
		return new tg.Mutation({
			kind: "merge",
			value: (await tg.resolve(value)) as { [key: string]: tg.Value },
		}) as tg.Mutation<T>;
	}

	static expect(value: unknown): Mutation {
		tg.assert(value instanceof Mutation);
		return value;
	}

	static assert(value: unknown): asserts value is Mutation {
		tg.assert(value instanceof Mutation);
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
			for (const innerKey in inner) {
				const mutation = inner[innerKey];
				if (!(mutation instanceof tg.Mutation)) {
					target[innerKey] = mutation;
				} else {
					await mutation.apply(target, innerKey);
				}
			}
		}
	}

	get inner() {
		return this.#inner;
	}
}

export namespace Mutation {
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

	export type Kind =
		| "set"
		| "unset"
		| "set_if_unset"
		| "prepend"
		| "append"
		| "prefix"
		| "suffix"
		| "merge";
}
