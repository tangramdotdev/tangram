import * as tg from "./index.ts";
import { type MaybeNestedArray, flatten } from "./util.ts";

export async function mutation<T extends tg.Value = tg.Value>(
	arg: tg.Unresolved<Mutation.Arg<T>>,
): Promise<Mutation<T>> {
	return await Mutation.new(arg);
}

export class Mutation<T extends tg.Value = tg.Value> {
	#inner: Mutation.Inner;

	constructor(inner: Mutation.Inner) {
		this.#inner = inner;
	}

	static async new<T extends tg.Value = tg.Value>(
		unresolved: tg.Unresolved<Mutation.Arg<T>>,
	): Promise<Mutation<T>> {
		let arg = await tg.resolve(unresolved);
		if (arg.kind === "prepend" || arg.kind === "append") {
			return new Mutation({ kind: arg.kind, values: flatten(arg.values) });
		} else if (arg.kind === "prefix" || arg.kind === "suffix") {
			return new Mutation({
				kind: arg.kind,
				template: await tg.template(arg.template),
				separator: arg.separator,
			});
		} else if (arg.kind === "merge") {
			return new Mutation({
				kind: arg.kind,
				value: arg.value,
			});
		} else if (arg.kind === "unset") {
			return new Mutation({ kind: "unset" });
		} else {
			return new Mutation({ kind: arg.kind, value: arg.value });
		}
	}

	static set<T extends tg.Value = tg.Value>(
		value: tg.Unresolved<T>,
	): Promise<Mutation<T>> {
		return Mutation.new({ kind: "set", value } as any);
	}

	static unset(): Mutation {
		return new Mutation({ kind: "unset" });
	}

	static setIfUnset<T extends tg.Value = tg.Value>(
		value: tg.Unresolved<T>,
	): Promise<Mutation<T>> {
		return Mutation.new({ kind: "set_if_unset", value } as any);
	}

	static prepend<T extends tg.Value = tg.Value>(
		values: tg.Unresolved<MaybeNestedArray<T>>,
	): Promise<Mutation<Array<T>>> {
		return Mutation.new({
			kind: "prepend",
			values,
		} as any);
	}

	static append<T extends tg.Value = tg.Value>(
		values: tg.Unresolved<MaybeNestedArray<T>>,
	): Promise<Mutation<Array<T>>> {
		return Mutation.new({
			kind: "append",
			values,
		} as any);
	}

	static prefix(
		template: tg.Unresolved<tg.Template.Arg>,
		separator?: string | undefined,
	): Promise<Mutation<tg.Template>> {
		return Mutation.new({
			kind: "prefix",
			template,
			separator,
		});
	}

	static suffix(
		template: tg.Unresolved<tg.Template.Arg>,
		separator?: string | undefined,
	): Promise<Mutation<tg.Template>> {
		return Mutation.new({
			kind: "suffix",
			template,
			separator,
		});
	}

	static merge(value: { [key: string]: tg.Value }): Promise<
		Mutation<{ [key: string]: tg.Value }>
	> {
		return Mutation.new({
			kind: "merge",
			value,
		});
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
			map[key] = [...flatten(this.#inner.values), ...array];
		} else if (this.#inner.kind === "append") {
			if (!(key in map) || map[key] === undefined) {
				map[key] = [];
			}
			let array = map[key];
			tg.assert(array instanceof Array);
			map[key] = [...array, ...flatten(this.#inner.values)];
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
				values: T extends Array<infer U> ? MaybeNestedArray<U> : never;
		  }
		| {
				kind: "append";
				values: T extends Array<infer U> ? MaybeNestedArray<U> : never;
		  }
		| {
				kind: "prefix";
				template: T extends tg.Template ? tg.Template.Arg : never;
				separator?: string | undefined;
		  }
		| {
				kind: "suffix";
				template: T extends tg.Template ? tg.Template.Arg : never;
				separator?: string | undefined;
		  }
		| {
				kind: "merge";
				value: T extends { [key: string]: tg.Value } ? T : never;
		  };

	export type Inner =
		| { kind: "unset" }
		| { kind: "set"; value: tg.Value }
		| { kind: "set_if_unset"; value: tg.Value }
		| {
				kind: "prepend";
				values: Array<tg.Value>;
		  }
		| {
				kind: "append";
				values: Array<tg.Value>;
		  }
		| {
				kind: "prefix";
				template: tg.Template;
				separator: string | undefined;
		  }
		| {
				kind: "suffix";
				template: tg.Template;
				separator: string | undefined;
		  }
		| {
				kind: "merge";
				value: { [key: string]: tg.Value };
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
