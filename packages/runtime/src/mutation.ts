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

	static expect(value: unknown): Mutation {
		tg.assert(value instanceof Mutation);
		return value;
	}

	static assert(value: unknown): asserts value is Mutation {
		tg.assert(value instanceof Mutation);
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
		  };

	export type Kind =
		| "set"
		| "unset"
		| "set_if_unset"
		| "prepend"
		| "append"
		| "prefix"
		| "suffix";
}
