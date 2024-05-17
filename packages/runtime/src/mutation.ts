import { assert as assert_ } from "./assert.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import { type Template, template } from "./template.ts";
import { type MaybeNestedArray, flatten } from "./util.ts";
import type { Value } from "./value.ts";

export async function mutation<T extends Value = Value>(
	arg: Unresolved<Mutation.Arg<T>>,
): Promise<Mutation<T>> {
	return await Mutation.new(arg);
}

export class Mutation<T extends Value = Value> {
	#inner: Mutation.Inner;

	constructor(inner: Mutation.Inner) {
		this.#inner = inner;
	}

	static async new<T extends Value = Value>(
		unresolved: Unresolved<Mutation.Arg<T>>,
	): Promise<Mutation<T>> {
		let arg = await resolve(unresolved);
		if (arg.kind === "prepend" || arg.kind === "append") {
			return new Mutation({ kind: arg.kind, values: flatten(arg.values) });
		} else if (arg.kind === "prefix" || arg.kind === "suffix") {
			return new Mutation({
				kind: arg.kind,
				template: await template(arg.template),
				separator: arg.separator,
			});
		} else if (arg.kind === "unset") {
			return new Mutation({ kind: "unset" });
		} else {
			return new Mutation({ kind: arg.kind, value: arg.value });
		}
	}

	static set<T extends Value = Value>(
		value: Unresolved<T>,
	): Promise<Mutation<T>> {
		return Mutation.new({ kind: "set", value } as any);
	}

	static unset(): Mutation {
		return new Mutation({ kind: "unset" });
	}

	static setIfUnset<T extends Value = Value>(
		value: Unresolved<T>,
	): Promise<Mutation<T>> {
		return Mutation.new({ kind: "set_if_unset", value } as any);
	}

	static prepend<T extends Value = Value>(
		values: Unresolved<MaybeNestedArray<T>>,
	): Promise<Mutation<Array<T>>> {
		return Mutation.new({
			kind: "prepend",
			values,
		} as any);
	}

	static append<T extends Value = Value>(
		values: Unresolved<MaybeNestedArray<T>>,
	): Promise<Mutation<Array<T>>> {
		return Mutation.new({
			kind: "append",
			values,
		} as any);
	}

	static prefix(
		template: Unresolved<Template.Arg>,
		separator?: string | undefined,
	): Promise<Mutation<Template>> {
		return Mutation.new({
			kind: "prefix",
			template,
			separator,
		});
	}

	static suffix(
		template: Unresolved<Template.Arg>,
		separator?: string | undefined,
	): Promise<Mutation<Template>> {
		return Mutation.new({
			kind: "suffix",
			template,
			separator,
		});
	}

	static expect(value: unknown): Mutation {
		assert_(value instanceof Mutation);
		return value;
	}

	static assert(value: unknown): asserts value is Mutation {
		assert_(value instanceof Mutation);
	}

	get inner() {
		return this.#inner;
	}
}

export namespace Mutation {
	export type Arg<T extends Value = Value> =
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
				template: T extends Template ? Template.Arg : never;
				separator?: string | undefined;
		  }
		| {
				kind: "suffix";
				template: T extends Template ? Template.Arg : never;
				separator?: string | undefined;
		  };

	export type Inner =
		| { kind: "unset" }
		| { kind: "set"; value: Value }
		| { kind: "set_if_unset"; value: Value }
		| {
				kind: "prepend";
				values: Array<Value>;
		  }
		| {
				kind: "append";
				values: Array<Value>;
		  }
		| {
				kind: "prefix";
				template: Template;
				separator: string | undefined;
		  }
		| {
				kind: "suffix";
				template: Template;
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
