import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert as assert_, unreachable } from "./assert.ts";
import { mutation } from "./mutation.ts";
import type { Unresolved } from "./resolve.ts";

export let template = (...args: Args<Template.Arg>): Promise<Template> => {
	return Template.new(...args);
};

export class Template {
	#components: Array<Template.Component>;

	constructor(components: Array<Template.Component>) {
		this.#components = components;
	}

	static async new(...args: Args<Template.Arg>): Promise<Template> {
		type Apply = {
			components: Array<Template.Component>;
		};
		let { components } = await Args.apply<Template.Arg, Apply>(
			args,
			async (arg) => {
				if (arg === undefined) {
					return {};
				} else if (typeof arg === "string" || Artifact.is(arg)) {
					return {
						components: await mutation({
							kind: "array_append",
							values: [arg],
						}),
					};
				} else if (Template.is(arg)) {
					return {
						components: await mutation({
							kind: "array_append",
							values: arg.components,
						}),
					};
				} else {
					return unreachable();
				}
			},
		);

		// Normalize the components.
		components = (components ?? []).reduce<Array<Template.Component>>(
			(components, component) => {
				let lastComponent = components.at(-1);
				if (component === "") {
					// Ignore empty string components.
				} else if (
					typeof lastComponent === "string" &&
					typeof component === "string"
				) {
					// Merge adjacent string components.
					components.splice(-1, 1, lastComponent + component);
				} else {
					components.push(component);
				}
				return components;
			},
			[],
		);

		return new Template(components);
	}

	static is(value: unknown): value is Template {
		return value instanceof Template;
	}

	static expect(value: unknown): Template {
		assert_(Template.is(value));
		return value;
	}

	static assert(value: unknown): asserts value is Template {
		assert_(Template.is(value));
	}

	/** Join an array of templates with a separator. */
	static async join(
		separator: Unresolved<Template.Arg>,
		...args: Array<Unresolved<Template.Arg>>
	): Promise<Template> {
		let separatorTemplate = await template(separator);
		let argTemplates = await Promise.all(args.map((arg) => template(arg)));
		argTemplates = argTemplates.filter((arg) => arg.components.length > 0);
		let templates = [];
		for (let i = 0; i < argTemplates.length; i++) {
			if (i > 0) {
				templates.push(separatorTemplate);
			}
			let argTemplate = argTemplates[i];
			assert_(argTemplate);
			templates.push(argTemplate);
		}
		return template(...templates);
	}

	get components(): Array<Template.Component> {
		return this.#components;
	}
}

export namespace Template {
	export type Arg = undefined | Component | Template;

	export type Component = string | Artifact;
}
