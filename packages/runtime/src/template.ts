import * as tg from "./index.ts";
import { flatten, trimTemplateStrings } from "./util.ts";

export async function template(
	...args: tg.Args<Template.Arg>
): Promise<Template>;
export async function template(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<Template.Arg>
): Promise<Template>;
export async function template(...args: any): Promise<Template> {
	if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = trimTemplateStrings(args[0]);
		let placeholders = args.slice(1) as tg.Args<Template>;
		let components = [];
		for (let i = 0; i < strings.length - 1; i++) {
			let string = strings[i]!;
			components.push(string);
			let placeholder = placeholders[i]!;
			components.push(placeholder);
		}
		components.push(strings[strings.length - 1]!);
		return await Template.new(...components);
	} else {
		return await Template.new(...(args as tg.Args<Template>));
	}
}

export class Template {
	#components: Array<Template.Component>;

	constructor(components: Array<Template.Component>) {
		this.#components = components;
	}

	static async new(...args: tg.Args<Template.Arg>): Promise<Template> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let flattened = flatten(resolved);
		let components = (
			await Promise.all(
				flattened.map(async (arg) => {
					if (arg === undefined) {
						return [];
					} else if (typeof arg === "string" || tg.Artifact.is(arg)) {
						return [arg];
					} else {
						return arg.components;
					}
				}),
			)
		).flat(1);
		let components_ = components.reduce<Array<Template.Component>>(
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
		return new Template(components_);
	}

	static expect(value: unknown): Template {
		tg.assert(value instanceof Template);
		return value;
	}

	static assert(value: unknown): asserts value is Template {
		tg.assert(value instanceof Template);
	}

	/** Join an array of templates with a separator. */
	static async join(
		separator: tg.Unresolved<Template.Arg>,
		...args: tg.Args<Template.Arg>
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
			tg.assert(argTemplate);
			templates.push(argTemplate);
		}
		return template(...templates);
	}

	get components(): Array<Template.Component> {
		return [...this.#components];
	}
}

export namespace Template {
	export type Arg = undefined | Component | Template;

	export type Component = string | tg.Artifact;
}
