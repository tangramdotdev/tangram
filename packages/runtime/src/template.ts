import * as tg from "./index.ts";
import { flatten } from "./util.ts";

export async function template(
	...args: tg.Args<Template.Arg>
): Promise<Template>;
export async function template(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<Template.Arg>
): Promise<Template>;
export async function template(...args: any): Promise<Template> {
	return await templateInner(false, ...args);
}

async function templateInner(raw: boolean, ...args: any): Promise<Template> {
	if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = !raw ? unindent(args[0]) : args[0];
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

	export let raw = async (...args: any): Promise<Template> => {
		return await templateInner(true, ...args);
	};
}

let unindent = (strings: Array<string>): Array<string> => {
	// Concatenate the strings and collect the placeholder indices.
	let placeholderIndices: Array<number> = [];
	let string = strings[0]!;
	for (let i = 1; i < strings.length; i++) {
		placeholderIndices.push(string.length);
		string += strings[i];
	}

	// If the string starts with a newline, then remove it and update the placeholder indices.
	if (string.startsWith("\n")) {
		string = string.slice(1);
		for (let i = 0; i < placeholderIndices.length; i++) {
			placeholderIndices[i]! -= 1;
		}
	}

	// Split the string into lines.
	let lines = string.split("\n");

	// Compute the indentation.
	let indentation = Math.min(
		...lines
			.filter((line) => line.trim().length > 0)
			.map(countLeadingWhitespace),
	);
	if (indentation === Number.POSITIVE_INFINITY) {
		indentation = 0;
	}

	// Unindent each line and update the placeholder indices.
	let position = 0;
	for (let i = 0; i < lines.length; i++) {
		let lineIndentation = Math.min(
			indentation,
			countLeadingWhitespace(lines[i]!) ?? 0,
		);
		lines[i] = lines[i]!.slice(lineIndentation);
		for (let j = 0; j < placeholderIndices.length; j++) {
			if (placeholderIndices[j]! > position) {
				placeholderIndices[j]! -= lineIndentation;
			}
		}
		position += lines[i]!.length + 1;
	}
	string = lines.join("\n");

	// Split the string at the placeholder indices.
	let output: Array<string> = [];
	let index = 0;
	for (let i = 0; i < placeholderIndices.length; i++) {
		let nextIndex = placeholderIndices[i]!;
		output.push(string.slice(index, nextIndex));
		index = nextIndex;
	}
	output.push(string.slice(index));

	return output;
};

let countLeadingWhitespace = (line: string): number => {
	for (let i = 0; i < line.length; i++) {
		if (!(line[i] === " " || line[i] === "\t")) {
			return i;
		}
	}
	return line.length;
};
