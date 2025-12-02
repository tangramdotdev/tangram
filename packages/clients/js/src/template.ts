import * as tg from "./index.ts";

export async function template(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): Promise<tg.Template>;
export async function template(
	...args: tg.Args<tg.Template.Arg>
): Promise<tg.Template>;
export async function template(
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Template.Arg>>,
	...args: tg.Args<tg.Template.Arg>
): Promise<tg.Template> {
	return await inner(false, firstArg, ...args);
}

async function inner(
	raw: boolean,
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Template.Arg>>,
	...args: tg.Args<tg.Template.Arg>
): Promise<tg.Template> {
	if (Array.isArray(firstArg) && "raw" in firstArg) {
		let strings = !raw ? unindent(firstArg) : firstArg;
		let placeholders = args as tg.Args<tg.Template>;
		let components = [];
		for (let i = 0; i < strings.length - 1; i++) {
			let string = strings[i]!;
			components.push(string);
			let placeholder = placeholders[i]!;
			components.push(placeholder);
		}
		components.push(strings[strings.length - 1]!);
		return await tg.Template.new(...components);
	} else {
		return await tg.Template.new(
			firstArg as tg.Unresolved<tg.Template.Arg>,
			...args,
		);
	}
}

export class Template {
	#components: Array<tg.Template.Component>;

	constructor(components: Array<tg.Template.Component>) {
		this.#components = components;
	}

	static async new(...args: tg.Args<tg.Template.Arg>): Promise<tg.Template> {
		let resolved = await Promise.all(args.map(tg.resolve));
		let components = (
			await Promise.all(
				resolved.map(async (arg) => {
					if (arg === undefined) {
						return [];
					} else if (
						typeof arg === "string" ||
						tg.Artifact.is(arg) ||
						arg instanceof tg.Placeholder
					) {
						return [arg];
					} else {
						return arg.components;
					}
				}),
			)
		).flat(1);
		let components_ = components.reduce<Array<tg.Template.Component>>(
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
		return new tg.Template(components_);
	}

	static expect(value: unknown): tg.Template {
		tg.assert(value instanceof tg.Template);
		return value;
	}

	static assert(value: unknown): asserts value is tg.Template {
		tg.assert(value instanceof tg.Template);
	}

	static toData(value: tg.Template): tg.Template.Data {
		return {
			components: value.components.map((component) => {
				if (typeof component === "string") {
					return { kind: "string", value: component };
				} else if (component instanceof tg.Placeholder) {
					return {
						kind: "placeholder",
						value: tg.Placeholder.toData(component),
					};
				} else {
					return { kind: "artifact", value: component.id };
				}
			}),
		};
	}

	static fromData(data: tg.Template.Data): tg.Template {
		return new tg.Template(
			data.components.map((component) => {
				if (component.kind === "string") {
					return component.value;
				} else if (component.kind === "placeholder") {
					return tg.Placeholder.fromData(component.value);
				} else {
					return tg.Artifact.withId(component.value);
				}
			}),
		);
	}

	objects(): Array<tg.Object> {
		return this.#components.flatMap((component) => {
			if (
				typeof component === "string" ||
				component instanceof tg.Placeholder
			) {
				return [];
			} else {
				return [component];
			}
		});
	}

	static async join(
		separator: tg.Unresolved<tg.Template.Arg>,
		...args: tg.Args<tg.Template.Arg>
	): Promise<tg.Template> {
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

	get components(): Array<tg.Template.Component> {
		return [...this.#components];
	}
}

export namespace Template {
	export type Arg = undefined | tg.Template.Component | tg.Template;

	export type Component = string | tg.Artifact | tg.Placeholder;

	export type Data = {
		components: Array<tg.Template.Data.Component>;
	};

	export namespace Data {
		export type Component =
			| { kind: "string"; value: string }
			| { kind: "artifact"; value: tg.Artifact.Id }
			| { kind: "placeholder"; value: tg.Placeholder.Data };
	}

	export let raw = async (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): Promise<tg.Template> => {
		return await inner(true, strings, ...placeholders);
	};
}

export let unindent = (strings: Array<string>): Array<string> => {
	// Concatenate the strings and collect the placeholder indices.
	let placeholderIndices: Array<number> = [];
	let string = strings[0]!;
	for (let i = 1; i < strings.length; i++) {
		placeholderIndices.push(string.length);
		string += strings[i];
	}

	// Split the string into lines.
	let lines = string.split("\n");

	// Compute the indentation.
	let position = 0;
	let indentation =
		Math.min(
			...lines
				.map((line, index) => {
					if (index === 0) {
						position += line.length + 1;
						return undefined;
					}
					let firstNonWhitespaceIndex: number | undefined;
					for (let i = 0; i < line.length; i++) {
						if (!(line[i] === " " || line[i] === "\t")) {
							firstNonWhitespaceIndex = i;
							break;
						}
					}
					let firstPlaceholderIndex = placeholderIndices.find(
						(index) => index >= position && index < position + line.length + 1,
					);
					if (firstPlaceholderIndex !== undefined) {
						firstPlaceholderIndex -= position;
					}
					let count: number | undefined;
					if (
						firstNonWhitespaceIndex === undefined &&
						firstPlaceholderIndex === undefined
					) {
						count = undefined;
					} else if (
						firstNonWhitespaceIndex !== undefined &&
						firstPlaceholderIndex === undefined
					) {
						count = firstNonWhitespaceIndex;
					} else if (
						firstNonWhitespaceIndex === undefined &&
						firstPlaceholderIndex !== undefined
					) {
						count = firstPlaceholderIndex;
					} else {
						count = Math.min(firstNonWhitespaceIndex!, firstPlaceholderIndex!);
					}
					position += line.length + 1;
					return count;
				})
				.filter((count) => count !== undefined),
		) ?? 0;

	// If the first line is empty, then remove it and update the placeholder indices.
	if (string[0] === "\n" && placeholderIndices[0] !== 0) {
		string = string.slice(1);
		lines = lines.slice(1);
		for (let i = 0; i < placeholderIndices.length; i++) {
			placeholderIndices[i]! -= 1;
		}
	}

	// Unindent each line and update the placeholder indices.
	position = 0;
	for (let i = 0; i < lines.length; i++) {
		let line = lines[i]!;
		let firstNonWhitespaceIndex: number | undefined;
		for (let i = 0; i < line.length; i++) {
			if (!(line[i] === " " || line[i] === "\t")) {
				firstNonWhitespaceIndex = i;
				break;
			}
		}
		let firstPlaceholderIndex = placeholderIndices.find(
			(index) => index >= position && index < position + line.length,
		);
		if (firstPlaceholderIndex !== undefined) {
			firstPlaceholderIndex -= position;
		}
		let remove = Math.min(
			indentation,
			line.length,
			firstNonWhitespaceIndex ?? Number.POSITIVE_INFINITY,
			firstPlaceholderIndex ?? Number.POSITIVE_INFINITY,
		);
		lines[i] = lines[i]!.slice(remove);
		for (let i = 0; i < placeholderIndices.length; i++) {
			if (placeholderIndices[i]! >= position) {
				placeholderIndices[i]! -= remove;
			}
		}
		position += lines[i]!.length + 1;
	}

	// Join the lines.
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
