import * as tg from "./index.ts";

/** Create a template. */
export function template(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.Template.Builder;
export function template(
	...args: tg.Args<tg.Template.Arg>
): tg.Template.Builder;
export function template(
	firstArg:
		| TemplateStringsArray
		| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Template.Arg>>,
	...args: tg.Args<tg.Template.Arg>
): tg.Template.Builder {
	return new tg.Template.Builder(firstArg, ...args);
}

async function joinTemplate(
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
	return await template(...templates);
}

/** A template. */
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

	/** Expect that a value is a `tg.Template`. */
	static expect(value: unknown): tg.Template {
		tg.assert(value instanceof tg.Template);
		return value;
	}

	/** Assert that a value is a `tg.Template`. */
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

	/** Join an array of templates with a separator. */
	static join(
		separator: tg.Unresolved<tg.Template.Arg>,
		...args: tg.Args<tg.Template.Arg>
	): tg.Template.Builder {
		return tg.Template.Builder.join(separator, ...args);
	}

	/** Get this template's components. */
	get components(): Array<tg.Template.Component> {
		return [...this.#components];
	}
}

export namespace Template {
	export class Builder {
		#args: tg.Args<tg.Template.Arg>;
		#create: (...args: Array<any>) => Promise<tg.Template>;

		constructor(
			raw: boolean,
			strings: TemplateStringsArray,
			...placeholders: tg.Args<tg.Template.Arg>
		);
		constructor(
			strings: TemplateStringsArray,
			...placeholders: tg.Args<tg.Template.Arg>
		);
		constructor(
			firstArg:
				| TemplateStringsArray
				| tg.Unresolved<tg.ValueOrMaybeMutationMap<tg.Template.Arg>>,
			...args: tg.Args<tg.Template.Arg>
		);
		constructor(...args: tg.Args<tg.Template.Arg>);
		constructor(...args: any[]) {
			let raw = false;
			if (typeof args[0] === "boolean") {
				raw = args[0];
				args = args.slice(1);
			}
			let firstArg = args[0];
			if (Array.isArray(firstArg) && "raw" in firstArg) {
				let strings = firstArg as TemplateStringsArray;
				let strings_ = !raw ? unindent([...strings]) : strings;
				let placeholders = args.slice(1) as tg.Args<tg.Template.Arg>;
				let components = [];
				for (let i = 0; i < strings_.length - 1; i++) {
					let string = strings_[i]!;
					components.push(string);
					let placeholder = placeholders[i]!;
					components.push(placeholder);
				}
				components.push(strings_[strings_.length - 1]!);
				this.#args = components;
			} else {
				this.#args = args;
			}
			this.#create = tg.Template.new;
		}

		static join(
			separator: tg.Unresolved<tg.Template.Arg>,
			...args: tg.Args<tg.Template.Arg>
		): tg.Template.Builder {
			let builder = new tg.Template.Builder();
			builder.#args = [separator, ...args];
			builder.#create = joinTemplate;
			return builder;
		}

		then<TResult1 = tg.Template, TResult2 = never>(
			onfulfilled?:
				| ((value: tg.Template) => TResult1 | PromiseLike<TResult1>)
				| undefined
				| null,
			onrejected?:
				| ((reason: any) => TResult2 | PromiseLike<TResult2>)
				| undefined
				| null,
		): PromiseLike<TResult1 | TResult2> {
			return this.#create(...this.#args).then(onfulfilled, onrejected);
		}
	}

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

		export let children = (data: tg.Template.Data): Array<tg.Object.Id> => {
			return data.components.flatMap((component) => {
				if (component.kind === "artifact") {
					return [component.value];
				} else {
					return [];
				}
			});
		};
	}

	/** A tagged template function that behaves identically to `tg.template` except that it does not trim leading whitespace. */
	export let raw = (
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Template.Builder => {
		return new tg.Template.Builder(true, strings, ...placeholders);
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
