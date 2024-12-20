import type * as tg from "./index.ts";

export let flatten = <T>(value: MaybeNestedArray<T>): Array<T> => {
	if (value instanceof Array) {
		// @ts-ignore
		return value.flat(Number.POSITIVE_INFINITY);
	} else {
		return [value];
	}
};

export type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

export type MaybePromise<T> = T | Promise<T>;

export type MaybeMutation<T extends tg.Value = tg.Value> = T | tg.Mutation<T>;

export type MutationMap<
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
> = {
	[K in keyof T]?: tg.Mutation<T[K]>;
};

export type MaybeMutationMap<
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
> = {
	[K in keyof T]?: MaybeMutation<T[K]>;
};

export type ValueOrMaybeMutationMap<T extends tg.Value = tg.Value> = T extends
	| undefined
	| boolean
	| number
	| string
	| Object
	| Uint8Array
	| tg.Mutation
	| tg.Template
	| Array<infer _U extends tg.Value>
	? T
	: T extends { [key: string]: tg.Value }
		? MaybeMutationMap<T>
		: never;

export let trimMultilineString = (
	strings: TemplateStringsArray,
	...placeholders: Array<string>
): string => {
	// Concatenate the strings and placeholders.
	let string = "";
	let i = 0;
	while (i < placeholders.length) {
		string += strings[i];
		string += placeholders[i];
		i = i + 1;
	}
	string += strings[i];

	// Split the lines.
	let lines = string.split("\n");

	// Remove the first and last lines.
	if (lines.length > 2) {
		lines = lines.slice(1, -1);
	}

	// Get the number of leading tabs to remove.
	const leadingWhitespaceCount = Math.min(
		...lines
			.filter((line) => line.length > 0)
			.map((line) => line.search(/[^\t ]|$/)),
	);

	// Remove the leading tabs from each line and combine them with newlines.
	return lines.map((line) => line.slice(leadingWhitespaceCount)).join("\n");
};

export let trimTemplateStrings = (strings: Array<string>): Array<string> => {
	// Remove the trailing whitespace.
	const trailingRemoved = strings.map((str, index, arr) => {
		if (index === 0) {
			// Strip the first string's whitespace through the last '\n'.
			return str.replace(/^[ \t\n]*(?=\n)\n/g, "");
		} else if (index === arr.length - 1) {
			// Strip the last string's whitespace.
			return str.replace(/[ \t\n]*$/g, "");
		} else {
			// Strip trailing space in each line.
			return str.replace(/[ \t]*(?=\n)/gm, "");
		}
	});

	// Get the shortest leading whitespace length.
	const leadingWhitespaceCount = Math.min(
		...trailingRemoved
			.filter((str) => str.length > 0)
			.flatMap((str) => str.split("\n"))
			.filter((line) => line.length > 0)
			.map((line) => line.search(/[^\t ]|$/)),
	);

	// Cut the leading whitespace to size on each line.
	return trailingRemoved.map((str) =>
		str
			.replace(new RegExp(`^[ \t]{${leadingWhitespaceCount}}`, "g"), "")
			.replace(new RegExp(`\n[ \t]{${leadingWhitespaceCount}}`, "g"), "\n"),
	);
};
