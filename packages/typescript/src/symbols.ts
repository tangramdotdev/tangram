import ts from "typescript";
import type { Module } from "./module.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
};

export type Response = {
	symbols: Array<Symbol> | null;
};

export type Symbol = {
	name: string;
	detail: string | null;
	kind: Kind;
	tags: Array<Tag>;
	range: Range;
	selection: Range;
	children: Array<Symbol> | null;
};

export type Kind =
	| "array"
	| "boolean"
	| "class"
	| "constant"
	| "constructor"
	| "enum"
	| "enum_member"
	| "event"
	| "field"
	| "file"
	| "function"
	| "interface"
	| "key"
	| "method"
	| "module"
	| "namespace"
	| "null"
	| "number"
	| "object"
	| "operator"
	| "package"
	| "property"
	| "string"
	| "type_parameter"
	| "variable";

export type Tag = "deprecated";

export let handle = (request: Request): Response => {
	// Get the module's filename and source.
	let fileName = typescript.fileNameFromModule(request.module);
	let sourceFile = typescript.host.getSourceFile(
		fileName,
		ts.ScriptTarget.ESNext,
	);

	if (sourceFile === undefined) {
		throw new Error();
	}

	// Get the navigation tree for this file.
	let navigationTree = typescript.languageService.getNavigationTree(fileName);

	// Get the symbols by walking the navigation tree.
	let root = walk(sourceFile, navigationTree);

	// Drop the root and return the symbols directly. The item returned by getNavigationTree is guaranteed to be a module.
	return { symbols: root.children };
};

export let walk = (
	sourceFile: ts.SourceFile,
	tree: ts.NavigationTree,
): Symbol => {
	let name = tree.text;

	// Find the range of this symbol and its selection range.
	let startPosition = Math.min(...tree.spans.map((span) => span.start));
	let endPosition = Math.max(
		...tree.spans.map((span) => span.start + span.length),
	);

	// Convert text spans to ranges.
	let range = {
		start: ts.getLineAndCharacterOfPosition(sourceFile, startPosition),
		end: ts.getLineAndCharacterOfPosition(sourceFile, endPosition),
	};

	// Parse the symbol kind from the nav tree.
	let kind = getKind(tree.kind);

	// Collect the nested children.
	let children = tree.childItems?.map((child) => walk(sourceFile, child));

	return {
		name,
		kind,
		tags: [],
		detail: null,
		range,
		selection: range,
		children: children ?? null,
	};
};

let getKind = (tsKind: string): Kind => {
	switch (tsKind) {
		case "accessor":
			return "property";
		case "class":
			return "class";
		case "constructor":
			return "constructor";
		case "enum":
			return "enum";
		case "external module":
			return "module";
		case "function":
			return "function";
		case "getter":
			return "method";
		case "interface":
			return "interface";
		case "local class":
			return "class";
		case "local function":
			return "function";
		case "local var":
			return "variable";
		case "method":
			return "method";
		case "module":
			return "module";
		case "parameter":
			return "variable";
		case "property":
			return "property";
		case "script":
			return "file";
		case "setter":
			return "method";
		case "type parameter":
			return "type_parameter";
		case "type":
			return "class";
		case "var":
			return "variable";
		default:
			return "variable";
	}
};
