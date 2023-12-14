import { Module } from "./module.ts";
import { Range } from "./range.ts";
import * as typescript from "./typescript.ts";
import ts from "typescript";

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
	selectionRange: Range;
	children: Array<Symbol> | null;
};

export type Kind =
	| "file"
	| "module"
	| "namespace"
	| "package"
	| "class"
	| "method"
	| "property"
	| "field"
	| "constructor"
	| "enum"
	| "interface"
	| "function"
	| "variable"
	| "constant"
	| "string"
	| "number"
	| "boolean"
	| "array"
	| "object"
	| "key"
	| "null"
	| "enumMember"
	| "event"
	| "operator"
	| "typeParameter";

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

	// Find the range of this symbol and its selectionRange.
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
		selectionRange: range,
		children: children ?? null,
	};
};

// Map the "kind" field from NavigationTree to a variant that is meaningful to LSP.
let getKind = (tsKind: string): Kind => {
	switch (tsKind) {
		case "script":
			return "file";
		case "module":
			return "module";
		case "class":
			return "class";
		case "local class":
			return "class";
		case "interface":
			return "interface";
		case "type":
			return "class";
		case "enum":
			return "enum";
		case "var":
			return "variable";
		case "local var":
			return "variable";
		case "function":
			return "function";
		case "local function":
			return "function";
		case "method":
			return "method";
		case "getter":
			return "method";
		case "setter":
			return "method";
		case "property":
			return "property";
		case "accessor":
			return "property";
		case "constructor":
			return "constructor";
		case "parameter":
			return "variable";
		case "type parameter":
			return "typeParameter";
		case "external module":
			return "module";
		default:
			return "variable";
	}
};
