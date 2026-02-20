import ts from "typescript";
import type { Module } from "./module.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	query: string;
};

export type Response = {
	symbols: Array<Symbol> | undefined;
};

export type Symbol = {
	name: string;
	kind: string;
	module: Module;
	range: Range;
	containerName: string | undefined;
	deprecated: boolean | undefined;
};

export let handle = (request: Request): Response => {
	let items = typescript.languageService.getNavigateToItems(request.query);
	let symbols = items
		.map(convertNavigateToItem)
		.filter((symbol): symbol is Symbol => symbol !== undefined);

	return {
		symbols: symbols.length === 0 ? undefined : symbols,
	};
};

let convertNavigateToItem = (item: ts.NavigateToItem): Symbol | undefined => {
	let sourceFile = typescript.host.getSourceFile(
		item.fileName,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		return undefined;
	}
	let module: Module;
	try {
		module = typescript.moduleFromFileName(item.fileName);
	} catch {
		return undefined;
	}
	let start = ts.getLineAndCharacterOfPosition(sourceFile, item.textSpan.start);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		item.textSpan.start + item.textSpan.length,
	);
	let containerName = stringOrUndefined(item.containerName);
	let deprecated = item.kindModifiers
		.split(",")
		.map((modifier) => modifier.trim())
		.includes("deprecated");

	return {
		name: item.name,
		kind: item.kind,
		module,
		range: { start, end },
		containerName,
		deprecated: deprecated || undefined,
	};
};

let stringOrUndefined = (value: string): string | undefined => {
	if (value.length === 0) {
		return undefined;
	}
	return value;
};
