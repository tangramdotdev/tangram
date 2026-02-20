import ts from "typescript";
import type { Module } from "./module.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	range: Range;
	only: Array<string> | undefined;
};

export type Response = {
	actions: Array<CodeAction> | undefined;
};

export type CodeAction = {
	title: string;
	kind: string | undefined;
	edits: Array<TextEdit> | undefined;
};

export type TextEdit = {
	module: Module;
	range: Range;
	new_text: string;
};

export let handle = (request: Request): Response => {
	// Get the source file.
	let fileName = typescript.fileNameFromModule(request.module);
	let sourceFile = typescript.host.getSourceFile(
		fileName,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		throw new Error();
	}

	// Get the byte range.
	let start = ts.getPositionOfLineAndCharacter(
		sourceFile,
		request.range.start.line,
		request.range.start.character,
	);
	let end = ts.getPositionOfLineAndCharacter(
		sourceFile,
		request.range.end.line,
		request.range.end.character,
	);
	if (end < start) {
		end = start;
	}

	// Collect all code actions.
	let actions = [];

	if (shouldIncludeKind(request.only, "quickfix")) {
		actions.push(...getQuickFixActions(fileName, start, end));
	}

	if (shouldIncludeKind(request.only, "source.organizeImports")) {
		let organizeImportsAction = getOrganizeImportsAction(fileName);
		if (organizeImportsAction !== undefined) {
			actions.push(organizeImportsAction);
		}
	}

	// Deduplicate equivalent actions.
	let deduplicatedActions = deduplicateActions(actions);

	return {
		actions: deduplicatedActions.length === 0 ? undefined : deduplicatedActions,
	};
};

let shouldIncludeKind = (
	only: Array<string> | undefined,
	kind: string,
): boolean => {
	if (only === undefined || only.length === 0) {
		return true;
	}
	return only.some(
		(filterKind) => kind === filterKind || kind.startsWith(`${filterKind}.`),
	);
};

let getQuickFixActions = (
	fileName: string,
	start: number,
	end: number,
): Array<CodeAction> => {
	let diagnostics = [
		...typescript.languageService.getSyntacticDiagnostics(fileName),
		...typescript.languageService.getSemanticDiagnostics(fileName),
		...typescript.languageService.getSuggestionDiagnostics(fileName),
	];

	let actions = [];
	for (let diagnostic of diagnostics) {
		if (diagnostic.start === undefined || diagnostic.length === undefined) {
			continue;
		}

		let diagnosticStart = diagnostic.start;
		let diagnosticEnd = diagnostic.start + diagnostic.length;
		if (!spansOverlap(start, end, diagnosticStart, diagnosticEnd)) {
			continue;
		}

		let fixes = typescript.languageService.getCodeFixesAtPosition(
			fileName,
			diagnosticStart,
			diagnosticEnd,
			[diagnostic.code],
			{},
			{},
		);
		for (let fix of fixes) {
			let edits = convertFileTextChanges(fix.changes);
			if (edits.length === 0) {
				continue;
			}
			actions.push({
				title: fix.description,
				kind: "quickfix",
				edits,
			});
		}
	}

	return actions;
};

let getOrganizeImportsAction = (fileName: string): CodeAction | undefined => {
	let changes = typescript.languageService.organizeImports(
		{ type: "file", fileName },
		{},
		undefined,
	);
	let edits = convertFileTextChanges(changes);
	if (edits.length === 0) {
		return undefined;
	}
	return {
		title: "organize imports",
		kind: "source.organizeImports",
		edits,
	};
};

let spansOverlap = (
	aStart: number,
	aEnd: number,
	bStart: number,
	bEnd: number,
): boolean => {
	return aStart <= bEnd && bStart <= aEnd;
};

let convertFileTextChanges = (
	changes: ReadonlyArray<ts.FileTextChanges>,
): Array<TextEdit> => {
	let edits = [];
	for (let change of changes) {
		let sourceFile = typescript.host.getSourceFile(
			change.fileName,
			ts.ScriptTarget.ESNext,
		);
		if (sourceFile === undefined) {
			continue;
		}
		let module: Module;
		try {
			module = typescript.moduleFromFileName(change.fileName);
		} catch {
			continue;
		}
		for (let textChange of change.textChanges) {
			let start = ts.getLineAndCharacterOfPosition(
				sourceFile,
				textChange.span.start,
			);
			let end = ts.getLineAndCharacterOfPosition(
				sourceFile,
				textChange.span.start + textChange.span.length,
			);
			edits.push({
				module,
				range: { start, end },
				new_text: textChange.newText,
			});
		}
	}
	return edits;
};

let deduplicateActions = (actions: Array<CodeAction>): Array<CodeAction> => {
	let deduplicatedActions = [];
	let seen = new Set<string>();
	for (let action of actions) {
		let key = JSON.stringify([
			action.title,
			action.kind ?? null,
			action.edits ?? null,
		]);
		if (seen.has(key)) {
			continue;
		}
		seen.add(key);
		deduplicatedActions.push(action);
	}
	return deduplicatedActions;
};
