import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	positions: Array<Position>;
};

export type Response = {
	ranges: Array<SelectionRange> | undefined;
};

export type SelectionRange = {
	range: Range;
	parent: SelectionRange | undefined;
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

	// Convert each position to a smart selection range.
	let ranges = request.positions.map((position) => {
		let offset = ts.getPositionOfLineAndCharacter(
			sourceFile,
			position.line,
			position.character,
		);
		let selectionRange = typescript.languageService.getSmartSelectionRange(
			fileName,
			offset,
		);
		return convertSelectionRange(sourceFile, selectionRange);
	});

	return {
		ranges: ranges.length === 0 ? undefined : ranges,
	};
};

let convertSelectionRange = (
	sourceFile: ts.SourceFile,
	selectionRange: ts.SelectionRange,
): SelectionRange => {
	let start = ts.getLineAndCharacterOfPosition(
		sourceFile,
		selectionRange.textSpan.start,
	);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		selectionRange.textSpan.start + selectionRange.textSpan.length,
	);
	let parent =
		selectionRange.parent === undefined
			? undefined
			: convertSelectionRange(sourceFile, selectionRange.parent);

	return {
		range: { start, end },
		parent,
	};
};
