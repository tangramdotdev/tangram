import ts from "typescript";
import type { Module } from "./module.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
};

export type Response = {
	ranges?: Array<FoldingRange> | null;
};

export type FoldingRange = {
	startLine: number;
	startCharacter?: number | null;
	endLine: number;
	endCharacter?: number | null;
	kind?: FoldingRangeKind | null;
};

export type FoldingRangeKind = "comment" | "imports" | "region";

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

	// Get the outlining spans.
	let spans = typescript.languageService.getOutliningSpans(fileName);

	// Convert the outlining spans.
	let ranges = spans
		.map((span) => convertOutliningSpan(sourceFile, span))
		.filter((range): range is FoldingRange => range !== null);

	return {
		ranges: ranges.length === 0 ? null : ranges,
	};
};

let convertOutliningSpan = (
	sourceFile: ts.SourceFile,
	span: ts.OutliningSpan,
): FoldingRange | null => {
	let start = ts.getLineAndCharacterOfPosition(sourceFile, span.textSpan.start);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		span.textSpan.start + span.textSpan.length,
	);

	let endLine = end.line;
	let endCharacter: number | null = end.character;
	if (end.character === 0 && end.line > start.line) {
		endLine = end.line - 1;
		endCharacter = null;
	}

	if (endLine < start.line) {
		return null;
	}

	let kind = convertOutliningSpanKind(span.kind);
	return {
		startLine: start.line,
		startCharacter: start.character,
		endLine,
		endCharacter,
		kind,
	};
};

let convertOutliningSpanKind = (
	kind: ts.OutliningSpanKind,
): FoldingRangeKind | null => {
	switch (kind) {
		case ts.OutliningSpanKind.Comment:
			return "comment";
		case ts.OutliningSpanKind.Imports:
			return "imports";
		case ts.OutliningSpanKind.Region:
			return "region";
		case ts.OutliningSpanKind.Code:
			return null;
	}
};
