import ts from "typescript";
import type { Module } from "./module.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
};

export type Response = {
	ranges: Array<FoldingRange> | undefined;
};

export type FoldingRange = {
	startLine: number;
	startCharacter: number | undefined;
	endLine: number;
	endCharacter: number | undefined;
	kind: FoldingRangeKind | undefined;
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
		.filter((range): range is FoldingRange => range !== undefined);

	return {
		ranges: ranges.length === 0 ? undefined : ranges,
	};
};

let convertOutliningSpan = (
	sourceFile: ts.SourceFile,
	span: ts.OutliningSpan,
): FoldingRange | undefined => {
	let start = ts.getLineAndCharacterOfPosition(sourceFile, span.textSpan.start);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		span.textSpan.start + span.textSpan.length,
	);

	let endLine = end.line;
	let endCharacter: number | undefined = end.character;
	if (end.character === 0 && end.line > start.line) {
		endLine = end.line - 1;
		endCharacter = undefined;
	}

	if (endLine < start.line) {
		return undefined;
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
): FoldingRangeKind | undefined => {
	switch (kind) {
		case ts.OutliningSpanKind.Comment:
			return "comment";
		case ts.OutliningSpanKind.Imports:
			return "imports";
		case ts.OutliningSpanKind.Region:
			return "region";
		case ts.OutliningSpanKind.Code:
			return undefined;
	}
};
