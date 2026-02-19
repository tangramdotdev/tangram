import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	position: Position;
};

export type Response = {
	highlights: Array<DocumentHighlight> | undefined;
};

export type DocumentHighlight = {
	range: Range;
	kind: DocumentHighlightKind | undefined;
};

export type DocumentHighlightKind = "text" | "read" | "write";

export let handle = (request: Request): Response => {
	// Get the source file and position.
	let fileName = typescript.fileNameFromModule(request.module);
	let sourceFile = typescript.host.getSourceFile(
		fileName,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		throw new Error();
	}
	let position = ts.getPositionOfLineAndCharacter(
		sourceFile,
		request.position.line,
		request.position.character,
	);

	// Get the highlights.
	let documentHighlights = typescript.languageService.getDocumentHighlights(
		fileName,
		position,
		[fileName],
	);

	// Convert the highlights.
	let highlights = documentHighlights?.flatMap((documentHighlight) => {
		let destinationFile = typescript.host.getSourceFile(
			documentHighlight.fileName,
			ts.ScriptTarget.ESNext,
		);
		if (destinationFile === undefined) {
			throw new Error();
		}
		return documentHighlight.highlightSpans.map((span) => {
			let start = ts.getLineAndCharacterOfPosition(
				destinationFile,
				span.textSpan.start,
			);
			let end = ts.getLineAndCharacterOfPosition(
				destinationFile,
				span.textSpan.start + span.textSpan.length,
			);
			let kind = convertHighlightSpanKind(span.kind);
			return {
				range: { start, end },
				kind,
			};
		});
	});

	if (highlights?.length === 0) {
		highlights = undefined;
	}

	return { highlights };
};

let convertHighlightSpanKind = (
	kind: ts.HighlightSpanKind,
): DocumentHighlightKind => {
	switch (kind) {
		case ts.HighlightSpanKind.none:
			return "text";
		case ts.HighlightSpanKind.reference:
			return "read";
		case ts.HighlightSpanKind.writtenReference:
		case ts.HighlightSpanKind.definition:
			return "write";
	}
};
