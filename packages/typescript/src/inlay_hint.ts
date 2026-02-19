import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	range: Range;
};

export type Response = {
	hints: Array<InlayHint> | undefined;
};

export type InlayHint = {
	position: Position;
	label: string;
	kind: InlayHintKind | undefined;
	paddingLeft: boolean | undefined;
	paddingRight: boolean | undefined;
};

export type InlayHintKind = "type" | "parameter";

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

	// Get the span.
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
	let span = {
		start,
		length: end - start,
	};

	// Get the inlay hints.
	let hints = typescript.languageService.provideInlayHints(
		fileName,
		span,
		undefined,
	);
	let convertedHints = hints
		.map((hint) => convertInlayHint(sourceFile, hint))
		.filter((hint): hint is InlayHint => hint !== undefined);

	return {
		hints: convertedHints.length === 0 ? undefined : convertedHints,
	};
};

let convertInlayHint = (
	sourceFile: ts.SourceFile,
	hint: ts.InlayHint,
): InlayHint | undefined => {
	let label = hint.displayParts?.map((part) => part.text).join("") ?? hint.text;
	if (label.length === 0) {
		return undefined;
	}

	let position = ts.getLineAndCharacterOfPosition(sourceFile, hint.position);
	let kind = convertInlayHintKind(hint.kind);
	let paddingLeft = hint.whitespaceBefore;
	let paddingRight = hint.whitespaceAfter;
	return {
		position,
		label,
		kind,
		paddingLeft,
		paddingRight,
	};
};

let convertInlayHintKind = (
	kind: ts.InlayHintKind,
): InlayHintKind | undefined => {
	switch (kind) {
		case ts.InlayHintKind.Type:
		case ts.InlayHintKind.Enum:
			return "type";
		case ts.InlayHintKind.Parameter:
			return "parameter";
	}
};
