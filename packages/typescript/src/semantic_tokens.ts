import ts from "typescript";
import type { Module } from "./module.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
};

export type Response = {
	tokens: Array<SemanticToken> | undefined;
};

export type SemanticToken = {
	line: number;
	start: number;
	length: number;
	tokenType: number;
	tokenModifiersBitset: number;
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

	// Get the semantic classifications.
	let span = {
		start: 0,
		length: sourceFile.text.length,
	};
	let classifications =
		typescript.languageService.getEncodedSemanticClassifications(
			fileName,
			span,
			ts.SemanticClassificationFormat.TwentyTwenty,
		);

	// Decode the classifications into semantic tokens.
	let tokens = decodeSemanticTokens(sourceFile, classifications.spans);

	return {
		tokens: tokens.length === 0 ? undefined : tokens,
	};
};

let decodeSemanticTokens = (
	sourceFile: ts.SourceFile,
	spans: Array<number>,
): Array<SemanticToken> => {
	let output = [];
	for (let i = 0; i < spans.length; i += 3) {
		let start = spans[i]!;
		let length = spans[i + 1]!;
		let classification = spans[i + 2]!;
		let tokenType = (classification >> 8) - 1;
		let tokenModifiersBitset = classification & 0xff;
		if (tokenType < 0) {
			continue;
		}
		let position = ts.getLineAndCharacterOfPosition(sourceFile, start);
		output.push({
			line: position.line,
			start: position.character,
			length,
			tokenType,
			tokenModifiersBitset,
		});
	}
	return output;
};
