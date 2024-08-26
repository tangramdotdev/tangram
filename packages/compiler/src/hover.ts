import ts from "typescript";
import type { Position } from "./position.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: string;
	position: Position;
};

export type Response = {
	text: string | undefined;
};

export let handle = (request: Request): Response => {
	// Get the source file.
	let sourceFile = typescript.host.getSourceFile(
		typescript.fileNameFromModuleReference(request.module),
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		throw new Error();
	}

	// Get the position of the hover.
	let position = ts.getPositionOfLineAndCharacter(
		sourceFile,
		request.position.line,
		request.position.character,
	);

	// Get the quick info at the position.
	let quickInfo = typescript.languageService.getQuickInfoAtPosition(
		typescript.fileNameFromModuleReference(request.module),
		position,
	);

	// Get the text.
	let text = quickInfo?.displayParts?.map(({ text }) => text).join("");

	return { text };
};
