import ts from "typescript";
import type { Position } from "./position.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: string;
	position: Position;
};

export type Response = {
	entries: Array<CompletionEntry> | undefined;
};

export type CompletionEntry = {
	name: string;
};

export let handle = (request: Request): Response => {
	// Get the source file and position.
	let sourceFile = typescript.host.getSourceFile(
		typescript.fileNameFromModule(request.module),
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

	// Get the completions.
	let info = typescript.languageService.getCompletionsAtPosition(
		typescript.fileNameFromModule(request.module),
		position,
		undefined,
	);

	// Convert the completion entries.
	let entries = info?.entries.map((entry) => ({ name: entry.name }));

	return {
		entries,
	};
};
