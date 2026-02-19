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
	prepare: PrepareRename | undefined;
};

export type PrepareRename = {
	range: Range;
	placeholder: string;
};

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

	// Get the rename info.
	let info = typescript.languageService.getRenameInfo(fileName, position, {});
	if (!info.canRename) {
		return { prepare: undefined };
	}

	// Convert the rename info.
	let start = ts.getLineAndCharacterOfPosition(
		sourceFile,
		info.triggerSpan.start,
	);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		info.triggerSpan.start + info.triggerSpan.length,
	);
	let prepare = {
		range: { start, end },
		placeholder: info.displayName,
	};

	return { prepare };
};
