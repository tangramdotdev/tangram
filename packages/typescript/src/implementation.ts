import ts from "typescript";
import type { Location } from "./location.ts";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	position: Position;
};

export type Response = {
	locations: Array<Location> | undefined;
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

	// Get the implementations.
	let implementations = typescript.languageService.getImplementationAtPosition(
		typescript.fileNameFromModule(request.module),
		position,
	);

	// Convert the implementations.
	let locations = implementations?.map((implementation) => {
		let destinationFile = typescript.host.getSourceFile(
			implementation.fileName,
			ts.ScriptTarget.ESNext,
		);
		if (destinationFile === undefined) {
			throw new Error();
		}
		let start = ts.getLineAndCharacterOfPosition(
			destinationFile,
			implementation.textSpan.start,
		);
		let end = ts.getLineAndCharacterOfPosition(
			destinationFile,
			implementation.textSpan.start + implementation.textSpan.length,
		);
		return {
			module: typescript.moduleFromFileName(implementation.fileName),
			range: { start, end },
		};
	});

	return { locations };
};
