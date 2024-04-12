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

	// Get the rename locations.
	let renameLocations = typescript.languageService.findRenameLocations(
		typescript.fileNameFromModule(request.module),
		position,
		false,
		false,
	);

	// Convert the definitions.
	let locations = renameLocations?.map((renameLocation) => {
		let destFile = typescript.host.getSourceFile(
			renameLocation.fileName,
			ts.ScriptTarget.ESNext,
		);
		if (destFile === undefined) {
			throw new Error();
		}
		let start = ts.getLineAndCharacterOfPosition(
			destFile,
			renameLocation.textSpan.start,
		);
		let end = ts.getLineAndCharacterOfPosition(
			destFile,
			renameLocation.textSpan.start + renameLocation.textSpan.length,
		);
		let location = {
			module: typescript.moduleFromFileName(renameLocation.fileName),
			range: { start, end },
		};
		return location;
	});

	return { locations };
};
