import { Location } from "./location.ts";
import { Module } from "./module.ts";
import { Position } from "./position.ts";
import * as typescript from "./typescript.ts";
import ts from "typescript";

export type Request = {
	module: Module;
	position: Position;
};

export type Response = {
	locations: Array<Location> | null;
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
	let references = typescript.languageService.getReferencesAtPosition(
		typescript.fileNameFromModule(request.module),
		position,
	);

	// Convert the references.
	let locations =
		references?.map((reference) => {
			let destFile = typescript.host.getSourceFile(
				reference.fileName,
				ts.ScriptTarget.ESNext,
			);
			if (destFile === undefined) {
				throw new Error(destFile);
			}
			// Get the references's range.
			let start = ts.getLineAndCharacterOfPosition(
				destFile,
				reference.textSpan.start,
			);
			let end = ts.getLineAndCharacterOfPosition(
				destFile,
				reference.textSpan.start + reference.textSpan.length,
			);

			let location = {
				module: typescript.moduleFromFileName(reference.fileName),
				range: { start, end },
			};

			return location;
		}) ?? null;

	return {
		locations,
	};
};
