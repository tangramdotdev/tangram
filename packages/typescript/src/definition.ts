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

	// Get the definitions.
	let definitions = typescript.languageService.getDefinitionAtPosition(
		typescript.fileNameFromModule(request.module),
		position,
	);

	// Convert the definitions.
	let locations = definitions?.map((definition) => {
		let destFile = typescript.host.getSourceFile(
			definition.fileName,
			ts.ScriptTarget.ESNext,
		);
		if (destFile === undefined) {
			throw new Error();
		}
		let start = ts.getLineAndCharacterOfPosition(
			destFile,
			definition.textSpan.start,
		);
		let end = ts.getLineAndCharacterOfPosition(
			destFile,
			definition.textSpan.start + definition.textSpan.length,
		);
		let location = {
			module: typescript.moduleFromFileName(definition.fileName),
			range: { start, end },
		};

		return location;
	});

	return {
		locations,
	};
};

export let handleDeclaration = (request: Request): Response => {
	return handle(request);
};

export let handleTypeDefinition = (request: Request): Response => {
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

	// Get the definitions.
	let definitions = typescript.languageService.getTypeDefinitionAtPosition(
		typescript.fileNameFromModule(request.module),
		position,
	);

	// Convert the definitions.
	let locations = definitions?.map((definition) => {
		let destFile = typescript.host.getSourceFile(
			definition.fileName,
			ts.ScriptTarget.ESNext,
		);
		if (destFile === undefined) {
			throw new Error();
		}
		let start = ts.getLineAndCharacterOfPosition(
			destFile,
			definition.textSpan.start,
		);
		let end = ts.getLineAndCharacterOfPosition(
			destFile,
			definition.textSpan.start + definition.textSpan.length,
		);
		let location = {
			module: typescript.moduleFromFileName(definition.fileName),
			range: { start, end },
		};

		return location;
	});

	return {
		locations,
	};
};
