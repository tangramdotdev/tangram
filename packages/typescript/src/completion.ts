import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	position: Position;
};

export type Response = {
	entries: Array<CompletionEntry> | undefined;
};

export type CompletionEntry = {
	name: string;
	kind: string;
	kindModifiers: string | undefined;
	sortText: string;
	insertText: string | undefined;
	filterText: string | undefined;
	isSnippet: boolean | undefined;
	source: string | undefined;
	commitCharacters: Array<string> | undefined;
	data: unknown;
	labelDetails: CompletionEntryLabelDetails | undefined;
};

export type CompletionEntryLabelDetails = {
	detail: string | undefined;
	description: string | undefined;
};

export type ResolveRequest = {
	module: Module;
	position: Position;
	name: string;
	source: string | undefined;
	data: unknown;
};

export type ResolveResponse = {
	entry: CompletionEntryDetails | undefined;
};

export type CompletionEntryDetails = {
	kind: string;
	kindModifiers: string | undefined;
	detail: string | undefined;
	documentation: string | undefined;
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
	let entries = info?.entries.map(convertCompletionEntry);

	return {
		entries,
	};
};

export let handleResolve = (request: ResolveRequest): ResolveResponse => {
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

	// Get the completion details.
	let details = typescript.languageService.getCompletionEntryDetails(
		fileName,
		position,
		request.name,
		undefined,
		request.source,
		undefined,
		request.data as ts.CompletionEntryData | undefined,
	);
	let entry =
		details === undefined ? undefined : convertCompletionEntryDetails(details);

	return { entry };
};

let convertCompletionEntry = (entry: ts.CompletionEntry): CompletionEntry => {
	let labelDetails = convertCompletionEntryLabelDetails(entry);
	return {
		name: entry.name,
		kind: entry.kind,
		kindModifiers: stringOrUndefined(entry.kindModifiers),
		sortText: entry.sortText,
		insertText: entry.insertText,
		filterText: entry.filterText,
		isSnippet: entry.isSnippet,
		source: entry.source,
		commitCharacters: entry.commitCharacters,
		data: entry.data,
		labelDetails,
	};
};

let convertCompletionEntryLabelDetails = (
	entry: ts.CompletionEntry,
): CompletionEntryLabelDetails | undefined => {
	let detail = entry.labelDetails?.detail;
	let description = entry.labelDetails?.description;
	if (detail === undefined && description === undefined) {
		return undefined;
	}
	return { detail, description };
};

let convertCompletionEntryDetails = (
	details: ts.CompletionEntryDetails,
): CompletionEntryDetails => {
	let detail = displayPartsToString(details.displayParts);
	let documentation = formatDocumentation(details);
	return {
		kind: details.kind,
		kindModifiers: stringOrUndefined(details.kindModifiers),
		detail: stringOrUndefined(detail),
		documentation,
	};
};

let displayPartsToString = (
	parts: ReadonlyArray<ts.SymbolDisplayPart> | undefined,
): string => {
	return parts?.map(({ text }) => text).join("") ?? "";
};

let formatDocumentation = (
	details: ts.CompletionEntryDetails,
): string | undefined => {
	let documentation = stringOrUndefined(
		displayPartsToString(details.documentation),
	);
	let tags = details.tags
		?.map((tag) => {
			let text = displayPartsToString(tag.text);
			return text.length === 0 ? `@${tag.name}` : `@${tag.name} ${text}`;
		})
		.join("\n");
	let sections = [documentation, stringOrUndefined(tags)].filter(
		(value): value is string => value !== undefined,
	);
	if (sections.length === 0) {
		return undefined;
	}
	return sections.join("\n\n");
};

let stringOrUndefined = (value: string | undefined): string | undefined => {
	if (value === undefined || value.length === 0) {
		return undefined;
	}
	return value;
};
