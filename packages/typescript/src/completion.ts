import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	position: Position;
};

export type Response = {
	entries?: Array<CompletionEntry> | null;
};

export type CompletionEntry = {
	name: string;
	kind: string;
	kindModifiers?: string | null;
	sortText: string;
	insertText?: string | null;
	filterText?: string | null;
	isSnippet?: boolean | null;
	source?: string | null;
	commitCharacters?: Array<string> | null;
	data?: {} | null;
	labelDetails?: CompletionEntryLabelDetails | null;
};

export type CompletionEntryLabelDetails = {
	detail?: string | null;
	description?: string | null;
};

export type ResolveRequest = {
	module: Module;
	position: Position;
	name: string;
	source?: string | null;
	data?: {} | null;
};

export type ResolveResponse = {
	entry?: CompletionEntryDetails | null;
};

export type CompletionEntryDetails = {
	kind: string;
	kindModifiers?: string | null;
	detail?: string | null;
	documentation?: string | null;
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
		entries: entries ?? null,
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
		request.source ?? undefined,
		undefined,
		(request.data ?? undefined) as ts.CompletionEntryData | undefined,
	);
	let entry =
		details === undefined ? null : convertCompletionEntryDetails(details);

	return { entry };
};

let convertCompletionEntry = (entry: ts.CompletionEntry): CompletionEntry => {
	let labelDetails = convertCompletionEntryLabelDetails(entry);
	return {
		name: entry.name,
		kind: entry.kind,
		kindModifiers: stringOrNull(entry.kindModifiers),
		sortText: entry.sortText,
		insertText: entry.insertText ?? null,
		filterText: entry.filterText ?? null,
		isSnippet: entry.isSnippet ?? null,
		source: entry.source ?? null,
		commitCharacters: entry.commitCharacters ?? null,
		data: (entry.data ?? null) as {} | null,
		labelDetails,
	};
};

let convertCompletionEntryLabelDetails = (
	entry: ts.CompletionEntry,
): CompletionEntryLabelDetails | null => {
	let detail = entry.labelDetails?.detail;
	let description = entry.labelDetails?.description;
	if (detail === undefined && description === undefined) {
		return null;
	}
	return { detail: detail ?? null, description: description ?? null };
};

let convertCompletionEntryDetails = (
	details: ts.CompletionEntryDetails,
): CompletionEntryDetails => {
	let detail = displayPartsToString(details.displayParts);
	let documentation = formatDocumentation(details);
	return {
		kind: details.kind,
		kindModifiers: stringOrNull(details.kindModifiers),
		detail: stringOrNull(detail),
		documentation: documentation ?? null,
	};
};

let displayPartsToString = (
	parts: ReadonlyArray<ts.SymbolDisplayPart> | undefined,
): string => {
	return parts?.map(({ text }) => text).join("") ?? "";
};

let formatDocumentation = (
	details: ts.CompletionEntryDetails,
): string | null => {
	let documentation = stringOrNull(displayPartsToString(details.documentation));
	let tags = details.tags
		?.map((tag) => {
			let text = displayPartsToString(tag.text);
			return text.length === 0 ? `@${tag.name}` : `@${tag.name} ${text}`;
		})
		.join("\n");
	let sections = [documentation, stringOrNull(tags)].filter(
		(value): value is string => value !== null,
	);
	if (sections.length === 0) {
		return null;
	}
	return sections.join("\n\n");
};

let stringOrNull = (value: string | undefined): string | null => {
	if (value === undefined || value.length === 0) {
		return null;
	}
	return value;
};
