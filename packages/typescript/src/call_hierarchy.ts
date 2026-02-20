import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type PrepareRequest = {
	module: Module;
	position: Position;
};

export type PrepareResponse = {
	items: Array<Item> | undefined;
};

export type IncomingRequest = {
	module: Module;
	position: Position;
};

export type IncomingResponse = {
	calls: Array<IncomingCall> | undefined;
};

export type OutgoingRequest = {
	module: Module;
	position: Position;
};

export type OutgoingResponse = {
	calls: Array<OutgoingCall> | undefined;
};

export type Item = {
	name: string;
	kind: string;
	detail: string | undefined;
	module: Module;
	range: Range;
	selection: Range;
	containerName: string | undefined;
	data: ItemData;
};

export type ItemData = {
	module: Module;
	position: Position;
};

export type IncomingCall = {
	from: Item;
	fromRanges: Array<Range>;
};

export type OutgoingCall = {
	to: Item;
	fromRanges: Array<Range>;
};

export let handlePrepare = (request: PrepareRequest): PrepareResponse => {
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

	// Get the call hierarchy items.
	let items = typescript.languageService.prepareCallHierarchy(
		fileName,
		position,
	);
	let output =
		items === undefined
			? undefined
			: (Array.isArray(items) ? items : [items])
					.map(convertCallHierarchyItem)
					.filter((item): item is Item => item !== undefined);

	return {
		items: output === undefined || output.length === 0 ? undefined : output,
	};
};

export let handleIncoming = (request: IncomingRequest): IncomingResponse => {
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

	// Get incoming calls.
	let calls = typescript.languageService.provideCallHierarchyIncomingCalls(
		fileName,
		position,
	);
	let output = calls
		.map((call) => convertIncomingCall(call))
		.filter((call): call is IncomingCall => call !== undefined);

	return {
		calls: output.length === 0 ? undefined : output,
	};
};

export let handleOutgoing = (request: OutgoingRequest): OutgoingResponse => {
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

	// Get outgoing calls.
	let calls = typescript.languageService.provideCallHierarchyOutgoingCalls(
		fileName,
		position,
	);
	let output = calls
		.map((call) => convertOutgoingCall(sourceFile, call))
		.filter((call): call is OutgoingCall => call !== undefined);

	return {
		calls: output.length === 0 ? undefined : output,
	};
};

let convertCallHierarchyItem = (
	item: ts.CallHierarchyItem,
): Item | undefined => {
	let sourceFile = typescript.host.getSourceFile(
		item.file,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		return undefined;
	}
	let module: Module;
	try {
		module = typescript.moduleFromFileName(item.file);
	} catch {
		return undefined;
	}
	let range = convertTextSpanToRange(sourceFile, item.span);
	let selection = convertTextSpanToRange(sourceFile, item.selectionSpan);
	if (range === undefined || selection === undefined) {
		return undefined;
	}
	let data = {
		module,
		position: ts.getLineAndCharacterOfPosition(
			sourceFile,
			item.selectionSpan.start,
		),
	};
	return {
		name: item.name,
		kind: item.kind,
		detail: undefined,
		module,
		range,
		selection,
		containerName: stringOrUndefined(item.containerName),
		data,
	};
};

let convertIncomingCall = (
	call: ts.CallHierarchyIncomingCall,
): IncomingCall | undefined => {
	let from = convertCallHierarchyItem(call.from);
	if (from === undefined) {
		return undefined;
	}
	let sourceFile = typescript.host.getSourceFile(
		call.from.file,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		return undefined;
	}
	let fromRanges = call.fromSpans
		.map((span) => convertTextSpanToRange(sourceFile, span))
		.filter((range): range is Range => range !== undefined);
	return {
		from,
		fromRanges,
	};
};

let convertOutgoingCall = (
	sourceFile: ts.SourceFile,
	call: ts.CallHierarchyOutgoingCall,
): OutgoingCall | undefined => {
	let to = convertCallHierarchyItem(call.to);
	if (to === undefined) {
		return undefined;
	}
	let fromRanges = call.fromSpans
		.map((span) => convertTextSpanToRange(sourceFile, span))
		.filter((range): range is Range => range !== undefined);
	return {
		to,
		fromRanges,
	};
};

let convertTextSpanToRange = (
	sourceFile: ts.SourceFile,
	span: ts.TextSpan,
): Range | undefined => {
	if (span.length < 0) {
		return undefined;
	}
	let start = ts.getLineAndCharacterOfPosition(sourceFile, span.start);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		span.start + span.length,
	);
	return { start, end };
};

let stringOrUndefined = (value: string | undefined): string | undefined => {
	if (value === undefined || value.length === 0) {
		return undefined;
	}
	return value;
};
