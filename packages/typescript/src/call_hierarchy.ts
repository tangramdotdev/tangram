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
	items?: Array<Item> | null;
};

export type IncomingRequest = {
	module: Module;
	position: Position;
};

export type IncomingResponse = {
	calls?: Array<IncomingCall> | null;
};

export type OutgoingRequest = {
	module: Module;
	position: Position;
};

export type OutgoingResponse = {
	calls?: Array<OutgoingCall> | null;
};

export type Item = {
	name: string;
	kind: string;
	detail?: string | null;
	module: Module;
	range: Range;
	selection: Range;
	containerName?: string | null;
	data?: ItemData | null;
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
			? null
			: (Array.isArray(items) ? items : [items])
					.map(convertCallHierarchyItem)
					.filter((item): item is Item => item !== null);

	return {
		items: output === null || output.length === 0 ? null : output,
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
		.filter((call): call is IncomingCall => call !== null);

	return {
		calls: output.length === 0 ? null : output,
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
		.filter((call): call is OutgoingCall => call !== null);

	return {
		calls: output.length === 0 ? null : output,
	};
};

let convertCallHierarchyItem = (item: ts.CallHierarchyItem): Item | null => {
	let sourceFile = typescript.host.getSourceFile(
		item.file,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		return null;
	}
	let module: Module;
	try {
		module = typescript.moduleFromFileName(item.file);
	} catch {
		return null;
	}
	let range = convertTextSpanToRange(sourceFile, item.span);
	let selection = convertTextSpanToRange(sourceFile, item.selectionSpan);
	if (range === null || selection === null) {
		return null;
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
		detail: null,
		module,
		range,
		selection,
		containerName: stringOrNull(item.containerName),
		data,
	};
};

let convertIncomingCall = (
	call: ts.CallHierarchyIncomingCall,
): IncomingCall | null => {
	let from = convertCallHierarchyItem(call.from);
	if (from === null) {
		return null;
	}
	let sourceFile = typescript.host.getSourceFile(
		call.from.file,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		return null;
	}
	let fromRanges = call.fromSpans
		.map((span) => convertTextSpanToRange(sourceFile, span))
		.filter((range): range is Range => range !== null);
	return {
		from,
		fromRanges,
	};
};

let convertOutgoingCall = (
	sourceFile: ts.SourceFile,
	call: ts.CallHierarchyOutgoingCall,
): OutgoingCall | null => {
	let to = convertCallHierarchyItem(call.to);
	if (to === null) {
		return null;
	}
	let fromRanges = call.fromSpans
		.map((span) => convertTextSpanToRange(sourceFile, span))
		.filter((range): range is Range => range !== null);
	return {
		to,
		fromRanges,
	};
};

let convertTextSpanToRange = (
	sourceFile: ts.SourceFile,
	span: ts.TextSpan,
): Range | null => {
	if (span.length < 0) {
		return null;
	}
	let start = ts.getLineAndCharacterOfPosition(sourceFile, span.start);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		span.start + span.length,
	);
	return { start, end };
};

let stringOrNull = (value: string | undefined): string | null => {
	if (value === undefined || value.length === 0) {
		return null;
	}
	return value;
};
