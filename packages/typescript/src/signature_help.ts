import ts from "typescript";
import type { Module } from "./module.ts";
import type { Position } from "./position.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
	position: Position;
};

export type Response = {
	help: SignatureHelp | undefined;
};

type SignatureHelp = {
	signatures: Array<SignatureInformation>;
	activeSignature: number | undefined;
	activeParameter: number | undefined;
};

type SignatureInformation = {
	label: string;
	documentation: string | undefined;
	parameters: Array<ParameterInformation>;
	activeParameter: number | undefined;
};

type ParameterInformation = {
	label: string;
	documentation: string | undefined;
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

	// Get the signature help items.
	let info = typescript.languageService.getSignatureHelpItems(
		fileName,
		position,
		undefined,
	);
	let help = info === undefined ? undefined : convertSignatureHelp(info);

	return { help };
};

let convertSignatureHelp = (info: ts.SignatureHelpItems): SignatureHelp => {
	let signatures = info.items.map(convertSignatureInformation);
	return {
		signatures,
		activeSignature: info.selectedItemIndex,
		activeParameter: info.argumentIndex,
	};
};

let convertSignatureInformation = (
	item: ts.SignatureHelpItem,
): SignatureInformation => {
	let prefix = displayPartsToString(item.prefixDisplayParts);
	let separator = displayPartsToString(item.separatorDisplayParts);
	let suffix = displayPartsToString(item.suffixDisplayParts);
	let parameters = item.parameters.map(convertParameterInformation);
	let label = `${prefix}${parameters.map(({ label }) => label).join(separator)}${suffix}`;
	let documentation = displayPartsToDocumentation(item.documentation);

	return {
		label,
		documentation,
		parameters,
		activeParameter: undefined,
	};
};

let convertParameterInformation = (
	parameter: ts.SignatureHelpParameter,
): ParameterInformation => {
	return {
		label: displayPartsToString(parameter.displayParts),
		documentation: displayPartsToDocumentation(parameter.documentation),
	};
};

let displayPartsToString = (parts: Array<ts.SymbolDisplayPart>): string => {
	return parts.map(({ text }) => text).join("");
};

let displayPartsToDocumentation = (
	parts: Array<ts.SymbolDisplayPart>,
): string | undefined => {
	let text = displayPartsToString(parts);
	return text.length === 0 ? undefined : text;
};
