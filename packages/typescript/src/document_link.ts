import ts from "typescript";
import type { Module } from "./module.ts";
import type { Range } from "./range.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	module: Module;
};

export type Response = {
	links: Array<Link> | undefined;
};

export type Link = {
	range: Range;
	module: Module;
	tooltip: string | undefined;
};

export let handle = (request: Request): Response => {
	// Get the source file.
	let fileName = typescript.fileNameFromModule(request.module);
	let sourceFile = typescript.host.getSourceFile(
		fileName,
		ts.ScriptTarget.ESNext,
	);
	if (sourceFile === undefined) {
		throw new Error();
	}

	// Collect all import-like specifiers.
	let specifiers = collectSpecifiers(sourceFile);

	// Resolve and convert links.
	let links = specifiers
		.map(({ specifier, attributes }) =>
			convertLink(request.module, sourceFile, specifier, attributes),
		)
		.filter((link): link is Link => link !== undefined);

	// Deduplicate identical links.
	let deduplicatedLinks = deduplicateLinks(links);

	return {
		links: deduplicatedLinks.length === 0 ? undefined : deduplicatedLinks,
	};
};

type Specifier = {
	specifier: ts.StringLiteral;
	attributes: { [key: string]: string } | undefined;
};

let collectSpecifiers = (sourceFile: ts.SourceFile): Array<Specifier> => {
	let specifiers: Array<Specifier> = [];
	let visit = (node: ts.Node): void => {
		if (
			(ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) &&
			node.moduleSpecifier !== undefined &&
			ts.isStringLiteral(node.moduleSpecifier)
		) {
			specifiers.push({
				specifier: node.moduleSpecifier,
				attributes: getImportAttributesFromImportDeclaration(node),
			});
		} else if (
			ts.isCallExpression(node) &&
			node.expression.kind === ts.SyntaxKind.ImportKeyword
		) {
			let firstArgument = node.arguments.at(0);
			if (firstArgument !== undefined && ts.isStringLiteral(firstArgument)) {
				specifiers.push({
					specifier: firstArgument,
					attributes: getImportAttributesFromImportExpression(node),
				});
			}
		}
		ts.forEachChild(node, visit);
	};
	visit(sourceFile);
	return specifiers;
};

let convertLink = (
	module: Module,
	sourceFile: ts.SourceFile,
	specifier: ts.StringLiteral,
	attributes: { [key: string]: string } | undefined,
): Link | undefined => {
	let resolvedModule: Module;
	try {
		resolvedModule = syscall(
			"module_resolve",
			module,
			specifier.text,
			attributes,
		);
	} catch {
		return undefined;
	}

	// Exclude the quotes from the string literal range.
	let start = ts.getLineAndCharacterOfPosition(
		sourceFile,
		specifier.getStart(sourceFile) + 1,
	);
	let end = ts.getLineAndCharacterOfPosition(
		sourceFile,
		specifier.getEnd() - 1,
	);
	return {
		range: { start, end },
		module: resolvedModule,
		tooltip: undefined,
	};
};

let deduplicateLinks = (links: Array<Link>): Array<Link> => {
	let deduplicatedLinks = [];
	let seen = new Set<string>();
	for (let link of links) {
		let key = JSON.stringify([link.range, link.module]);
		if (seen.has(key)) {
			continue;
		}
		seen.add(key);
		deduplicatedLinks.push(link);
	}
	return deduplicatedLinks;
};

let getImportAttributesFromImportDeclaration = (
	declaration: ts.ImportDeclaration | ts.ExportDeclaration,
): { [key: string]: string } | undefined => {
	if (declaration.attributes === undefined) {
		return undefined;
	}
	let attributes: { [key: string]: string } = {};
	for (let attribute of declaration.attributes.elements) {
		let key = attribute.name.text;
		if (!ts.isStringLiteral(attribute.value)) {
			continue;
		}
		let value = attribute.value.text;
		attributes[key] = value;
	}
	return attributes;
};

let getImportAttributesFromImportExpression = (
	expression: ts.CallExpression,
): { [key: string]: string } | undefined => {
	let argument = expression.arguments.at(1);
	if (argument === undefined || !ts.isObjectLiteralExpression(argument)) {
		return undefined;
	}
	let with_: ts.Expression | undefined;
	for (let property of argument.properties) {
		if (
			!(
				ts.isPropertyAssignment(property) &&
				(ts.isIdentifier(property.name) || ts.isStringLiteral(property.name))
			)
		) {
			continue;
		}
		if (property.name.text !== "with") {
			continue;
		}
		with_ = property.initializer;
		break;
	}
	if (with_ === undefined || !ts.isObjectLiteralExpression(with_)) {
		return undefined;
	}
	let attributes: { [key: string]: string } = {};
	for (let property of with_.properties) {
		if (
			!(
				ts.isPropertyAssignment(property) &&
				(ts.isIdentifier(property.name) || ts.isStringLiteral(property.name)) &&
				ts.isStringLiteral(property.initializer)
			)
		) {
			continue;
		}
		let key = property.name.text;
		let value = property.initializer.text;
		attributes[key] = value;
	}
	return attributes;
};
