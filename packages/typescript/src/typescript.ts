import ts from "typescript";
import { assert, unreachable } from "./assert.ts";
import type { Diagnostic, Severity } from "./diagnostics.ts";
import { log } from "./log.ts";
import { Module } from "./module.ts";

// Create the TypeScript compiler options.
export let compilerOptions: ts.CompilerOptions = {
	allowJs: true,
	checkJs: true,
	exactOptionalPropertyTypes: true,
	isolatedModules: true,
	module: ts.ModuleKind.ESNext,
	moduleDetection: ts.ModuleDetectionKind.Force,
	noEmit: true,
	noUncheckedIndexedAccess: true,
	noUncheckedSideEffectImports: true,
	skipLibCheck: true,
	strict: true,
	target: ts.ScriptTarget.ESNext,
	verbatimModuleSyntax: true,
};

// Create the host implementation for the TypeScript language service and compiler.
export let host: ts.LanguageServiceHost & ts.CompilerHost = {
	fileExists: () => {
		return false;
	},

	getCompilationSettings: () => {
		return compilerOptions;
	},

	getCanonicalFileName: (fileName) => {
		return fileName;
	},

	getCurrentDirectory: () => {
		return "";
	},

	getDefaultLibFileName: () => {
		return "lib:/tangram.d.ts";
	},

	getNewLine: () => {
		return "\n";
	},

	getScriptFileNames: () => {
		return syscall("document_list").map(fileNameFromModule);
	},

	getScriptSnapshot: (fileName) => {
		let text: string | undefined;
		try {
			text = syscall("module_load", moduleFromFileName(fileName));
		} catch (error) {
			log(error);
			return undefined;
		}
		return ts.ScriptSnapshot.fromString(text);
	},

	getScriptVersion: (fileName) => {
		return syscall("module_version", moduleFromFileName(fileName));
	},

	getSourceFile: (fileName, languageVersion) => {
		let text: string | undefined;
		try {
			text = syscall("module_load", moduleFromFileName(fileName));
		} catch (error) {
			log(error);
			return undefined;
		}
		let sourceFile = ts.createSourceFile(fileName, text, languageVersion);
		return sourceFile;
	},

	hasInvalidatedResolutions: (fileName) => {
		try {
			let module = moduleFromFileName(fileName);
			return syscall("module_invalidated_resolutions", module);
		} catch (error) {
			log(error);
			return false;
		}
	},

	readFile: () => {
		throw new Error();
	},

	resolveModuleNameLiterals: (imports, fileName) => {
		let output = imports.map((import_) => {
			let specifier = import_.text;
			let parent = import_.parent;
			let attributes: { [key: string]: string } | undefined;
			if (ts.isImportDeclaration(parent) || ts.isExportDeclaration(parent)) {
				attributes = getImportAttributesFromImportDeclaration(parent);
			} else if (
				ts.isCallExpression(parent) &&
				parent.expression.kind === ts.SyntaxKind.ImportKeyword
			) {
				attributes = getImportAttributesFromImportExpression(parent);
			} else {
				return unreachable();
			}
			let resolvedFileName: string;
			let extension: string;
			let module = moduleFromFileName(fileName);
			let resolvedModule: Module;
			try {
				resolvedModule = syscall(
					"module_resolve",
					module,
					specifier,
					attributes,
				);
			} catch (error) {
				log(error);
				return { resolvedModule: undefined };
			}
			resolvedFileName = fileNameFromModule(resolvedModule);
			if (resolvedModule.kind === "js") {
				extension = ".js";
			} else if (resolvedModule.kind === "ts") {
				extension = ".ts";
			} else {
				extension = ".ts";
			}
			return {
				resolvedModule: {
					resolvedFileName,
					extension,
				},
			};
		});
		try {
			let module = moduleFromFileName(fileName);
			syscall("module_validate_resolutions", module);
		} catch (error) {
			log(error);
		}
		return output;
	},

	useCaseSensitiveFileNames: () => {
		return true;
	},

	writeFile: () => {
		throw new Error();
	},
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
	if (argument === undefined) {
		return undefined;
	}
	if (!ts.isObjectLiteralExpression(argument)) {
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
	if (with_ === undefined) {
		return undefined;
	}
	if (!ts.isObjectLiteralExpression(with_)) {
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

/** Convert a module to a TypeScript file name. */
export let fileNameFromModule = (module: Module): string => {
	if (module.kind === "dts") {
		let item = module.referent.item;
		assert(typeof item === "string");
		return `lib:/${item.slice(2)}`;
	}
	let string = Module.toDataString(module);
	let extension: string;
	if (module.kind === "js") {
		extension = ".js";
	} else if (module.kind === "ts") {
		extension = ".ts";
	} else {
		extension = ".ts";
	}
	string += `&extension=${extension}`;
	return string;
};

/** Convert a TypeScript file name to a module. */
export let moduleFromFileName = (fileName: string): Module => {
	if (fileName.startsWith("lib:/")) {
		let path = fileName.slice(5);
		let item = `./${path}`;
		return {
			kind: "dts",
			referent: { item },
		};
	}
	return Module.fromDataString(fileName);
};

/** Convert a diagnostic. */
export let convertDiagnostic = (diagnostic: ts.Diagnostic): Diagnostic => {
	// Get the diagnostic's location.
	let location = null;
	if (
		diagnostic.file !== undefined &&
		diagnostic.start !== undefined &&
		diagnostic.length !== undefined
	) {
		// Get the diagnostic's module.
		let module_ = moduleFromFileName(diagnostic.file.fileName);

		// Get the diagnostic's range.
		let start = ts.getLineAndCharacterOfPosition(
			diagnostic.file,
			diagnostic.start,
		);
		let end = ts.getLineAndCharacterOfPosition(
			diagnostic.file,
			diagnostic.start + diagnostic.length,
		);
		let range = { start, end };

		location = {
			module: module_,
			range,
		};
	}

	// Convert the diagnostic's severity.
	let severity: Severity;
	switch (diagnostic.category) {
		case ts.DiagnosticCategory.Warning: {
			severity = "warning";
			break;
		}
		case ts.DiagnosticCategory.Error: {
			severity = "error";
			break;
		}
		case ts.DiagnosticCategory.Suggestion: {
			severity = "hint";
			break;
		}
		case ts.DiagnosticCategory.Message: {
			severity = "info";
			break;
		}
		default: {
			throw new Error("unknown diagnostic category");
		}
	}

	let message: string;
	switch (diagnostic.code) {
		case 2732:
		case 2792: {
			message = "cannot find the module";
			break;
		}
		default: {
			message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
			break;
		}
	}

	return {
		location,
		severity,
		message,
	};
};

// Create the document registry.
export let documentRegistry = ts.createDocumentRegistry();

// Create the TypeScript language service.
export let languageService = ts.createLanguageService(host, documentRegistry);
