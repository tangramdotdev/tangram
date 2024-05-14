import ts from "typescript";
import { assert } from "./assert.ts";
import type { Diagnostic, Severity } from "./diagnostics.ts";
import { log } from "./log.ts";
import type { Module } from "./module.ts";

// Create the TypeScript compiler options.
export let compilerOptions: ts.CompilerOptions = {
	allowJs: true,
	allowArbitraryExtensions: true,
	isolatedModules: true,
	module: ts.ModuleKind.ESNext,
	noEmit: true,
	noUncheckedIndexedAccess: true,
	skipLibCheck: true,
	strict: true,
	target: ts.ScriptTarget.ESNext,
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
		return "/";
	},

	getDefaultLibFileName: () => {
		return "/library/tangram.d.ts";
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
		} catch (e) {
			log(e);
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
		} catch (e) {
			log(e);
			return undefined;
		}
		let sourceFile = ts.createSourceFile(fileName, text, languageVersion);
		return sourceFile;
	},

	hasInvalidatedResolutions: (_fileName) => {
		return false;
	},

	readFile: () => {
		throw new Error();
	},

	resolveModuleNameLiterals: (imports, module) => {
		return imports.map((import_) => {
			let specifier = import_.text;
			let declaration = import_.parent;
			assert(
				ts.isImportDeclaration(declaration) ||
					ts.isExportDeclaration(declaration),
			);
			let attributes = Object.fromEntries(
				(declaration.attributes?.elements ?? []).map((attribute) => {
					let key = attribute.name.text;
					assert(ts.isStringLiteral(attribute.value));
					let value = attribute.value.text;
					return [key, value];
				}),
			);
			let resolvedFileName: string | undefined;
			try {
				resolvedFileName = fileNameFromModule(
					syscall(
						"module_resolve",
						moduleFromFileName(module),
						specifier,
						attributes,
					),
				);
			} catch (e) {
				log(e);
				return { resolvedModule: undefined };
			}
			let extension = resolvedFileName.slice(-3);
			return {
				resolvedModule: {
					resolvedFileName,
					extension,
				},
			};
		});
	},

	useCaseSensitiveFileNames: () => {
		return true;
	},

	writeFile: () => {
		throw new Error();
	},
};

// Create the document registry.
export let documentRegistry = ts.createDocumentRegistry();

// Create the TypeScript language service.
export let languageService = ts.createLanguageService(host, documentRegistry);

/** Convert a module to a TypeScript file name. */
export let fileNameFromModule = (module: Module): string => {
	if (module.kind === "dts") {
		return `/library/${module.value.path.slice(2)}`;
	} else {
		let json = syscall("encoding_json_encode", module);
		let utf8 = syscall("encoding_utf8_encode", json);
		let hex = syscall("encoding_hex_encode", utf8);
		let extension: string;
		if (module.kind === "js") {
			extension = ".js";
		} else if (module.kind === "ts") {
			extension = ".ts";
		} else {
			extension = ".js";
		}
		return `/${hex}${extension}`;
	}
};

/** Convert a TypeScript file name to a module. */
export let moduleFromFileName = (fileName: string): Module => {
	let module: Module;
	if (fileName.startsWith("/library/")) {
		let path = fileName.slice(9);
		module = { kind: "dts", value: { path } };
	} else {
		let hex = fileName.slice(1, -3);
		let utf8 = syscall("encoding_hex_decode", hex);
		let json = syscall("encoding_utf8_decode", utf8);
		module = syscall("encoding_json_decode", json) as Module;
	}
	return module;
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
