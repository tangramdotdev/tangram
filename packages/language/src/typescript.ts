import { assert } from "./assert.ts";
import { Module } from "./module.ts";
import * as syscall from "./syscall.ts";
import ts from "typescript";

// Create the TypeScript compiler options.
export let compilerOptions: ts.CompilerOptions = {
	allowJs: true,
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
		return syscall.documents().map(fileNameFromModule);
	},

	getScriptSnapshot: (fileName) => {
		let text;
		try {
			text = syscall.module_.load(moduleFromFileName(fileName));
		} catch {
			return undefined;
		}
		return ts.ScriptSnapshot.fromString(text);
	},

	getScriptVersion: (fileName) => {
		return syscall.module_.version(moduleFromFileName(fileName));
	},

	getSourceFile: (fileName, languageVersion) => {
		let text;
		try {
			text = syscall.module_.load(moduleFromFileName(fileName));
		} catch {
			return undefined;
		}
		let sourceFile = ts.createSourceFile(fileName, text, languageVersion);
		return sourceFile;
	},

	hasInvalidatedResolutions: (_fileName) => {
		return false;
	},

	readFile: () => {
		throw new Error("Unimplemented.");
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
			let resolvedFileName;
			try {
				resolvedFileName = fileNameFromModule(
					syscall.module_.resolve(
						moduleFromFileName(module),
						specifier,
						attributes,
					),
				);
			} catch {
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
		throw new Error("Unimplemented.");
	},
};

// Create the document registry.
export let documentRegistry = ts.createDocumentRegistry();

// Create the TypeScript language service.
export let languageService = ts.createLanguageService(host, documentRegistry);

/** Convert a module to a TypeScript file name. */
export let fileNameFromModule = (module_: Module): string => {
	if (module_.kind === "library") {
		return `/library/${module_.value.path}`;
	} else {
		let data = syscall.encoding.hex.encode(
			syscall.encoding.utf8.encode(syscall.encoding.json.encode(module_)),
		);
		let extension;
		if (module_.value.path.endsWith(".js")) {
			extension = ".js";
		} else if (module_.value.path.endsWith(".ts")) {
			extension = ".ts";
		} else {
			throw new Error("Invalid extension.");
		}
		return `/${data}${extension}`;
	}
};

/** Convert a TypeScript file name to a module. */
export let moduleFromFileName = (fileName: string): Module => {
	let module_: Module;
	if (fileName.startsWith("/library/")) {
		let path = fileName.slice(9);
		module_ = { kind: "library", value: { path } };
	} else {
		let data = fileName.slice(1, -3);
		module_ = syscall.encoding.json.decode(
			syscall.encoding.utf8.decode(syscall.encoding.hex.decode(data)),
		) as Module;
	}
	return module_;
};
