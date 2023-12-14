import {
	Diagnostic,
	convertDiagnosticFromESLint,
	convertDiagnosticFromTypeScript,
} from "./diagnostics.ts";
import * as eslint from "./eslint.ts";
import { Module } from "./module.ts";
import * as syscall from "./syscall.ts";
import * as typescript from "./typescript.ts";
import ts from "typescript";

export type Request = {
	modules: Array<Module>;
};

export type Response = {
	diagnostics: Array<Diagnostic>;
};

export let handle = (request: Request): Response => {
	let diagnostics = [];

	// Create a typescript program.
	let program = ts.createProgram({
		rootNames: request.modules.map(typescript.fileNameFromModule),
		options: typescript.compilerOptions,
		host: typescript.host,
	});

	// Collect the TypeScript diagnostics.
	diagnostics.push(
		...[
			...program.getConfigFileParsingDiagnostics(),
			...program.getOptionsDiagnostics(),
			...program.getGlobalDiagnostics(),
			...program.getDeclarationDiagnostics(),
			...program.getSyntacticDiagnostics(),
			...program.getSemanticDiagnostics(),
		].map(convertDiagnosticFromTypeScript),
	);

	// Collect the ESLint diagnostics.
	let modules = program
		.getSourceFiles()
		.map((sourceFile) => typescript.moduleFromFileName(sourceFile.fileName));
	for (let module_ of modules) {
		diagnostics.push(
			...eslint.linter
				.verify(
					syscall.module_.load(module_),
					eslint.createConfig(program),
					typescript.fileNameFromModule(module_),
				)
				.map((lintMessage) =>
					convertDiagnosticFromESLint(module_, lintMessage),
				),
		);
	}

	return { diagnostics };
};
