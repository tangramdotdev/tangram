import ts from "typescript";
import type { Diagnostic } from "./diagnostics.ts";
import type { Module } from "./module.ts";
import * as typescript from "./typescript.ts";

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
		].map(typescript.convertDiagnostic),
	);

	// Collect the diagnostics that the compiler reports for each module, such as warnings about exports.
	for (let file of program.getSourceFiles()) {
		if (!file.isDeclarationFile) {
			let module = typescript.moduleFromFileName(file.fileName);
			diagnostics.push(...syscall("module_diagnostics", module, file.text));
		}
	}

	return { diagnostics };
};
