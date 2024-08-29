import ts from "typescript";
import type { Diagnostic } from "./diagnostics.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	modules: Array<string>;
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

	return { diagnostics };
};
