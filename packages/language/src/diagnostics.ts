import type { Location } from "./location.ts";
import * as typescript from "./typescript.ts";

export type Request = unknown;

export type Response = {
	diagnostics: Array<Diagnostic>;
};

export type Diagnostic = {
	location: Location | null;
	severity: Severity;
	message: string;
};

export type Severity = "error" | "warning" | "information" | "hint";

export let handle = (_request: Request): Response => {
	// Get the modules for all documents.
	let modules = syscall("documents");

	// Collect the diagnostics.
	let diagnostics: Array<Diagnostic> = [];
	for (let module_ of modules) {
		let fileName = typescript.fileNameFromModule(module_);
		diagnostics.push(
			...[
				...typescript.languageService.getSyntacticDiagnostics(fileName),
				...typescript.languageService.getSemanticDiagnostics(fileName),
				...typescript.languageService.getSuggestionDiagnostics(fileName),
			].map(typescript.convertDiagnostic),
		);
	}

	return {
		diagnostics,
	};
};
