import type { Location } from "./location.ts";
import * as typescript from "./typescript.ts";

export type Request = unknown;

export type Response = {
	diagnostics: { [key: string]: Array<Diagnostic> };
};

export type Diagnostic = {
	location: Location | null;
	severity: Severity;
	message: string;
};

export type Severity = "error" | "warning" | "info" | "hint";

export let handle = (_request: Request): Response => {
	// Get the documents.
	let documents = syscall("document_list");

	// Collect the diagnostics.
	let diagnostics = Object.fromEntries(
		documents.map((module) => {
			let fileName = typescript.fileNameFromModule(module);
			let diagnostics = [
				...typescript.languageService.getSyntacticDiagnostics(fileName),
				...typescript.languageService.getSemanticDiagnostics(fileName),
				...typescript.languageService.getSuggestionDiagnostics(fileName),
			].map(typescript.convertDiagnostic);
			return [module, diagnostics];
		}),
	);

	return {
		diagnostics,
	};
};
