import type { Location } from "./location.ts";
import { Module } from "./module.ts";
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
	// Get the modules for all documents.
	let modules = syscall("document_list");

	// Collect the diagnostics.
	let diagnostics = Object.fromEntries(
		modules.map((module_) => {
			let fileName = typescript.fileNameFromModule(module_);
			let diagnostics = [
				...typescript.languageService.getSyntacticDiagnostics(fileName),
				...typescript.languageService.getSemanticDiagnostics(fileName),
				...typescript.languageService.getSuggestionDiagnostics(fileName),
			].map(typescript.convertDiagnostic);
			return [Module.toUrl(module_), diagnostics];
		}),
	);

	return {
		diagnostics,
	};
};
