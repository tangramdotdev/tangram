import type { Location } from "./location.ts";
import type { Module } from "./module.ts";
import * as typescript from "./typescript.ts";

export type Request = {
	modules: Array<Module>;
};

export type Response = {
	diagnostics: Array<Diagnostic>;
};

export type Diagnostic = {
	location: Location | null;
	message: string;
	severity: Severity;
};

export type Severity = "error" | "warning" | "info" | "hint";

export let handle = (request: Request): Response => {
	// Collect the diagnostics.
	let diagnostics = request.modules
		.flatMap((module) => {
			let fileName = typescript.fileNameFromModule(module);
			return [
				...typescript.languageService.getSyntacticDiagnostics(fileName),
				...typescript.languageService.getSemanticDiagnostics(fileName),
				...typescript.languageService.getSuggestionDiagnostics(fileName),
			];
		})
		.map(typescript.convertDiagnostic);
	return {
		diagnostics,
	};
};
