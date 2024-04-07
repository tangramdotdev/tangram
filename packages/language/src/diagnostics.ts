import ts from "typescript";
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
			].map(convertDiagnosticFromTypeScript),
		);
	}

	return {
		diagnostics,
	};
};

/** Convert a diagnostic from TypeScript. */
export let convertDiagnosticFromTypeScript = (
	diagnostic: ts.Diagnostic,
): Diagnostic => {
	// Get the diagnostic's location.
	let location = null;
	if (
		diagnostic.file !== undefined &&
		diagnostic.start !== undefined &&
		diagnostic.length !== undefined
	) {
		// Get the diagnostic's module.
		let module_ = typescript.moduleFromFileName(diagnostic.file.fileName);

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
			severity = "information";
			break;
		}
		default: {
			throw new Error("unknown diagnostic category");
		}
	}

	let message: string;
	// Map diagnostics for '.ts' extensions to import errors instead.
	if (diagnostic.code === 2691) {
		// TS2691: An import path cannot end with a '.ts' extension. Consider importing 'bad-module' instead.
		message = "could not load the module";
	} else if (diagnostic.code === 2792) {
		// TS2792: Cannot find module. Did you mean to set the 'moduleResolution' option to 'node', or to add aliases to the 'paths' option?
		message = "could not load the module";
	} else {
		// Get the diagnostic's message.
		message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
	}

	return {
		location,
		severity,
		message,
	};
};
