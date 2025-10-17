import * as tg from "./index.ts";

export type Diagnostic = {
	location?: tg.Location;
	message: string;
	severity: Diagnostic.Severity;
};

export namespace Diagnostic {
	export type Severity = "error" | "warning" | "info" | "hint";

	export type Data = {
		location?: tg.Location.Data;
		message: string;
		severity: Diagnostic.Severity;
	};

	export let toData = (value: tg.Diagnostic): tg.Diagnostic.Data => {
		let data: tg.Diagnostic.Data = {
			message: value.message,
			severity: value.severity,
		};
		if (value.location !== undefined) {
			data.location = tg.Location.toData(value.location);
		}
		return data;
	};

	export let fromData = (data: tg.Diagnostic.Data): tg.Diagnostic => {
		let diagnostic: tg.Diagnostic = {
			message: data.message,
			severity: data.severity,
		};
		if (data.location !== undefined) {
			diagnostic.location = tg.Location.fromData(data.location);
		}
		return diagnostic;
	};
}
