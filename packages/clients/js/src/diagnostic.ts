import * as tg from "./index.ts";

export type Diagnostic = {
	location: tg.Module.Location | null;
	message: string;
	severity: Diagnostic.Severity;
};

export namespace Diagnostic {
	export type Severity = "error" | "warning" | "info" | "hint";

	export type Data = {
		location?: tg.Module.Location.Data | null;
		message: string;
		severity: Diagnostic.Severity;
	};

	export let toData = (value: tg.Diagnostic): tg.Diagnostic.Data => {
		let data: tg.Diagnostic.Data = {
			message: value.message,
			severity: value.severity,
		};
		if (value.location !== undefined && value.location !== null) {
			data.location = tg.Module.Location.toData(value.location);
		}
		return data;
	};

	export let fromData = (data: tg.Diagnostic.Data): tg.Diagnostic => {
		let diagnostic: tg.Diagnostic = {
			location:
				data.location !== undefined && data.location !== null
					? tg.Module.Location.fromData(data.location)
					: null,
			message: data.message,
			severity: data.severity,
		};
		return diagnostic;
	};

	export let children = (value: tg.Diagnostic): Array<tg.Object> => {
		if (value.location !== undefined && value.location !== null) {
			return tg.Module.Location.children(value.location);
		} else {
			return [];
		}
	};

	export namespace Data {
		export let children = (data: tg.Diagnostic.Data): Array<tg.Object.Id> => {
			if (data.location !== undefined && data.location !== null) {
				return tg.Module.Location.Data.children(data.location);
			} else {
				return [];
			}
		};

		export let withoutTokens = (
			data: tg.Diagnostic.Data,
		): tg.Diagnostic.Data => {
			if (data.location !== undefined && data.location !== null) {
				data.location = tg.Module.Location.Data.withoutTokens(data.location);
			}
			return data;
		};
	}
}
