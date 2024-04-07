import * as check from "./check.ts";
import * as completion from "./completion.ts";
import * as definition from "./definition.ts";
import * as diagnostics from "./diagnostics.ts";
import * as doc from "./doc.ts";
import { Error_ } from "./error.ts";
import * as hover from "./hover.ts";
import { log } from "./log.ts";
import * as references from "./references.ts";
import * as rename from "./rename.ts";
import * as symbols from "./symbols.ts";

Object.defineProperties(globalThis, {
	console: { value: { log } },
});

type Request =
	| { kind: "check"; request: check.Request }
	| { kind: "completion"; request: completion.Request }
	| { kind: "definition"; request: definition.Request }
	| { kind: "diagnostics"; request: diagnostics.Request }
	| { kind: "doc"; request: doc.Request }
	| { kind: "hover"; request: hover.Request }
	| { kind: "references"; request: references.Request }
	| { kind: "rename"; request: rename.Request }
	| { kind: "symbols"; request: symbols.Request };

type Response =
	| { kind: "check"; response: check.Response }
	| { kind: "completion"; response: completion.Response }
	| { kind: "definition"; response: definition.Response }
	| { kind: "diagnostics"; response: diagnostics.Response }
	| { kind: "doc"; response: doc.Response }
	| { kind: "hover"; response: hover.Response }
	| { kind: "references"; response: references.Response }
	| { kind: "rename"; response: rename.Response }
	| { kind: "symbols"; response: symbols.Response };

let handle = async ({ kind, request }: Request): Promise<Response> => {
	switch (kind) {
		case "check": {
			let response = check.handle(request);
			return { kind: "check", response };
		}
		case "completion": {
			let response = completion.handle(request);
			return { kind: "completion", response };
		}
		case "definition": {
			let response = definition.handle(request);
			return { kind: "definition", response };
		}
		case "diagnostics": {
			let response = diagnostics.handle(request);
			return { kind: "diagnostics", response };
		}
		case "doc": {
			let response = doc.handle(request);
			return { kind: "doc", response };
		}
		case "hover": {
			let response = hover.handle(request);
			return { kind: "hover", response };
		}
		case "references": {
			let response = references.handle(request);
			return { kind: "references", response };
		}
		case "rename": {
			let response = rename.handle(request);
			return { kind: "rename", response };
		}
		case "symbols": {
			let response = symbols.handle(request);
			return { kind: "symbols", response };
		}
	}
};

let language = {
	Error: Error_,
	handle,
};

Object.defineProperties(globalThis, {
	language: { value: language },
});
