import * as callHierarchy from "./call_hierarchy.ts";
import * as check from "./check.ts";
import * as codeAction from "./code_action.ts";
import * as completion from "./completion.ts";
import * as definition from "./definition.ts";
import * as diagnostics from "./diagnostics.ts";
import * as documentHighlight from "./document_highlight.ts";
import * as documentLink from "./document_link.ts";
import * as document from "./document.ts";
import * as foldingRange from "./folding_range.ts";
import * as hover from "./hover.ts";
import * as implementation from "./implementation.ts";
import * as inlayHint from "./inlay_hint.ts";
import * as prepareRename from "./prepare_rename.ts";
import * as references from "./references.ts";
import * as rename from "./rename.ts";
import * as selectionRange from "./selection_range.ts";
import * as semanticTokens from "./semantic_tokens.ts";
import * as signatureHelp from "./signature_help.ts";
import * as symbols from "./symbols.ts";
import * as workspaceSymbol from "./workspace_symbol.ts";

type Request =
	| { kind: "call_hierarchy_incoming"; request: callHierarchy.IncomingRequest }
	| { kind: "call_hierarchy_outgoing"; request: callHierarchy.OutgoingRequest }
	| { kind: "call_hierarchy_prepare"; request: callHierarchy.PrepareRequest }
	| { kind: "check"; request: check.Request }
	| { kind: "code_action"; request: codeAction.Request }
	| { kind: "completion"; request: completion.Request }
	| { kind: "completion_resolve"; request: completion.ResolveRequest }
	| { kind: "declaration"; request: definition.Request }
	| { kind: "definition"; request: definition.Request }
	| { kind: "document"; request: document.Request }
	| { kind: "document_diagnostics"; request: diagnostics.Request }
	| { kind: "document_highlight"; request: documentHighlight.Request }
	| { kind: "document_link"; request: documentLink.Request }
	| { kind: "folding_range"; request: foldingRange.Request }
	| { kind: "hover"; request: hover.Request }
	| { kind: "implementation"; request: implementation.Request }
	| { kind: "inlay_hint"; request: inlayHint.Request }
	| { kind: "prepare_rename"; request: prepareRename.Request }
	| { kind: "references"; request: references.Request }
	| { kind: "rename"; request: rename.Request }
	| { kind: "selection_range"; request: selectionRange.Request }
	| { kind: "semantic_tokens"; request: semanticTokens.Request }
	| { kind: "signature_help"; request: signatureHelp.Request }
	| { kind: "symbols"; request: symbols.Request }
	| { kind: "type_definition"; request: definition.Request }
	| { kind: "workspace_symbol"; request: workspaceSymbol.Request };

type Response =
	| {
			kind: "call_hierarchy_incoming";
			response: callHierarchy.IncomingResponse;
	  }
	| {
			kind: "call_hierarchy_outgoing";
			response: callHierarchy.OutgoingResponse;
	  }
	| {
			kind: "call_hierarchy_prepare";
			response: callHierarchy.PrepareResponse;
	  }
	| { kind: "check"; response: check.Response }
	| { kind: "code_action"; response: codeAction.Response }
	| { kind: "completion"; response: completion.Response }
	| { kind: "completion_resolve"; response: completion.ResolveResponse }
	| { kind: "declaration"; response: definition.Response }
	| { kind: "definition"; response: definition.Response }
	| { kind: "document"; response: document.Response }
	| { kind: "document_diagnostics"; response: diagnostics.Response }
	| { kind: "document_highlight"; response: documentHighlight.Response }
	| { kind: "document_link"; response: documentLink.Response }
	| { kind: "folding_range"; response: foldingRange.Response }
	| { kind: "hover"; response: hover.Response }
	| { kind: "implementation"; response: implementation.Response }
	| { kind: "inlay_hint"; response: inlayHint.Response }
	| { kind: "prepare_rename"; response: prepareRename.Response }
	| { kind: "references"; response: references.Response }
	| { kind: "rename"; response: rename.Response }
	| { kind: "selection_range"; response: selectionRange.Response }
	| { kind: "semantic_tokens"; response: semanticTokens.Response }
	| { kind: "signature_help"; response: signatureHelp.Response }
	| { kind: "symbols"; response: symbols.Response }
	| { kind: "type_definition"; response: definition.Response }
	| { kind: "workspace_symbol"; response: workspaceSymbol.Response };

let handle = ({ kind, request }: Request): Response => {
	switch (kind) {
		case "call_hierarchy_incoming": {
			let response = callHierarchy.handleIncoming(request);
			return { kind: "call_hierarchy_incoming", response };
		}
		case "call_hierarchy_outgoing": {
			let response = callHierarchy.handleOutgoing(request);
			return { kind: "call_hierarchy_outgoing", response };
		}
		case "call_hierarchy_prepare": {
			let response = callHierarchy.handlePrepare(request);
			return { kind: "call_hierarchy_prepare", response };
		}
		case "check": {
			let response = check.handle(request);
			return { kind: "check", response };
		}
		case "code_action": {
			let response = codeAction.handle(request);
			return { kind: "code_action", response };
		}
		case "completion": {
			let response = completion.handle(request);
			return { kind: "completion", response };
		}
		case "completion_resolve": {
			let response = completion.handleResolve(request);
			return { kind: "completion_resolve", response };
		}
		case "declaration": {
			let response = definition.handleDeclaration(request);
			return { kind: "declaration", response };
		}
		case "definition": {
			let response = definition.handle(request);
			return { kind: "definition", response };
		}
		case "document": {
			let response = document.handle(request);
			return { kind: "document", response };
		}
		case "document_diagnostics": {
			let response = diagnostics.handle(request);
			return { kind: "document_diagnostics", response };
		}
		case "document_highlight": {
			let response = documentHighlight.handle(request);
			return { kind: "document_highlight", response };
		}
		case "document_link": {
			let response = documentLink.handle(request);
			return { kind: "document_link", response };
		}
		case "folding_range": {
			let response = foldingRange.handle(request);
			return { kind: "folding_range", response };
		}
		case "hover": {
			let response = hover.handle(request);
			return { kind: "hover", response };
		}
		case "implementation": {
			let response = implementation.handle(request);
			return { kind: "implementation", response };
		}
		case "inlay_hint": {
			let response = inlayHint.handle(request);
			return { kind: "inlay_hint", response };
		}
		case "prepare_rename": {
			let response = prepareRename.handle(request);
			return { kind: "prepare_rename", response };
		}
		case "references": {
			let response = references.handle(request);
			return { kind: "references", response };
		}
		case "rename": {
			let response = rename.handle(request);
			return { kind: "rename", response };
		}
		case "selection_range": {
			let response = selectionRange.handle(request);
			return { kind: "selection_range", response };
		}
		case "semantic_tokens": {
			let response = semanticTokens.handle(request);
			return { kind: "semantic_tokens", response };
		}
		case "signature_help": {
			let response = signatureHelp.handle(request);
			return { kind: "signature_help", response };
		}
		case "symbols": {
			let response = symbols.handle(request);
			return { kind: "symbols", response };
		}
		case "type_definition": {
			let response = definition.handleTypeDefinition(request);
			return { kind: "type_definition", response };
		}
		case "workspace_symbol": {
			let response = workspaceSymbol.handle(request);
			return { kind: "workspace_symbol", response };
		}
	}
};

Object.defineProperties(globalThis, {
	handle: { value: handle },
});
