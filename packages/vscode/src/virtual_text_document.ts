import { TangramLanguageClient } from "./language_client";
import * as vscode from "vscode";
import * as vscodeLanguageClient from "vscode-languageclient";

export interface VirtualTextDocumentParams {
	textDocument: vscodeLanguageClient.TextDocumentIdentifier;
}

export class TangramTextDocumentContentProvider
	implements vscode.TextDocumentContentProvider
{
	constructor(private client: TangramLanguageClient) {}

	provideTextDocumentContent(
		uri: vscode.Uri,
		token: vscode.CancellationToken,
	): vscode.ProviderResult<string> {
		if (!this.client.languageClient) {
			throw new Error("The Tangram language server has not started.");
		}

		let virtualTextDocumentRequest = new vscodeLanguageClient.RequestType<
			VirtualTextDocumentParams,
			string,
			void
		>("tangram/virtualTextDocument");

		return this.client.languageClient.sendRequest(
			virtualTextDocumentRequest,
			{ textDocument: { uri: uri.toString() } },
			token,
		);
	}
}
