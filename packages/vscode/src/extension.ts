import * as vscode from "vscode";
import { TangramLanguageClient } from "./language_client";
import { TangramTextDocumentContentProvider } from "./virtual_text_document";

// Create the language client.
let languageClient = new TangramLanguageClient();

export let activate = async (context: vscode.ExtensionContext) => {
	// Start the language client.
	languageClient.start();

	// Restart the language client whenever the workspace configuration changes.
	let onDidChangeConfiguration = vscode.workspace.onDidChangeConfiguration(
		async (event) => {
			if (event.affectsConfiguration("tangram")) {
				languageClient.restart();
			}
		},
	);
	context.subscriptions.push(onDidChangeConfiguration);

	// Register the content provider for virtual files.
	context.subscriptions.push(
		vscode.workspace.registerTextDocumentContentProvider(
			"tg",
			new TangramTextDocumentContentProvider(languageClient),
		),
	);

	// Register a command to restart the Tangram language server.
	let restartCommand = vscode.commands.registerCommand(
		"tangram.restartLanguageServer",
		async () => {
			vscode.window.showInformationMessage(
				"Restarting the Tangram language server.",
			);
			await languageClient.restart();
		},
	);
	context.subscriptions.push(restartCommand);
};

export let deactivate = () => {
	languageClient.stop();
};
