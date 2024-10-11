import * as path from "node:path";
import * as vscode from "vscode";
import type { OutputChannel } from "vscode";
import {
	LanguageClient,
	type LanguageClientOptions,
	type ServerOptions,
} from "vscode-languageclient/node";

let languageClient: TangramLanguageClient;

export let activate = async (context: vscode.ExtensionContext) => {
	// Create the language client.
	languageClient = new TangramLanguageClient();

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

class TangramLanguageClient {
	languageClient: LanguageClient | undefined = undefined;
	outputChannel: OutputChannel;

	constructor() {
		this.outputChannel = vscode.window.createOutputChannel(
			"Tangram Language Server",
		);
	}

	async start() {
		if (this.languageClient != null) {
			return;
		}

		let tangramConfig = vscode.workspace.getConfiguration("tangram");

		let defaultPath = path.join(process.env.HOME!, ".tangram/bin/tg");

		let serverOptions: ServerOptions = {
			command: tangramConfig.get<string>("path", defaultPath),
			args: ["lsp"],
			options: {
				env: {
					...process.env,
				},
			},
		};
		let clientOptions: LanguageClientOptions = {
			diagnosticCollectionName: "tangram",
			documentSelector: [
				{ language: "tangram-javascript", scheme: "file" },
				{ language: "tangram-typescript", scheme: "file" },
			],
			outputChannel: this.outputChannel,
		};

		this.languageClient = new LanguageClient(
			"tangram",
			"Tangram Language Server",
			serverOptions,
			clientOptions,
		);

		await this.languageClient.start();
	}

	async stop() {
		if (!this.languageClient) {
			return;
		}
		await this.languageClient.stop();
		this.languageClient = undefined;
	}

	async restart() {
		await this.stop();
		await this.start();
	}
}
