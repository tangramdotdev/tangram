import * as vscode from "vscode";
import { OutputChannel } from "vscode";
import {
	LanguageClient,
	LanguageClientOptions,
	ServerOptions,
} from "vscode-languageclient/node";

export class TangramLanguageClient {
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

		// Get the tangram configuration.
		let tangramConfig = vscode.workspace.getConfiguration("tangram");

		let serverOptions: ServerOptions = {
			command: tangramConfig.get<string>("path", "tg"),
			args: ["lsp"],
			options: {
				env: {
					...process.env,
					TANGRAM_TRACING: tangramConfig.get<string>("tracing", ""),
				},
			},
		};
		let clientOptions: LanguageClientOptions = {
			diagnosticCollectionName: "tangram",
			documentSelector: [
				{ language: "tangram-typescript", scheme: "file" },
				{ language: "tangram-typescript", scheme: "tangram" },
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
