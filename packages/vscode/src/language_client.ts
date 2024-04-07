import * as path from "node:path";
import * as vscode from "vscode";
import type { OutputChannel } from "vscode";
import {
	LanguageClient,
	type LanguageClientOptions,
	type ServerOptions,
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
				{ language: "tangram-typescript", scheme: "file" },
				{ language: "tangram-typescript", scheme: "tg" },
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
