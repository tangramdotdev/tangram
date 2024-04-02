import * as eslint from "./eslint.ts";
import * as eslint_ from "eslint";
import * as prettierPluginEstree from "prettier/plugins/estree";
import * as prettierPluginTypescript from "prettier/plugins/typescript";
import * as prettier from "prettier/standalone";

export type Request = {
	text: string;
};

export type Response = {
	text: string;
};

// Create the Prettier options.
let prettierOptions = {
	useTabs: true,
	parser: "typescript",
	plugins: [prettierPluginEstree, prettierPluginTypescript],
};

// Create the ESLint config.
let eslintConfig: eslint_.Linter.Config = {
	rules: {
		"@tangramdotdev/sort-imports": "warn",
		"sort-imports": ["warn", { ignoreDeclarationSort: true }],
		semi: "warn",
	},
	parser: "@typescript-eslint/parser",
};

export let handle = async (request: Request): Promise<Response> => {
	let text = request.text;

	// Format the text with Prettier.
	try {
		text = await prettier.format(text, prettierOptions);
	} catch {
		// Ignore errors.
	}

	try {
		// Run ESLint and fix any errors.
		let eslintOutput = eslint.linter.verifyAndFix(text, eslintConfig);
		text = eslintOutput.output;
	} catch (e) {
		// Ignore errors.
	}

	return { text };
};
