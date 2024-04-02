import * as tangramEslintPlugin from "@tangramdotdev/eslint-plugin";
import * as typescriptEslintPlugin from "@typescript-eslint/eslint-plugin";
import * as typescriptEslintParser from "@typescript-eslint/parser";
import * as eslint from "eslint";
import ts from "typescript";

// Create an ESLint linter.
export let linter = new eslint.Linter();

// Use the TypeScript ESLint parser.
linter.defineParser(
	"@typescript-eslint/parser",
	typescriptEslintParser as eslint.Linter.ParserModule,
);

// Define the rules.
for (let [name, rule] of Object.entries(typescriptEslintPlugin.rules)) {
	linter.defineRule(
		`@typescript-eslint/${name}`,
		rule as unknown as eslint.Rule.RuleModule,
	);
}
for (let [name, rule] of Object.entries(tangramEslintPlugin.rules)) {
	linter.defineRule(`@tangramdotdev/${name}`, rule);
}

export let createConfig = (program: ts.Program): eslint.Linter.Config => {
	return {
		parser: "@typescript-eslint/parser",
		parserOptions: {
			programs: [program],
		},
		rules: {
			"@tangramdotdev/sort-imports": "warn",
			"sort-imports": ["warn", { ignoreDeclarationSort: true }],
			semi: "warn",
		},
	};
};
