require("@rushstack/eslint-patch/modern-module-resolution");

module.exports = {
	overrides: [
		{
			files: ["*.json"],
			parser: "jsonc-eslint-parser",
		},
	],
	parser: "@typescript-eslint/parser",
	parserOptions: {
		sourceType: "module",
	},
	plugins: ["@tangramdotdev", "@typescript-eslint", "jsonc"],
	root: true,
	rules: {
		"@next/next/no-html-link-for-pages": "off",
		"@tangramdotdev/no-const": "error",
		"@tangramdotdev/sort-imports": "error",
		// "@tangramdotdev/sort-keys": "error",
		"@typescript-eslint/array-type": ["error", { default: "generic" }],
		"no-duplicate-imports": "error",
		"no-restricted-syntax": ["error", "WithStatement"],
		"object-shorthand": "error",
		"sort-imports": ["error", { ignoreDeclarationSort: true }],
		"jsonc/sort-keys": "error",
	},
	settings: {
		"mdx/code-blocks": true,
	},
};
