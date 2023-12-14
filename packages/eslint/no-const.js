module.exports = {
	create: (context) => ({
		VariableDeclaration: (node) => {
			if (!(node.type === "VariableDeclaration")) {
				throw Error();
			}
			if (node.kind == "const") {
				context.report({
					fix: (fixer) => {
						let constToken = context
							.getSourceCode()
							.getFirstToken(node, { filter: (t) => t.value === "const" });
						return fixer.replaceText(constToken, "let");
					},
					loc: node.loc,
					message: "Use let instead of const.",
					node,
				});
			}
		},
	}),
	meta: {
		fixable: "code",
	},
};
