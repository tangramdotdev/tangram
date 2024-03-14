let makeRule = (context) => (node) => {
	let childrenKey;
	if (node.type === "ObjectExpression" || node.type === "ObjectPattern") {
		childrenKey = "properties";
	} else if (node.type === "TSInterfaceBody") {
		childrenKey = "body";
	} else if (node.type === "TSTypeLiteral") {
		childrenKey = "members";
	} else {
		throw Error();
	}
	for (let i = 0; i < node[childrenKey].length - 1; i++) {
		if (compare(node[childrenKey][i], node[childrenKey][i + 1]) > 0) {
			if (!node.loc) {
				continue;
			}
			let fix = (fixer) =>
				node[childrenKey]
					.slice()
					.sort(compare)
					.map((property, i) => ({
						newProperty: property,
						oldProperty: node[childrenKey][i],
					}))
					.map(({ newProperty, oldProperty }) => {
						if (!oldProperty.range || !newProperty.range) {
							throw Error();
						}
						let sourceCode = context.getSourceCode();
						let oldPropertyRange = [
							sourceCode.getCommentsBefore(oldProperty).at(0)?.range[0] ??
								oldProperty.range[0],
							oldProperty.range[1],
						];
						let newPropertyRange = [
							sourceCode.getCommentsBefore(newProperty).at(0)?.range[0] ??
								newProperty.range[0],
							newProperty.range[1],
						];
						return fixer.replaceTextRange(
							oldPropertyRange,
							sourceCode.getText().slice(...newPropertyRange),
						);
					});
			context.report({
				fix,
				loc: node.loc,
				message: "expected keys to be sorted",
				node,
			});
			break;
		}
	}
};

let compare = (a, b) => {
	if (
		!(
			(a.type === "Property" ||
				a.type === "TSPropertySignature" ||
				a.type === "TSMethodSignature") &&
			(b.type === "Property" ||
				b.type === "TSPropertySignature" ||
				b.type === "TSMethodSignature") &&
			a.key.type === "Identifier" &&
			b.key.type === "Identifier"
		)
	) {
		return 0;
	}
	if (a.key.name < b.key.name) {
		return -1;
	} else if (a.key.name > b.key.name) {
		return 1;
	} else {
		return 0;
	}
};

module.exports = {
	create: (context) => ({
		ObjectExpression: makeRule(context),
		ObjectPattern: makeRule(context),
		TSInterfaceBody: makeRule(context),
		TSTypeLiteral: makeRule(context),
	}),
	meta: {
		fixable: "code",
	},
};
