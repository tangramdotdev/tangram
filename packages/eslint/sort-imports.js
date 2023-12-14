module.exports = {
	create: (context) => ({
		Program: (node) => {
			if (node.type !== "Program") {
				throw Error();
			}
			let erroredBlocks = findErroredBlocks(node);
			reportErrors(context, erroredBlocks);
		},
	}),
	meta: {
		fixable: "code",
	},
};

let findErroredBlocks = (node) => {
	let erroredBlocks = [];
	let currentBlock = [];
	let currentBlockHasError = false;
	for (let i = 0; i < node.body.length - 1; i++) {
		let currentNode = node.body[i];
		let previousDeclaration = currentBlock[currentBlock.length - 1];
		if (currentNode.type !== "ImportDeclaration") {
			if (currentBlockHasError) {
				erroredBlocks.push(currentBlock);
			}
			currentBlock = [];
			currentBlockHasError = false;
		} else if (
			currentNode.loc &&
			previousDeclaration &&
			previousDeclaration.loc &&
			previousDeclaration.loc.end.line + 1 != currentNode.loc.start.line
		) {
			if (currentBlockHasError) {
				erroredBlocks.push(currentBlock);
			}
			currentBlock = [currentNode];
			currentBlockHasError = false;
		} else {
			currentBlock.push(currentNode);
			if (
				previousDeclaration &&
				compare(previousDeclaration, currentNode) > 0
			) {
				currentBlockHasError = true;
			}
		}
	}
	if (currentBlock.length > 0 && currentBlockHasError) {
		erroredBlocks.push(currentBlock);
	}
	return erroredBlocks;
};

let reportErrors = (context, erroredBlocks) => {
	for (let block of erroredBlocks) {
		let firstDeclaration = block[0];
		let lastDeclaration = block[block.length - 1];
		if (!firstDeclaration.loc || !lastDeclaration.loc) {
			throw Error();
		}
		context.report({
			fix: (fixer) =>
				block
					.slice()
					.sort(compare)
					.map((Declaration, i) => ({
						newDeclaration: Declaration,
						oldDeclaration: block[i],
					}))
					.map(({ newDeclaration, oldDeclaration }) => {
						if (!oldDeclaration.range || !newDeclaration.range) {
							throw Error();
						}
						let sourceCode = context.getSourceCode();
						let oldDeclarationRange = [
							sourceCode.getCommentsBefore(oldDeclaration).at(0)?.range[0] ??
								oldDeclaration.range[0],
							oldDeclaration.range[1],
						];
						let newDeclarationRange = [
							sourceCode.getCommentsBefore(newDeclaration).at(0)?.range[0] ??
								newDeclaration.range[0],
							newDeclaration.range[1],
						];
						return fixer.replaceTextRange(
							oldDeclarationRange,
							context
								.getSourceCode()
								.getText()
								.slice(...newDeclarationRange),
						);
					}),
			loc: {
				end: lastDeclaration.loc.end,
				start: firstDeclaration.loc.start,
			},
			message: "Expected module declarations to be sorted",
		});
	}
};

let compare = (a, b) => {
	if (
		typeof a.source.value !== "string" ||
		typeof b.source.value !== "string"
	) {
		return 0;
	}
	if (a.source.value < b.source.value) {
		return -1;
	} else if (a.source.value > b.source.value) {
		return 1;
	} else {
		return 0;
	}
};
