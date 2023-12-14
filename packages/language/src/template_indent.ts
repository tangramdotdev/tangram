import { TSESTree } from "@typescript-eslint/typescript-estree";
import * as eslint from "eslint";
import * as estree from "estree";

let rule: eslint.Rule.RuleModule = {
	meta: {
		fixable: "code",
	},

	create: (context) => {
		let isWhitespace = (char: string): boolean => {
			return char === " " || char === "\t";
		};

		let getIndentation = (line: string): string => {
			let whitespaceEnd = 0;

			// Keep incrementing whitespaceEnd until we hit a non-whitespace character.
			while (isWhitespace(line.charAt(whitespaceEnd))) {
				whitespaceEnd++;
			}

			return line.slice(0, whitespaceEnd);
		};

		let reindent = (
			node: TSESTree.TaggedTemplateExpression,
			baseIndentation: string,
		): string | undefined => {
			let isMultiLine = node.quasi.quasis.some(
				(element) => element.value.raw.indexOf("\n") !== -1,
			);

			if (isMultiLine) {
				return reindentTemplateMultiLine(node, baseIndentation);
			} else {
				return reindentTemplateSingleLine(node);
			}
		};

		/**
		 * Render a tagged template ESTree node as a string, but replace the string literal elements with the provided elements instead. This is used to change the text literals of a template without changing the expressions.
		 */
		let renderTaggedTemplateWithElements = (
			template: TSESTree.TaggedTemplateExpression,
			newElements: Array<string>,
		): string => {
			// Build the text of the template literal to replace the original. Start with the opening backtick with the template tag.
			let newNodeText = "t`";

			// Add each element, along with the expression that goes next to it (if any).
			newElements.forEach((element, elementIndex) => {
				let expression = template.quasi.expressions.at(elementIndex);

				newNodeText += element;

				// Add the expression if it exists. Note that this doesn't preserve leading or trailing between the expression and the surrounding "${}" syntax.
				if (expression != null) {
					newNodeText += "${";
					newNodeText += context.sourceCode.getText(expression as estree.Node);
					newNodeText += "}";
				}
			});

			// Add the closing backtick.
			newNodeText += "`";

			return newNodeText;
		};

		let reindentTemplateSingleLine = (
			node: TSESTree.TaggedTemplateExpression,
		): string | undefined => {
			// Construct a new list of template elements, and track if any elements are different.
			let newElements: Array<string> = [];
			let elementsChanged = false;

			let numElements = node.quasi.quasis.length;
			node.quasi.quasis.forEach((element, elementIndex) => {
				let isFirstElement = elementIndex === 0;
				let isLastElement = elementIndex === numElements - 1;

				// Start with the current element text.
				let elementText = element.value.raw;
				let newElementText = elementText;

				// Trim the start for the first element.
				// t` foo ${...}` -> t`foo ${...}`
				if (isFirstElement) {
					newElementText = newElementText.trimStart();
				}

				// Trim the end for the last element.
				// t`${...} foo ` -> t`${...} foo`
				if (isLastElement) {
					newElementText = newElementText.trimEnd();
				}

				// If the element text changed, then we need to re-render the template.
				if (newElementText !== elementText) {
					elementsChanged = true;
				}

				// Push the updated element.
				newElements.push(newElementText);
			});

			if (elementsChanged) {
				return renderTaggedTemplateWithElements(node, newElements);
			} else {
				return;
			}
		};

		let reindentTemplateMultiLine = (
			node: TSESTree.TaggedTemplateExpression,
			baseIndentation: string,
		): string | undefined => {
			// Add an extra tab so things get indented by one more level.
			let newTemplateIndentation = `${baseIndentation}\t`;

			// Determine the current indentation of the template by finding the shortest string of ' ' and '\t' characters at the start of each line.
			let currentTemplateIndentations = node.quasi.quasis.flatMap((element) => {
				let elementLines = element.value.raw.split("\n");
				let elementIndentations = elementLines.slice(1).flatMap((line) => {
					// Skip empty lines.
					if (line.trim() === "") {
						return [];
					}

					// Get the indentation.
					return [getIndentation(line)];
				});

				return elementIndentations;
			});
			currentTemplateIndentations.sort((a, b) => a.length - b.length);
			let currentTemplateIndentation = currentTemplateIndentations.at(0) ?? "";

			// Construct a new list of template elements, and track if any elements are different.
			let newElements: Array<string> = [];
			let elementsChanged = false;

			let numElements = node.quasi.quasis.length;
			node.quasi.quasis.forEach((element, elementIndex) => {
				let isFirstElement = elementIndex === 0;
				let isLastElement = elementIndex === numElements - 1;

				let elementText = element.value.raw;

				// Split on newlines.
				let elementTextLines = elementText.split("\n");

				// Add a starting newline to this element if this either if the first line is not empty (meaning there's content on the first line), or if the there is only one line (in a multiline template, that means the template has an interpolated expression on the first line).
				// t`foo${...}` -> t`\nfoo${...}`
				// t`fizz${buzz}\nfoo` -> t`\nfizz${buz}\nfoo`
				let shouldAddStartNewline =
					isFirstElement &&
					(elementTextLines.length === 1 || elementTextLines[0]?.trim() !== "");

				// Add an ending newline to this element if this is the last element.
				// t`${...}foo` -> t`${...}foo\n`
				let shouldAddEndNewline = isLastElement;

				// Adjust the indentation of each line of the element.
				let newElementLines = elementTextLines.map((line, lineIndex) => {
					let isFirstLine = lineIndex === 0;
					let isLastLine = lineIndex === elementTextLines.length - 1;

					// If this is the first line but not the first element, this means that this line is following an interpolated value. We don't want to indent it because the true start of this line is in a different element.
					// Example: t`\n${foo} bar\n` ('bar\n' is the element in this case; no extra indentation should be added because it's not on its own line).
					if (isFirstLine && !isFirstElement) {
						return line;
					}

					// Empty lines should not be indented normally.
					if (line.trim() === "") {
						// Add one last bit of indentation if this is the last line of the last element. This ensures that the closing backtick is indented properly.
						// \tt`\t\t${foo}\n` -> \tt`\t\t${foo}\n\t`
						if (isLastLine && !isLastElement) {
							return newTemplateIndentation;
						}

						// ...Otherwise, for normal blank lines, return an empty string. This strips extra trailing whitespace.
						return "";
					}

					// Calculate the byte offsets where the current baseline indentation ends and the line content starts. Note that the "content" in this case can still include extra indentation past the baseline that we want to preserve.
					let contentStartAfterIndentation = 0;
					while (
						contentStartAfterIndentation < currentTemplateIndentation.length &&
						isWhitespace(line.charAt(contentStartAfterIndentation))
					) {
						contentStartAfterIndentation++;
					}

					let lineContent = line.slice(contentStartAfterIndentation);

					// Add the new indentation to the start of the line content.
					return `${newTemplateIndentation}${lineContent}`;
				});
				let newElementText = newElementLines.join("\n");

				// Add a newline to the start of the element if necessary.
				if (shouldAddStartNewline) {
					newElementText = `\n${newElementText}`;
				}

				// Add a newline to the end of the element if necessary and if it doesn't already end with one.
				if (shouldAddEndNewline) {
					if (newElementText.charAt(newElementText.length - 1) !== "\n") {
						newElementText = `${newElementText}\n`;
					}

					// For the last line of the last element, add extra indentation so the closing backtick lines up with the opening backtick properly.
					newElementText += baseIndentation;
				}

				if (newElementText === elementText) {
					// If the element text already has the correct indentation, then keep it the same.
					newElements.push(elementText);
				} else {
					// Otherwise, replace the element with the new text.
					elementsChanged = true;
					newElements.push(newElementText);
				}
			});

			if (elementsChanged) {
				// Build the text of the template literal to replace the original. Start with the opening backtick with the template tag.
				let newNodeText = "t`";

				// Add each element, along with the expression that goes next to it (if any).
				newElements.forEach((element, elementIndex) => {
					let expression = node.quasi.expressions.at(elementIndex);

					newNodeText += element;

					// Add the expression if it exists. Note that this doesn't preserve leading or trailing between the expression and the surrounding "${}" syntax.
					if (expression != null) {
						newNodeText += "${";
						newNodeText += context.sourceCode.getText(
							expression as estree.Node,
						);
						newNodeText += "}";
					}
				});

				// Add the closing backtick.
				newNodeText += "`";

				return newNodeText;
			} else {
				return;
			}
		};

		let throwError = (message?: string): never => {
			throw new Error(message);
		};

		return {
			TaggedTemplateExpression: (node) => {
				// Find a tagged template expression whose tag is `t`.
				if (node.tag.type !== "Identifier" || node.tag.name !== "t") {
					return;
				}

				// Get the range the location.
				let nodeRange = node.range ?? throwError();
				let nodeLoc = node.loc ?? throwError();

				// Get the line where the template is defined.
				let [templateStartIndex, _] = nodeRange;
				let textToTemplateStart = context.sourceCode.text.slice(
					0,
					templateStartIndex,
				);
				let templateStartLine = textToTemplateStart.split("\n").at(-1) ?? "";

				// Get the indentation.
				let indentation = getIndentation(templateStartLine);

				// Perform the reindentation.
				let reindentedTemplate = reindent(
					node as TSESTree.TaggedTemplateExpression,
					indentation,
				);

				// Report an error with a fixer if necessary.
				if (reindentedTemplate !== undefined) {
					let newText = reindentedTemplate;
					context.report({
						message: "Expected the template to be properly indented.",
						loc: nodeLoc,
						fix: (fixer) => {
							return fixer.replaceTextRange(nodeRange, newText);
						},
					});
				}
			},
		};
	},
};

export default rule;
