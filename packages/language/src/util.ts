import { TSESTree } from "@typescript-eslint/typescript-estree";
import { visitorKeys } from "@typescript-eslint/visitor-keys";

/** Visit each node in a tree. */
export let visit = (
	node: TSESTree.Node,
	visitor: (node: TSESTree.Node) => void,
) => {
	// Visit the root.
	visitor(node);

	// Visit the children.
	let keys = visitorKeys[node.type];
	if (keys) {
		for (let key of keys) {
			let n = node as any;
			let child: TSESTree.Node | Array<TSESTree.Node> | undefined = n[key];
			if (child instanceof Array) {
				for (let item of child) {
					visit(item, visitor);
				}
			} else if (child) {
				visit(child, visitor);
			}
		}
	}
};
