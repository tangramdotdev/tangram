# Get the module input of a JavaScript command with no other object inputs.
export def module-input [command: record] {
	$command.node.args
	| where { |arg| $arg.kind == "value" and $arg.value.kind? == "module" }
	| first
	| get value.value.referent.node
	| split row "?"
	| first
}
