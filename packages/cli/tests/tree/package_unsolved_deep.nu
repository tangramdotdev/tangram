use ../../test.nu *

let server = spawn

let leaf_path = artifact {
	tangram.ts: '// leaf package'
}
tg tag leaf/1.0.0 $leaf_path

let middle_path = artifact {
	tangram.ts: '
		import * as leaf from "leaf/^1";
		import * as ghost from "ghost/^1";
	'
}
let middle_id = tg checkin --unsolved-dependencies $middle_path
tg tag middle/1.0.0 $middle_id

let root_path = artifact {
	tangram.ts: 'import * as middle from "middle/^1"'
}

let id = tg checkin --unsolved-dependencies $root_path
tg tag root $id

let output = tg tree root --kind=package
snapshot $output '
	root: dir_01v952dyrymra6460skyeexw8wzwz7ame7ev4dpbtj91mm7b7zhj20
	└╴middle/1.0.0: dir_01ja3skt3rzg70mqfkhxk4v0k7y3wn0en2jmsrjf83xctvb1b7ypy0
	  ├╴ghost/^1: null
	  └╴leaf/1.0.0: dir_01ekh5fjdrn8d0b0qhtka34p23d3gab0qr932z4fjrmj65kgya6xp0
'
