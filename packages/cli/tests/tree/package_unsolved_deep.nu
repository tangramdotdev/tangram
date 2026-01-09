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
	root: dir_013nes48jz9t47v28g986mvgeez7g315rn032qh6t1vkvzw69kssqg
	└╴middle/1.0.0: dir_01y83qbfqn306vzqa1s4fdvzqxt82zdmp64zk66gj2t0tp98238nqg
	  ├╴ghost/%5E1: null
	  └╴leaf/1.0.0: dir_010amcfrw5b59n7arj7s890qbarq3fpc0jmzhpqchjmn1y485m1mm0
'
