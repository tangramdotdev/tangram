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
	root: dir_01661vx5rfw3ebf2r3m8ksqy9sq5c4jmmymccapwwxamxydqspvzc0
	└╴middle/1.0.0: dir_018kdbjpswww54x0f4qcv0gmz3arkmf3pf3kg9sy0g61bke3w1nedg
	  ├╴ghost/^1: null
	  └╴leaf/1.0.0: dir_010amcfrw5b59n7arj7s890qbarq3fpc0jmzhpqchjmn1y485m1mm0
'
