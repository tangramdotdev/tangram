// objects children
// (object_id, child_id) -> key
// need to also keep track of parents for each child.
// (child_id, parent_id)-> key
// Need to do range queries to find the parents of a child and the children of a parent.

//objects
// (object_id, 0) complete -> key
// (object_id, 1) count -> key
// (object_id, 2) depth -> key
// (object_id, 3) size -> key
// (object_id, 4) weight
// (object_id, 5) incomplete_children

// Step 1: Set the incomplete children count for an object.
// For all of the object's children, see which onees are not present in the objects table or whose complete field is false. Set the incomplete children count, and keep track of all ids whose incomplete_children count is 0. => initial condition.

// Begin loop
// Step2: Update the count, depth, weight
// For all objects whose incomplete_children count is 0, find all of its children, grab their count, depth, and weight and add them up to get the count, depth, and weight of the parent.

// Step 3. For all objects who are now complete, find their parents and decrement their incomplete children count. All items whose incomplete_children count is 0 are now eligible for update themselves.
