module ExpectoTemplate.RRBVectorProps

open System
open Ficus
open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck

module Literals = Ficus.Literals

let isEmpty (node : Node) = node.NodeSize <= 0
// Note: do NOT call isNotTwig or isTwig on empty nodes!
let isNotTwig (node : Node) = node.Array.[0] :? Node
let isTwig (node : Node) = not (isNotTwig node)
let isRRB  (node : Node) = node :? RRBNode
let isFull (node : Node) = not (isRRB node)
let items (node : Node) = if isTwig node then node.Array else [||]
let children (node : Node) = if isNotTwig node then node.Array else [||]

let rec itemCount<'T> (node : Node) =
    if isEmpty node then 0
    elif node |> isTwig then node.Array.[0..node.NodeSize-1] |> Array.sumBy (fun leaf -> (leaf :?> 'T[]).Length)
    else node.Array.[0..node.NodeSize-1] |> Array.sumBy (fun n -> n :?> Node |> itemCount<'T>)

let inline down shift = shift - Literals.blockSizeShift

// Properties start here

// Note that when I check the shift, I use <= 0, because in unit testing, we really might run into
// some deformed trees where the shift isn't a proper multiple of Literals.blockSizeShift. If we
// run into one of those, I don't want to loop forever because we skipped from 1 to -1 and never hit 0.

let properties = [
    "All leaves should be at the same height", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            if shift <= Literals.blockSizeShift then
                not (isEmpty node) && isTwig node
            else
                not (isEmpty node) && isNotTwig node && node.Array.[0 .. node.NodeSize - 1] |> Array.forall (fun n -> n :? Node && check (down shift) (n :?> Node))
        match vec with
        | :? RRBSapling<'T> -> true // Saplings just have root and leaf, both at height 0
        | :? RRBTree<'T> as tree ->
            if tree.Root |> isEmpty then true
            else check tree.Shift tree.Root

    "The total number of items in all leaf nodes combined, plus the tail, should equal the vector length", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            Array.length sapling.Root + Array.length sapling.Tail = sapling.Length
        | :? RRBTree<'T> as tree ->
            itemCount<'T> tree.Root + Array.length tree.Tail = tree.Length

    "Unless the vector is empty, the tail should never be empty", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            (sapling.Length = 0) = (sapling.Tail.Length = 0)
        | :? RRBTree<'T> as tree ->
            (tree.Length = 0) = (tree.Tail.Length = 0)

    "The vector's length, shift, and tail offset must not be negative", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            sapling.Length >= 0 && sapling.Shift >= 0 && sapling.TailOffset >= 0
        | :? RRBTree<'T> as tree ->
            tree.Length >= 0 && tree.Shift >= 0 && tree.TailOffset >= 0

    "The number of items in each node should be <= the branching factor (Literals.blockSize)", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            if shift <= Literals.blockSizeShift then
                node.NodeSize <= Literals.blockSize &&
                node.Array.[0..node.NodeSize-1] |> Array.forall (fun n -> (n :?> 'T []).Length <= Literals.blockSize)
            else
                node.NodeSize <= Literals.blockSize &&
                node.Array.[0..node.NodeSize-1] |> Array.forall (fun n -> n :?> Node |> check (down shift))
        match vec with
        | :? RRBSapling<'T> as sapling ->
            sapling.Root.Length <= Literals.blockSize
        | :? RRBTree<'T> as tree ->
            check tree.Shift tree.Root

    "The number of items in the tail should be <= the branching factor (Literals.blockSize)", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            sapling.Tail.Length <= Literals.blockSize
        | :? RRBTree<'T> as tree ->
            tree.Tail.Length <= Literals.blockSize

    "The size table of any tree node should match the number of items its descendent leaves contain", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            match node with
            | :? RRBNode as n ->
                let expectedSizeTable =
                    if shift <= Literals.blockSizeShift then
                        node.Array.[0 .. node.NodeSize - 1] |> Array.map (fun n -> (n :?> 'T[]).Length) |> Array.scan (+) 0 |> Array.skip 1
                    else
                        node.Array.[0 .. node.NodeSize - 1] |> Array.map (fun n -> n :?> Node |> itemCount<'T>) |> Array.scan (+) 0 |> Array.skip 1
                n.SizeTable = expectedSizeTable
            | _ -> true
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Sapling roots are leaves, which don't have a size table
        | :? RRBTree<'T> as tree ->
            check tree.Shift tree.Root

    "The size table of any tree node should have as many entries as its # of direct child nodes", fun (vec : RRBVector<'T>) ->
        let rec check (node : Node) =
            match node with
            | :? RRBNode as n -> Array.length n.SizeTable = Array.length n.Array
            | _ -> true
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Sapling roots are leaves, which don't have a size table
        | :? RRBTree<'T> as tree ->
            check tree.Root

    "A full node should never contain less-than-full children except as its last child", fun (vec : RRBVector<'T>) ->
        let rec check shift seenFullParent isLastChild (node : Node) =
            if shift <= 0 then true else
            if shift <= Literals.blockSizeShift then
                match node with
                | :? RRBNode -> isLastChild || not seenFullParent // RRBNode twigs satisfy the property without need for further checking, since their children are leaves... as long as no parent was full, or as long as they were the last child
                | _ ->
                    if node.NodeSize <= 1 then true else
                    node.Array.[..node.NodeSize - 2] |> Array.forall (fun n ->
                        (n :?> 'T[]).Length = Literals.blockSize)
            else
                match node with
                | :? RRBNode ->
                    if shift <= Literals.blockSizeShift then
                        isLastChild || not seenFullParent // RRBNode twigs satisfy the property without need for further checking, since their children are leaves... as long as no parent was full, or as long as they were the last child
                    else
                        node.Array.[0 .. node.NodeSize - 1] |> Seq.indexed |> Seq.forall (fun (i,n) -> check (down shift) seenFullParent (i = node.NodeSize - 1) (n :?> Node))
                | _ ->
                    let fullCheck (n : Node) =
                        if shift <= Literals.blockSizeShift then
                            isFull n && n.NodeSize = Literals.blockSize
                        else
                            isFull n
                    if node.NodeSize = 0 then
                        true
                    elif node.NodeSize = 1 then
                        check (down shift) seenFullParent true (node.Array.[0] :?> Node)  // If a FullNode has just one element, it doesn't matter if it has an RRB child, but its children still need to be checked.
                    else
                        node.Array.[..node.NodeSize - 2] |> Array.forall (fun n ->
                            fullCheck (n :?> Node) && check (down shift) true true (n :?> Node)
                        ) && check (down shift) true true ((Array.last node.Array) :?> Node)
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            check tree.Shift false true tree.Root

    "The rightmost leaf node of the vector should always be full if its parent is full", fun (vec : RRBVector<'T>) ->
        // Except under certain conditions, that is... and I want to find out when this property turns out to be false
        // Right now, we say that this should be true as long as size >= 2*BS. But in fact, I think we need this to be
        // ALWAYS true as long as there's anything at all in the root, which means taking this into account in the slice algorithm.
        // Therefore, I need to tweak this property and then fix the cases where it's false.
        // NOTE: We still allow this to be false for trees of size BS or less, because those should be tail-only trees.
        match vec with
        | :? RRBSapling<'T> as sapling -> true  // Saplings are allowed to break the invariant
        | :? RRBTree<'T> as tree ->
            let rec check (node : Node) =
                if node |> isEmpty then
                    false
                elif node |> isTwig then
                    ((node.Array.[node.NodeSize - 1]) :?> 'T[]).Length = Literals.blockSize
                else
                    let arr = node.Array
                    node.NodeSize > 0 && node.Array.[node.NodeSize - 1] |> (fun n -> n :?> Node |> check)

            let twig = RRBHelpers.getRightmostTwig tree.Shift tree.Root
            match twig with
            | :? RRBNode -> true // No need for this invariant if last twig has size table
            | _ -> check tree.Root // TODO: Can rewrite this check to be simpler

    "The tail offset should equal (vector length - tail length) at all times, even when the vector is empty", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            sapling.Length - sapling.Tail.Length = sapling.TailOffset
        | :? RRBTree<'T> as tree ->
            tree.Length - tree.Tail.Length = tree.TailOffset

    "If the vector shift is > Literals.blockSizeShift, then the root node's length should be at least 2", fun (vec : RRBVector<'T>) -> // TODO: Move to "sometimesProperties" list
        // Except under certain conditions, that is... and I want to find out when this property turns out to be false
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            tree.Shift <= Literals.blockSizeShift || tree.Root.NodeSize >= 2

    "Leaf nodes' arrays should all contain items of type 'T, and all other nodes' arrays should contain nodes", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> true  // By the definition of saplings
        | :? RRBTree<'T> as tree ->
            let isNode (x:obj) = (x :? Node)
            let rec check level (node : Node) =
                if level <= Literals.blockSizeShift then
                    node.Array.[0 .. node.NodeSize - 1] |> Array.forall (fun (x : obj) -> x :? 'T [])
                else
                    node.Array.[0 .. node.NodeSize - 1] |> Array.forall isNode &&
                    node.Array.[0 .. node.NodeSize - 1] |> Array.forall (fun n -> n :?> Node |> check (level - Literals.blockSizeShift))
            check tree.Shift tree.Root  // No need to check the tail since it's defined as 'T []

    "Internal nodes that are at shift > Literals.blockSizeShift should never be twigs", fun (vec : RRBVector<'T>) ->
        let rec check shift node =
            if shift <= Literals.blockSizeShift then true else
            node |> isNotTwig && node.Array.[0 .. node.NodeSize - 1] |> Array.forall (fun child -> child :? Node && child :?> Node |> check (shift - Literals.blockSizeShift))
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            if tree.Root |> isEmpty then true else check tree.Shift tree.Root

    "The root node should be empty if (and only if) the tail offset is 0 (i.e., it's a tail-only node)", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            (sapling.Root.Length = 0) = (sapling.TailOffset = 0)
        | :? RRBTree<'T> as tree ->
            (tree.Root.NodeSize = 0) = (tree.TailOffset = 0)

    "If vector height > 0, the root node should not be empty", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            tree.Shift > 0 && tree.Root |> (not << isEmpty)

    "Iff vector height = 0, the tree should be a sapling", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> sapling.Shift = 0
        | :? RRBTree<'T> as tree -> tree.Shift > 0

    "If vector height = 0, the tree should have no more than blockSize * 2 total items in it", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            sapling.Length <= Literals.blockSize * 2
        | :? RRBTree<'T> as tree -> true // Not applicable to taller trees

    "If vector height > 0, the tree should not contain any empty nodes", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            let rec check shift node =
                if shift <= Literals.blockSizeShift then node |> (not << isEmpty)
                else node |> (not << isEmpty) && node.Array.[0 .. node.NodeSize - 1] |> Array.forall (fun n -> n :?> Node |> check (down shift))
            check tree.Shift tree.Root

    // TODO: Think about disabling this property, because in some rare circumstances, it is possible for this to happen.
    // For example, if we remove from the head of [M M] T1 twice, then inserting into the second leaf once, we'd get a [M] TM vector (height non-0).
    // But that's really not much of a concern, since a concatenation will deal with it.
    "If vector height > 0, the root node should contain more than one node", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> true
        | :? RRBTree<'T> as tree -> tree.Root.NodeSize > 1

    "If vector length <= (Literals.blockSize * 2), the root node should contain more than one node", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> sapling.Length <= Literals.blockSize * 2
        | :? RRBTree<'T> as tree -> tree.Length <= Literals.blockSize * 2 || tree.Root.NodeSize > 1

    "No RRBNode should contain a \"full\" size table. If the size table was full, it should have been turned into a FullNode.", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            if shift <= 0 then true else // No RRBNodes at leaf level
            match node with
            | :? RRBNode as n ->
                if shift <= Literals.blockSizeShift then
                    not (RRBMath.isSizeTableFullAtShift shift n.SizeTable)  // No need to descend further as there are no RRBNodes at the leaf level
                else
                    not (RRBMath.isSizeTableFullAtShift shift n.SizeTable) && n.Array |> Array.forall (fun n -> n :?> Node |> check (down shift))
            | _ ->
                if shift <= Literals.blockSizeShift then
                    true  // No need to descend further as there are no RRBNodes at the leaf level
                else
                    node.Array.[0 .. node.NodeSize - 1] |> Array.forall (fun n -> n :?> Node |> check (down shift))
        match vec with
        | :? RRBSapling<'T> as sapling -> true // Not applicable to saplings
        | :? RRBTree<'T> as tree ->
            check tree.Shift tree.Root

    "If the root node is empty, the shift is 0", fun (vec : RRBVector<'T>) ->
        // Note that the converse is *NOT* true: if shift is 0, it could be a root+tail vector
        match vec with
        | :? RRBSapling<'T> as sapling -> true
        | :? RRBTree<'T> as tree -> not (tree.Root |> isEmpty)

    "The shift of a vector should always be a multiple of Literals.blockSizeShift", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling ->
            sapling.Shift % Literals.blockSizeShift = 0
        | :? RRBTree<'T> as tree ->
            tree.Shift % Literals.blockSizeShift = 0

    "The shift of a vector should always be the height from the root to the leaves, multiplied by Literals.blockSizeShift", fun (vec : RRBVector<'T>) ->
        match vec with
        | :? RRBSapling<'T> as sapling -> sapling.Shift = 0
        | :? RRBTree<'T> as tree ->
            let rec height acc (node : Node) =
                if isEmpty node || isTwig node then acc else node |> RRBHelpers.getChildNode 0 |> height (acc+1)
            if tree.Root |> isEmpty then
                true // We test this condition in the "If the root node is empty ..." check above, not here.
            else // Non-empty root, so we can meaningfully check the height of the tree
                (height 1 tree.Root) * Literals.blockSizeShift = tree.Shift
]

// Other properties to add at some point:
// The number of extra steps we have to take in any node shall be no more than "e", where e is usually 2.
//   (First, have to figure out how to test that.)
//   (Also, this is a property that will sometimes fail, e.g. after certain splits, and that's fine.)
// "If-then" properties, that are kind of like mini-unit tests:
//   If a vector has a full tail, pushing one item into it will produce a vector with a tail of length 1.
//   If a vector has a tail of length 1, popping one item from it will produce a vector with a tail of length Literals.blockSize.
// Both of those will be tested only if the condition is true.
// TODO: Might want more conditional properties as well.

type PropResult = string list

let checkProperty name pred vec =
    try
        if pred vec then [] else [name]
    with
    | :? System.InvalidCastException ->
        ["Invalid cast while checking " + name]
    | :? System.IndexOutOfRangeException ->
        ["Index out of range while checking " + name]

let combine r1 r2 = (r1 @ r2)

open RRBVecGen

let getPropertyResults vec =
    properties |> List.map (fun (name,pred) -> checkProperty name pred vec) |> List.fold combine []

let checkProperties vec label =
    let result = getPropertyResults vec
    match result with
    | [] -> ()
    | errors -> Expecto.Tests.failtestf "%s %A\nRepr: %s\nfailed the following RRBVector invariants:\n%A" label vec (vecToTreeReprStr vec) errors

let checkPropertiesSimple vec = checkProperties vec "Vector"
