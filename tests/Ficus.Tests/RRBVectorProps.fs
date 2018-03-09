module ExpectoTemplate.RRBVectorProps

open System
open Ficus
open Ficus.RRBVector
open FsCheck

module Literals = Ficus.Literals

let isEmpty (node : Node) = node.Array.Length <= 0
// Note: do NOT call isNode or isLeaf on empty nodes!
let isNotLeaf (node : Node) = node.Array.[0] :? Node
let isLeaf (node : Node) = not (isNotLeaf node)
let isRRB  (node : Node) = node :? RRBNode
let isFull (node : Node) = not (isRRB node)
let items (node : Node) = if isLeaf node then node.Array else [||]
let children (node : Node) = if isNotLeaf node then node.Array else [||]

let rec itemCount (node : Node) =
    if isEmpty node then 0
    elif node |> isLeaf then node.Array.Length
    else node.Array |> Array.sumBy (fun n -> n :?> Node |> itemCount)

let inline down shift = shift - Literals.blockSizeShift

// Properties start here

// Note that when I check the shift, I use <= 0, because in unit testing, we really might run into
// some deformed trees where the shift isn't a proper multiple of Literals.blockSizeShift. If we
// run into one of those, I don't want to loop forever because we skipped from 1 to -1 and never hit 0.

let properties = [
    "All leaves should be at the same height", fun (vec : RRBVector<'T>) ->
        let rec check shift node =
            if shift <= 0 then
                node |> isEmpty || node |> isLeaf
            elif shift <= Literals.blockSizeShift then
                node.Array |> Array.forall (fun n -> not (isEmpty (n :?> Node)) && isLeaf (n :?> Node))
            else
                node.Array |> Array.forall (fun n -> isNotLeaf (n :?> Node) && check (down shift) (n :?> Node))
        if vec.Root |> isEmpty then true
        else check vec.Shift vec.Root

    "The total number of items in all leaf nodes combined, plus the tail, should equal the vector length", fun (vec : RRBVector<'T>) ->
        itemCount vec.Root + Array.length vec.Tail = vec.Length

    "Unless the vector is empty, the tail should never be empty", fun (vec : RRBVector<'T>) ->
        (vec.Length = 0) = (vec.Tail.Length = 0)

    "The vector's length, shift, and tail offset must not be negative", fun (vec : RRBVector<'T>) ->
        vec.Length >= 0 && vec.Shift >= 0 && vec.TailOffset >= 0

    "The number of items in each node should be <= the branching factor (Literals.blockSize)", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            if shift <= 0 then
                node.Array.Length <= Literals.blockSize
            else
                node.Array.Length <= Literals.blockSize &&
                node.Array |> Array.forall (fun n -> n :?> Node |> check (down shift))
        check vec.Shift vec.Root

    "The number of items in the tail should be <= the branching factor (Literals.blockSize)", fun (vec : RRBVector<'T>) ->
        vec.Tail.Length <= Literals.blockSize

    "The size table of any tree node should match the number of items its descendent leaves contain", fun (vec : RRBVector<'T>) ->
        let rec check (node : Node) =
            match node with
            | :? RRBNode as n ->
                let expectedSizeTable = node.Array |> Array.map (fun n -> n :?> Node |> itemCount) |> Array.scan (+) 0 |> Array.skip 1
                n.SizeTable = expectedSizeTable
            | _ -> true
        check vec.Root

    "The size table of any tree node should have as many entries as its # of direct child nodes", fun (vec : RRBVector<'T>) ->
        let rec check (node : Node) =
            match node with
            | :? RRBNode as n -> Array.length n.SizeTable = Array.length n.Array
            | _ -> true
        check vec.Root

    "A full node should never contain less-than-full children except as its last child", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            if shift <= 0 then true else
            match node with
            | :? RRBNode ->
                node.Array |> Array.forall (fun n -> check (down shift) (n :?> Node))
            | _ ->
                let fullCheck (n : Node) =
                    if shift <= Literals.blockSize then
                        isFull n && n.Array.Length = Literals.blockSize
                    else
                        isFull n
                if node.Array.Length <= 1 then
                    check (down shift) (node.Array.[0] :?> Node)
                else
                    node.Array.[..node.Array.Length - 2] |> Array.forall (fun n ->
                        fullCheck (n :?> Node) && check (down shift) (n :?> Node)
                    ) && check (down shift) ((Array.last node.Array) :?> Node)
        check vec.Shift vec.Root

    "The rightmost leaf node of the vector should always be full if its parent is full", fun (vec : RRBVector<'T>) ->
        // Except under certain conditions, that is... and I want to find out when this property turns out to be false
        // Right now, we say that this should be true as long as size >= 2*BS. But in fact, I think we need this to be
        // ALWAYS true as long as there's anything at all in the root, which means taking this into account in the slice algorithm.
        // Therefore, I need to tweak this property and then fix the cases where it's false.
        // NOTE: We still allow this to be false for trees of size BS or less, because those should be tail-only trees.
        if vec.Length <= Literals.blockSize || vec.Shift <= 0 then true else  // We don't care about leaf- or tail-only trees
        let rec check (node : Node) =
            if node |> isEmpty then
                false
            elif node |> isLeaf then
                node.Array |> Array.length = Literals.blockSize
            else
                let arr = node.Array
                Array.length arr > 0 && arr |> Array.last |> (fun n -> n :?> Node |> check)

        let twig = RRBHelpers.getRightmostTwig vec.Shift vec.Root
        match twig with
        | :? RRBNode -> true // No need for this invariant if last twig has size table
        | _ -> check vec.Root // TODO: Can rewrite this check to be simpler

    "The tail offset should equal (vector length - tail length) at all times, even when the vector is empty", fun (vec : RRBVector<'T>) ->
        vec.Length - vec.Tail.Length = vec.TailOffset

    "If the vector shift is > Literals.blockSizeShift, then the root node's length should be at least 2", fun (vec : RRBVector<'T>) -> // TODO: Move to "sometimesProperties" list
        // Except under certain conditions, that is... and I want to find out when this property turns out to be false
        vec.Shift <= Literals.blockSizeShift || vec.Root.Array |> Array.length >= 2

    "Leaf nodes' arrays should all contain items of type 'T, and all other nodes' arrays should contain nodes", fun (vec : RRBVector<'T>) ->
        let isOfTypeT x = (isNull x || box x :? 'T)   // For reference types where nulls are allowed, (null :? 'T) is always false
        let isNode (x:obj) = (x :? Node)
        let rec check level (node : Node) =
            if level > 0 then
                node.Array |> Array.forall isNode &&
                node.Array |> Array.forall (fun n -> n :?> Node |> check (level - 1))
            else
                node.Array |> Array.forall isOfTypeT
        check (vec.Shift / Literals.blockSizeShift) vec.Root && Array.forall isOfTypeT vec.Tail

    "Internal nodes should all contain items of type Node or RRBNode", fun (vec : RRBVector<'T>) ->
        // This is now a duplicate of the "All leaves should be at the same height" property
        let rec check shift (node : Node) =
            if shift <= 0 then true else  // This property doesn't check shift 0
            node |> isNotLeaf && node.Array |> Array.forall (fun child -> child :?> Node |> check (shift - Literals.blockSizeShift))
        if vec.Root |> isEmpty then true else check vec.Shift vec.Root

    "Internal nodes that are at shift > Literals.blockSizeShift should never contain LeafNodes", fun (vec : RRBVector<'T>) ->
        let rec check shift node =
            if shift <= Literals.blockSizeShift then true else
            node |> isNotLeaf && node.Array |> Array.forall (fun child -> child :? Node && child :?> Node |> check (shift - Literals.blockSizeShift))
        if vec.Root |> isEmpty then true else check vec.Shift vec.Root

    "The root node should be empty if (and only if) the tail offset is 0 (i.e., it's a tail-only node)", fun (vec : RRBVector<'T>) ->
        (vec.Root.Array.Length = 0) = (vec.TailOffset = 0)

    "If vector height > 0, the root node should not be empty", fun (vec : RRBVector<'T>) ->
        if vec.Shift > 0 then vec.Root |> (not << isEmpty) else true

    "If vector height = 0, the root node should be a leaf node", fun (vec : RRBVector<'T>) ->
        if vec.Shift = 0 then
            vec.Root |> isEmpty || vec.Root.Array |> Array.forall (fun item -> not (item :? Node) && (isNull item || item :? 'T))
        else
            true // Irrelevant if vec.Shift > 0

    "If vector height = 0, the tree should have no more than blockSize * 2 total items in it", fun (vec : RRBVector<'T>) ->
        vec.Shift > 0 || vec.Length <= Literals.blockSize * 2

    "If vector height > 0, the tree should not contain any empty nodes", fun (vec : RRBVector<'T>) ->
        let rec check shift node =
            if shift <= 0 then node |> (not << isEmpty)
            else node |> (not << isEmpty) && node.Array |> Array.forall (fun n -> n :?> Node |> check (down shift))
        vec.Shift <= 0 || check vec.Shift vec.Root

    // TODO: Think about disabling this property, because in some rare circumstances, it is possible for this to happen.
    // For example, if we remove from the head of [M M] T1 twice, then inserting into the second leaf once, we'd get a [M] TM vector (height non-0).
    // But that's really not much of a concern, since a concatenation will deal with it.
    "If vector height > 0, the root node should contain more than one node", fun (vec : RRBVector<'T>) ->
        vec.Shift <= 0 || vec.Root.Array.Length > 1

    "If vector length > (Literals.blockSize * 2), the root node should contain more than one node", fun (vec : RRBVector<'T>) ->
        vec.Length <= Literals.blockSize * 2 || vec.Root.Array.Length > 1

    "No RRBNode should contain a \"full\" size table. If the size table was full, it should have been turned into a FullNode.", fun (vec : RRBVector<'T>) ->
        let rec check shift (node : Node) =
            if shift <= 0 then true else // No RRBNodes at leaf level
            match node with
            | :? RRBNode as n -> not (RRBHelpers.isSizeTableFullAtShift shift n.SizeTable) && n.Array |> Array.forall (fun n -> n :?> Node |> check (down shift))
            | _ -> node.Array |> Array.forall (fun n -> n :?> Node |> check (down shift))
        check vec.Shift vec.Root

    "If the root node is empty, the shift is 0", fun (vec : RRBVector<'T>) ->
        if vec.Root |> isEmpty then vec.Shift = 0 else true  // Note that the converse is *NOT* true: if shift is 0, it could be a root+tail vector

    "The shift of a vector should always be a multiple of Literals.blockSizeShift", fun (vec : RRBVector<'T>) ->
        vec.Shift % Literals.blockSizeShift = 0

    "The shift of a vector should always be the height from the root to the leaves, multiplied by Literals.blockSizeShift", fun (vec : RRBVector<'T>) ->
        let rec height acc (node : Node) =
            if isEmpty node || isLeaf node then acc else node |> RRBHelpers.getChildNode 0 |> height (acc+1)
        if vec.Root |> isEmpty then
            true // We test this condition in the "If the root node is empty ..." check above, not here.
        else // Non-empty root, so we can meaningfully check the height of the tree
            (height 0 vec.Root) * Literals.blockSizeShift = vec.Shift
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

let checkProperties vec label =
    let result = properties |> List.map (fun (name,pred) -> checkProperty name pred vec) |> List.fold combine []
    match result with
    | [] -> ()
    | errors -> Expecto.Tests.failtestf "%s %A\nRepr: %s\nfailed the following RRBVector invariants:\n%A" label vec (vecToTreeReprStr vec) errors

let checkPropertiesSimple vec = checkProperties vec "Vector"
