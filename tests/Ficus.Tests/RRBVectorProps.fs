module ExpectoTemplate.RRBVectorProps

open System
open Ficus
open Ficus.RRBVectorBetterNodes
open Ficus.RRBVector
open Ficus.RRBVectorBetterNodes.RRBMath
open FsCheck

module Literals = Ficus.Literals

let children (node : RRBNode<'T>) = (node :?> RRBFullNode<'T>).Children |> Seq.truncate node.NodeSize

let isEmpty (node : RRBNode<'T>) = node.NodeSize <= 0
// Note: do NOT call isNotTwig or isTwig on empty nodes!
let isLeaf (node : RRBNode<'T>) = node :? RRBLeafNode<'T>
let isNode (node : RRBNode<'T>) = not (isLeaf node)
let isRelaxed (node : RRBNode<'T>) = node :? RRBRelaxedNode<'T>
let isFull (node : RRBNode<'T>) = isNode node && not (isRelaxed node)
let isTwig (node : RRBNode<'T>) = isNode node && children node |> Seq.forall isLeaf
let isNotTwig (node : RRBNode<'T>) = not (isTwig node)

let rec itemCount shift (node : RRBNode<'T>) =
    if shift <= 0 then node.NodeSize
    else children node |> Seq.sumBy (itemCount (down shift))

let rec height (node : RRBNode<'T>) =
    if isLeaf node then 0
    elif isEmpty node then 1  // Empty roots are considered at height 1 now
    else 1 + height (node :?> RRBFullNode<'T>).FirstChild

type Fullness = CompletelyFull | FullEnough | NotFull   // Used in node properties

// Properties start here

// Note that when I check the shift, I use <= 0, because in unit testing, we really might run into
// some deformed trees where the shift isn't a proper multiple of Literals.blockSizeShift. If we
// run into one of those, I don't want to loop forever because we skipped from 1 to -1 and never hit 0.

let nodeProperties = [
    "All twigs should be at height 1", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= Literals.blockSizeShift then
                not (isEmpty node) && isTwig node
            else
                not (isEmpty node) && isNotTwig node && children node |> Seq.forall (fun n -> check (down shift) (n :?> RRBFullNode<'T>))
        if root |> isEmpty then true
        else check shift root

    "All leaves should be at height 0", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift node =
            if isLeaf node then shift = 0 else not (isEmpty node) && children node |> Seq.forall (check (down shift))
        check shift root

    "The number of items in each node and leaf should be <= the branching factor (Literals.blockSize)", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then
                node.NodeSize <= Literals.blockSize
            else
                node.NodeSize <= Literals.blockSize &&
                children node |> Seq.forall (check (down shift))
        check shift root

    "The tree size of any node should match the total number of items its descendent leaves contain", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= Literals.blockSize
            then node |> itemCount shift = node.TreeSize shift
            else node |> itemCount shift = node.TreeSize shift && children node |> Seq.forall (check (down shift))
        check shift root

    "The tree size of any leaf should equal its node size", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0
            then node.NodeSize = node.TreeSize shift
            else children node |> Seq.forall (check (down shift))
        check shift root

    "The size table of any tree node should match the cumulative tree sizes of its children", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBFullNode<'T>) =
            let sizeTable = if isRelaxed node then (node :?> RRBRelaxedNode<'T>).SizeTable else node.BuildSizeTable shift node.NodeSize (node.NodeSize - 1)
                            |> Array.truncate node.NodeSize
            let expectedSizeTable = children node |> Seq.map (fun child -> child.TreeSize (down shift)) |> Seq.scan (+) 0 |> Seq.skip 1 |> Array.ofSeq
            sizeTable = expectedSizeTable
        check shift (root :?> RRBFullNode<'T>)

    "The size table of any tree node should have as many entries as its # of direct child nodes", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check (node : RRBFullNode<'T>) =
            if isRelaxed node
            then Array.length (node :?> RRBRelaxedNode<'T>).SizeTable = Array.length node.Children
            else true
        check (root :?> RRBFullNode<'T>)

    "A full node should never contain less-than-full children except as its last child", fun (shift : int) (root : RRBNode<'T>) ->
        // Rules:
        // We distinguish "completely full" and "full enough" from "not full".
        // A "completely full" node has NO leaf descendants that are not blockSize long, so its tree size is (1 <<< shift).
        // A "full enough" node has all but its last child be completely full, and its last child can be anything
        // A full node at height 1, which is NOT the last child of its parent, should contain nothing but full leaves exc

        // If we wrote from the ground up, then we'd keep track of a status called "completely full" and/or "full enough" (and a third, "not full")
        // A leaf would be either completely full or not full. A node would be completely full if all its children were completely full. If all its
        // children but the last were completely full but its last child was full enough or not full, then that node would be full enough. Otherwise,
        // the node would be not full. Yes, that includes if any child besides the last child is "full enough": that would grant a status of "not full"
        // to the parent.
        let rec fullness shift (node : RRBNode<'T>) =
            if shift <= 0 then
                if node.NodeSize = Literals.blockSize then (CompletelyFull, true) else (NotFull, true)
            else
                let c = children node |> Seq.map (fullness (down shift)) |> List.ofSeq
                let allButLast = c |> Seq.take (node.NodeSize - 1)
                let last = c |> Seq.skip (node.NodeSize - 1) |> Seq.head
                let fullnessResult =
                    if allButLast |> Seq.forall (fst >> (=) CompletelyFull) then
                        if (fst last) = CompletelyFull then CompletelyFull else FullEnough
                    else
                        NotFull
                let isValid =
                    (c |> Seq.forall snd) &&
                    if isFull node then fullnessResult <> NotFull
                    else true
                (fullnessResult, isValid)
        fullness shift root |> snd

    "All nodes at shift 0 should be leaves", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift node =
            if shift <= 0 then node |> isLeaf else children node |> Seq.forall (check (down shift))
        check shift root

    "Internal nodes that are at shift > 0 should never be leaves", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift node =
            if shift > 0 then node |> isNode && children node |> Seq.forall (check (down shift)) else true
        (* if root |> isEmpty then true else *)
        check shift root

    "A tree should not contain any empty nodes", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift node =
            node |> (not << isEmpty) &&
            if shift <= 0 then true else children node |> Seq.forall (check (down shift))
        check shift root

    "No RRBRelaxedNode should contain a \"full\" size table. If the size table was full, it should have been turned into an RRBFullNode.", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then true else
            let nodeValid = if isRelaxed node then not (isSizeTableFullAtShift shift (node :?> RRBRelaxedNode<'T>).SizeTable node.NodeSize) else true
            nodeValid && children node |> Seq.forall (check (down shift))
        check shift root

    "The shift of a vector should always be a multiple of Literals.blockSizeShift", fun (shift : int) (root : RRBNode<'T>) ->
        shift % Literals.blockSizeShift = 0

    "The shift of a vector should always be the height from the root to the leaves, multiplied by Literals.blockSizeShift", fun (shift : int) (root : RRBNode<'T>) ->
        let rec height acc (node : RRBNode<'T>) =
            if isLeaf node then acc else (node :?> RRBFullNode<'T>).FirstChild |> height (acc+1)
        (height 0 root) * Literals.blockSizeShift = shift

    "ExpandedNodes (and ExpandedRRBNodes) should not appear in a tree whose root is not an expanded node variant", fun (shift : int) (root : RRBNode<'T>) ->
        let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then true
            elif isExpanded node then false
            else children node |> Seq.forall (check (down shift))
        if isExpanded root then true else check shift root

    "If a tree's root is an expanded Node variant, its right spine should contain expanded nodes but nothing else should", fun (shift : int) (root : RRBNode<'T>) ->
        let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
        let rec check shift isLast (node : RRBNode<'T>) =
            if shift <= 0 then true
            elif isExpanded node && not isLast then false
            elif isLast && not (isExpanded node) then false
            else
                let checkResultForAllButLast = children node |> Seq.take (node.NodeSize - 1) |> Seq.forall (check (down shift) false)
                let checkResultForLastChild = children node |> Seq.skip (node.NodeSize - 1) |> Seq.head |> check (down shift) isLast
                checkResultForAllButLast && checkResultForLastChild
        if isExpanded root then check shift true root else true

    "ExpandedNodes (and ExpandedRRBNodes) should have exactly as much data in their children & size tables as their node size indicates", fun (shift : int) (root : RRBNode<'T>) ->
        let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then true else
            if isExpanded node then
                let sizeTableClean =
                    if isRelaxed node then
                        (node :?> RRBExpandedRelaxedNode<'T>).SizeTable |> Seq.skip node.NodeSize |> Seq.forall ((=) 0) &&
                        (node :?> RRBExpandedRelaxedNode<'T>).SizeTable |> Seq.take node.NodeSize |> Seq.forall ((<>) 0)
                    else true
                let childrenArrayClean =
                    if isRelaxed node then
                        (node :?> RRBExpandedRelaxedNode<'T>).Children |> Seq.skip node.NodeSize |> Seq.forall isNull &&
                        (node :?> RRBExpandedRelaxedNode<'T>).Children |> Seq.take node.NodeSize |> Seq.forall (not << isNull)
                    else
                        (node :?> RRBExpandedFullNode<'T>).Children |> Seq.skip node.NodeSize |> Seq.forall isNull &&
                        (node :?> RRBExpandedFullNode<'T>).Children |> Seq.take node.NodeSize |> Seq.forall (not << isNull)
                sizeTableClean && childrenArrayClean && children node |> Seq.forall (check (down shift))
            else
                children node |> Seq.forall (check (down shift))
        check shift root
]


let vectorPropertiesPersistent = [
    "The total number of items in all leaf nodes combined, plus the tail, should equal the vector length", fun (vec : RRBPersistentVector<'T>) ->
        let rootSize = itemCount vec.Shift vec.Root
        let tailLen = vec.Tail.Length
        rootSize + tailLen = vec.Length

    "Unless the vector is empty, the tail should never be empty", fun (vec : RRBPersistentVector<'T>) ->
        let tailLen = vec.Tail.Length
        if vec.Length > 0 then tailLen > 0 else tailLen = 0

    "The vector's length, shift, and tail offset must not be negative", fun (vec : RRBPersistentVector<'T>) ->
        vec.Length >= 0 && vec.Shift >= 0 && vec.TailOffset >= 0

    "The number of items in the tail should be <= the branching factor (Literals.blockSize)", fun (vec : RRBPersistentVector<'T>) ->
        let tailLen = vec.Tail.Length
        tailLen <= Literals.blockSize

    // Skipping for now
    // "The rightmost leaf node of the vector should always be full if its parent is full", fun (vec : RRBPersistentVector<'T>) -> // A true vector property because only by calling vector methods can it be achieved
    //     if vec.TailOffset = 0 then true else
    //     let rec getRightTwig shift (node : RRBFullNode<'T>) =
    //         if shift <= Literals.blockSizeShift then node
    //         else (node.LastChild :?> RRBFullNode<'T>) |> getRightTwig (RRBMath.down shift)
    //     let twig = getRightTwig vec.Shift (vec.Root :?> RRBFullNode<'T>)
    //     if isFull twig then twig.LastChild.NodeSize = Literals.blockSize else true

    "If the vector shift is > Literals.blockSizeShift, then the root node's length should be at least 2", fun (vec : RRBPersistentVector<'T>) -> // A true vector property because only by calling vector methods can it be achieved
        if vec.Shift > Literals.blockSizeShift then vec.Root.NodeSize >= 2 else true

    "The tail offset should equal (vector length - tail length) at all times, even when the vector is empty", fun (vec : RRBPersistentVector<'T>) ->
        let tailLen = vec.Tail.Length
        vec.TailOffset = vec.Length - tailLen

    "The tail offset should equal (root.TreeSize this.Shift) at all times, unless the tail offset is 0", fun (vec : RRBPersistentVector<'T>) ->
        if vec.TailOffset = 0 then true else vec.TailOffset = vec.Root.TreeSize vec.Shift

    "The root node should be empty if (and only if) the tail offset is 0 (i.e., it's a tail-only node)", fun (vec : RRBPersistentVector<'T>) ->
        (vec.TailOffset = 0) = (vec.Root.NodeSize = 0)

    "If tail offset = 0, the vector length should be blockSize or less", fun (vec : RRBPersistentVector<'T>) ->
        if vec.TailOffset > 0 then true else vec.Length <= Literals.blockSize

    "The shift of a vector should always be a multiple of Literals.blockSizeShift", fun (vec : RRBPersistentVector<'T>) ->
        vec.Shift % Literals.blockSizeShift = 0

    "The shift of a vector should always be the height from the root to the leaves, multiplied by Literals.blockSizeShift", fun (vec : RRBPersistentVector<'T>) ->
        vec.Shift = (height vec.Root) * Literals.blockSizeShift

    "If a tree is Persistent, its root should NOT be an Expanded node variant", fun (vec : RRBPersistentVector<'T>) ->
        let isExpanded (node : RRBNode<'T>) = (node :? RRBExpandedFullNode<'T>) || (node :? RRBExpandedRelaxedNode<'T>)
        not (isExpanded vec.Root)

    // The check for the tree's spine, etc., is already handled in the node properties
]

let vectorPropertiesTransient = [
    "The total number of items in all leaf nodes combined, plus the tail, should equal the vector length", fun (vec : RRBTransientVector<'T>) ->
        let rootSize = itemCount vec.Shift vec.Root
        let tailLen = vec.Length - vec.TailOffset
        rootSize + tailLen = vec.Length

    "Transients' tails should be full at all times", fun (vec : RRBTransientVector<'T>) ->
        vec.Tail.Length = Literals.blockSize

    "Transients' tails should have exactly (vec.Length - vec.TailOffset) real items, and the rest should equal Unchecked.defaultof<'T>", fun (vec : RRBTransientVector<'T>) ->
        let tailLen = vec.Length - vec.TailOffset
        let mutable result = true
        for i = tailLen to Literals.blockSize - 1 do
            result <- result && (vec.Tail.[i] = Unchecked.defaultof<'T>)
        result

    "Unless the vector is empty, the tail should never be empty", fun (vec : RRBTransientVector<'T>) ->
        let tailLen = vec.Length - vec.TailOffset
        if vec.Length > 0 then tailLen > 0 else tailLen = 0

    "The vector's length, shift, and tail offset must not be negative", fun (vec : RRBTransientVector<'T>) ->
        vec.Length >= 0 && vec.Shift >= 0 && vec.TailOffset >= 0

    "The number of items in the tail should be <= the branching factor (Literals.blockSize)", fun (vec : RRBTransientVector<'T>) ->
        let tailLen = vec.Length - vec.TailOffset
        tailLen <= Literals.blockSize

    "The rightmost leaf node of the vector should always be full if its parent is full", fun (vec : RRBTransientVector<'T>) -> // A true vector property because only by calling vector methods can it be achieved
        if vec.TailOffset = 0 then true else
        let rec getRightTwig shift (node : RRBFullNode<'T>) =
            if shift <= Literals.blockSizeShift then node
            else (node.LastChild :?> RRBFullNode<'T>) |> getRightTwig (RRBMath.down shift)
        let twig = getRightTwig vec.Shift (vec.Root :?> RRBFullNode<'T>)
        if isFull twig then twig.LastChild.NodeSize = Literals.blockSize else true

    "If the vector shift is > Literals.blockSizeShift, then the root node's length should be at least 2", fun (vec : RRBTransientVector<'T>) -> // A true vector property because only by calling vector methods can it be achieved
        if vec.Shift > Literals.blockSizeShift then vec.Root.NodeSize >= 2 else true

    "The tail offset should equal (vector length - tail length) at all times, even when the vector is empty", fun (vec : RRBTransientVector<'T>) ->
        let tailLen = vec.Length - vec.TailOffset
        vec.TailOffset = vec.Length - tailLen

    "The tail offset should equal (root.TreeSize this.Shift) at all times, unless the tail offset is 0", fun (vec : RRBTransientVector<'T>) ->
        if vec.TailOffset = 0 then true else vec.TailOffset = vec.Root.TreeSize vec.Shift

    "The root node should be empty if (and only if) the tail offset is 0 (i.e., it's a tail-only node)", fun (vec : RRBTransientVector<'T>) ->
        (vec.TailOffset = 0) = (vec.Root.NodeSize = 0)

    "If tail offset = 0, the vector length should be blockSize or less", fun (vec : RRBTransientVector<'T>) ->
        if vec.TailOffset > 0 then true else vec.Length <= Literals.blockSize

    "The shift of a vector should always be a multiple of Literals.blockSizeShift", fun (vec : RRBTransientVector<'T>) ->
        vec.Shift % Literals.blockSizeShift = 0

    "The shift of a vector should always be the height from the root to the leaves, multiplied by Literals.blockSizeShift", fun (vec : RRBTransientVector<'T>) ->
        vec.Shift = (height vec.Root) * Literals.blockSizeShift

    "If a tree is Transient, its root should be an Expanded node variant", fun (vec : RRBTransientVector<'T>) ->
        let isExpanded (node : RRBNode<'T>) = (node :? RRBExpandedFullNode<'T>) || (node :? RRBExpandedRelaxedNode<'T>)
        isExpanded vec.Root

    // The check for the tree's spine, etc., is already handled in the node properties
]



type PropResult = string list

let checkNodeProperty name pred shift root =
    try
        // logger.debug(eventX "Checking property {name} on root {root}" >> setField "name" name >> setField "root" (sprintf "%A" root))
        if pred shift root then [] else [name]
    with
    | :? System.InvalidCastException ->
        ["Invalid cast while checking " + name]
    | :? System.IndexOutOfRangeException ->
        ["Index out of range while checking " + name]

let checkVectorProperty name pred vec =
    try
        // logger.debug(eventX "Checking property {name} on vector {vec}" >> setField "name" name >> setField "vec" (sprintf "%A" vec))
        if pred vec then [] else [name]
    with
    | :? System.InvalidCastException ->
        ["Invalid cast while checking " + name]
    | :? System.IndexOutOfRangeException ->
        ["Index out of range while checking " + name]

// TODO: Can probably use List.collect instead of List.fold combine
let combine r1 r2 = (r1 @ r2)

let getNodePropertyResults shift root =
    nodeProperties |> List.map (fun (name,pred) -> checkNodeProperty name pred shift root) |> List.fold combine []

let checkNodeProperties shift root label =
    let result = getNodePropertyResults shift root
    match result with
    | [] -> ()
    | errors -> Expecto.Tests.failtestf "%s with shift=%d and root=%A\nfailed the following RRBVector invariants:\n%A" label shift root errors

let checkNodePropertiesSimple shift root = checkNodeProperties shift root "Node"

let getVecPropertyResults props vec =
    props |> List.map (fun (name,pred) -> checkVectorProperty name pred vec) |> List.fold combine []

let getVecAndNodePropertyResults props shift (root : RRBNode<'T>) vec =
    let vecProps = getVecPropertyResults props vec
    let nodeProps = if root.NodeSize > 0 then getNodePropertyResults shift root else []
    vecProps @ nodeProps

let getAllPropertyResults (vec : RRBVector<'T>) =
    match vec with
    | :? RRBPersistentVector<'T> as v -> getVecAndNodePropertyResults vectorPropertiesPersistent v.Shift v.Root v
    | :? RRBTransientVector<'T>  as v -> getVecAndNodePropertyResults vectorPropertiesTransient v.Shift v.Root v
    | _ -> failwith "Unknown vector type"

let checkAllProperties (vec : RRBVector<'T>) label =
    let result = getAllPropertyResults vec
    match result with
    | [] -> ()
    | errors -> Expecto.Tests.failtestf "%s %A\nfailed the following RRBVector invariants:\n%A" label vec errors

// Convenience function for tests written against the previous API
let checkProperties vec label = checkAllProperties vec label
let checkPropertiesSimple vec = checkAllProperties vec "Vector"

// Other properties to add at some point:
// The number of extra steps we have to take in any node shall be no more than "e", where e is usually 2.
//   (First, have to figure out how to test that.)
//   (Also, this is a property that will sometimes fail, e.g. after certain splits, and that's fine.)
// "If-then" properties, that are kind of like mini-unit tests:
//   If a vector has a full tail, pushing one item into it will produce a vector with a tail of length 1.
//   If a vector has a tail of length 1, popping one item from it will produce a vector with a tail of length Literals.blockSize.
// Both of those will be tested only if the condition is true.
// TODO: Might want more conditional properties as well.
