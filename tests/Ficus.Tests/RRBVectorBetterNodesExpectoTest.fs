module ExpectoTemplate.RRBVectorBetterNodesExpectoTest

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Ficus.RRBVectorBetterNodes
open FsCheck
open RRBMath
open Ficus.RRBVectorBetterNodes

module Literals = Ficus.Literals
let logger = Log.create "Expecto"

// TODO: Test all the low-level node-manipulation functions in all four node variants: compact/extended, full/relaxed
// Make sure that we haven't created a situation where we'll end up doing the wrong thing in one specific variant

// For these tests, make a bunch of leaves, and then a node above those leaves that will be either expanded or compact
// If we're testing full nodes, force all but the last leaf to be full, and the last leaf should have a 25% chance of being full (so that the node is COMPLETELY full)
// If we're testing relaxed nodes, have the leaves be anywhere from half-full to completely full, with a 25% chance of being completely full.
// So our leaf generator is: 25% of 32 items, or else a random number of items between 16 and 32 inclusive.
// Also use our standard tree generation code to make a bunch of trees of height ONE (only), and run the tests against those trees as well -- just to be safe(r).

// Tests will be things like "Here's a new, randomly-generated child leaf. Append it to the existing node and make sure the node is still full / becomes relaxed as appropriate"
// And we'll do some manually-generated tests (e.g., here's a manually-generated full leaf, or here's a manually generated all-but-last-is-full node which should become relaxed after append)



// === GENERATORS copied from ExpectoTest.fs ===
// We may or may not end up using these, but this is a good example of what to do.
// I'll probably write some simpler functions, since generating arbitrary arrays isn't really needed. All we really need is to generate arrays of length N, containing [|1..N|]

// For various tests, we'll want to generate a list and an index of an item within that list.
// Note that if the list is empty, the index will be 0, which is not a valid index of an
// empty list. So for some tests, we'll want to use arbNonEmptyArrayAndIdx instead.
type ArrayAndIdx = ArrayAndIdx of arr:int[] * idx:int
type NonEmptyArrayAndIdx = NonEmptyArrayAndIdx of arr:int[] * idx:int
type LeafPlusArrAndIdx = LeafPlusArrAndIdx of leaf:RRBLeafNode<int> * arr:int[] * idx:int
type NonEmptyLeafPlusArrAndIdx = NonEmptyLeafPlusArrAndIdx of leaf:RRBLeafNode<int> * arr:int[] * idx:int

// TODO: Determine if we can use Arb.from<PositiveInt> here, or Gen.nonEmptyListOf, or something
let genArray = Gen.sized <| fun s -> gen {
    let! arr = Gen.arrayOfLength s (Gen.choose (1,100))
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return ArrayAndIdx (arr,idx)
}
let genArraySimpler = gen {
    let! arr = Gen.arrayOf (Gen.choose (1,100))
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return ArrayAndIdx (arr,idx)
}
let genNonEmptyArray = Gen.sized <| fun s -> gen {
    let! arr = Gen.arrayOfLength (Operators.max s 1) (Gen.choose (1,100))
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return NonEmptyArrayAndIdx (arr,idx)
}
let genNonEmptyArraySimpler = gen {
    let! lst = Gen.nonEmptyListOf (Gen.choose (1,100))
    let arr = Array.ofList lst
    let! idx = Gen.choose (0, Operators.max 0 (Array.length arr - 1))
    return NonEmptyArrayAndIdx (arr,idx)
}
let mapArrAndIdxToLeaf (ArrayAndIdx (arr,idx)) = LeafPlusArrAndIdx (RRBNode<int>.MkLeaf nullOwner arr, arr, idx)
let mapNEArrAndIdxToNELeaf (NonEmptyArrayAndIdx (arr,idx)) = NonEmptyLeafPlusArrAndIdx (RRBNode<int>.MkLeaf nullOwner arr, arr, idx)

let genLeafPlusArrAndIdx = genArraySimpler |> Gen.map mapArrAndIdxToLeaf
let genNonEmptyLeafPlusArrAndIdx = genNonEmptyArraySimpler |> Gen.map mapNEArrAndIdxToNELeaf

let rec shrink (ArrayAndIdx (arr:int[],idx:int)) = seq {
    if arr.Length <= 0 then yield! Seq.empty else
    let rest = arr |> Array.skip 1
    let i = Operators.max 0 (Operators.min idx rest.Length - 1)
    yield ArrayAndIdx (rest,i)
    yield! shrink (ArrayAndIdx (rest,i))
}
let rec shrinkNonEmpty (NonEmptyArrayAndIdx (arr:int[],idx:int)) = seq {
    if arr.Length <= 1 then yield! Seq.empty else
    let rest = arr |> Array.skip 1
    let i = Operators.max 0 (Operators.min idx rest.Length - 1)
    yield NonEmptyArrayAndIdx (rest,i)
    yield! shrinkNonEmpty (NonEmptyArrayAndIdx (rest,i))
}
let shrinkLeaf (LeafPlusArrAndIdx (leaf,arr,idx)) =
    shrink (ArrayAndIdx (arr,idx)) |> Seq.map mapArrAndIdxToLeaf
let shrinkNonEmptyLeaf (NonEmptyLeafPlusArrAndIdx (leaf,arr,idx)) =
    shrinkNonEmpty (NonEmptyArrayAndIdx (arr,idx)) |> Seq.map mapNEArrAndIdxToNELeaf

let genDebug = Gen.sized <| fun s ->
    logger.debug (
        eventX "Generated number with size {size}"
        >> setField "size" s
    )
    Gen.choose(0,s)

// === From-scratch generators ===

let mkArr n = [| 1..n |]
let mkLeaf n = RRBLeafNode(nullOwner, mkArr n)
let fullLeaf = mkLeaf Literals.blockSize

type NodeKind =
    | Full
    | Relaxed
    | ExpandedFull
    | ExpandedRelaxed

let nodeKind (node : RRBFullNode<'T>) =
    match node with
    | :? RRBExpandedRelaxedNode<'T> -> ExpandedRelaxed
    | :? RRBExpandedFullNode<'T> -> ExpandedFull
    | :? RRBRelaxedNode<'T> -> Relaxed
    | _ -> Full

let genLeaf =
    Gen.oneof [ Gen.constant Literals.blockSize ; Gen.choose (Literals.blockSize / 2, Literals.blockSize) ]
    |> Gen.map mkLeaf

let genLeaves = Gen.nonEmptyListOf genLeaf |> Gen.map Array.ofList

let genFullLeaves = Gen.nonEmptyListOf (Gen.constant fullLeaf) |> Gen.map Array.ofList

let genFullLeavesExceptLast =
    let allButLast = Gen.sized <| fun s -> Gen.resize (s - 1 |> max 0) genFullLeaves
    let lastLeaf = genLeaf |> Gen.map Array.singleton
    Gen.zip allButLast lastLeaf |> Gen.map (fun (a1,a2) -> Array.append a1 a2)

let genLeavesForOneFullNode n =
    Gen.oneof [genFullLeavesExceptLast; genFullLeaves]
    |> Gen.map (fun leaves -> leaves |> Array.truncate n |> Array.map (fun l -> l :> RRBNode<int>))

let genLeavesForOneRelaxedNode n =
    genLeaves
    |> Gen.map (fun leaves -> leaves |> Array.truncate n |> Array.map (fun l -> l :> RRBNode<int>))

let genLeavesForMultipleFullNodes =
    Gen.oneof [genFullLeavesExceptLast; genFullLeaves]
    |> Gen.map (fun leaves -> leaves |> Array.chunkBySize Literals.blockSize |> Array.map (Array.map (fun l -> l :> RRBNode<int>)))

let genLeavesForMultipleRelaxedNodes =
    genLeaves
    |> Gen.map (fun leaves -> leaves |> Array.chunkBySize Literals.blockSize |> Array.map (Array.map (fun l -> l :> RRBNode<int>)))

let mkFullNode children = RRBNode<int>.MkFullNode nullOwner children
let mkExpandedFullNode children = RRBExpandedFullNode<int>(nullOwner, children) :> RRBFullNode<int>
let mkRelaxedNode children = RRBNode<int>.MkNode nullOwner Literals.blockSizeShift children
let mkExpandedRelaxedNode children =
    let sizeTable = RRBNode<int>.CreateSizeTable Literals.blockSizeShift children
    if isSizeTableFullAtShift Literals.blockSizeShift sizeTable sizeTable.Length
    then RRBExpandedFullNode<int>(nullOwner, children) :> RRBFullNode<int>
    else RRBExpandedRelaxedNode<int>(nullOwner, children, sizeTable) :> RRBFullNode<int>

let genFullNode n = genLeavesForOneFullNode n |> Gen.map mkFullNode
let genExpandedFullNode n = genLeavesForOneFullNode n |> Gen.map mkExpandedFullNode
let genRelaxedNode n = genLeavesForOneRelaxedNode n |> Gen.map mkRelaxedNode
let genExpandedRelaxedNode n = genLeavesForOneRelaxedNode n |> Gen.map mkExpandedRelaxedNode

let genNode = Gen.oneof [ genFullNode Literals.blockSize; genExpandedFullNode Literals.blockSize; genRelaxedNode Literals.blockSize; genExpandedRelaxedNode Literals.blockSize ]
let genShortNode = Gen.oneof [ genFullNode (Literals.blockSize - 1); genExpandedFullNode (Literals.blockSize - 1); genRelaxedNode (Literals.blockSize - 1); genExpandedRelaxedNode (Literals.blockSize  - 1)]

let genSmallFullTree =
    genLeavesForMultipleFullNodes
    |> Gen.map (fun leafChunks -> leafChunks |> Array.map mkFullNode)
    |> Gen.map (fun nodes -> if nodes.Length = 1 then Array.exactlyOne nodes else mkFullNode (nodes |> Array.map (fun node -> node :> RRBNode<int>)))

let genSmallRelaxedTree =
    genLeavesForMultipleRelaxedNodes
    |> Gen.map (fun leafChunks -> leafChunks |> Array.map mkRelaxedNode)
    |> Gen.map (fun nodes -> if nodes.Length = 1 then Array.exactlyOne nodes else mkRelaxedNode (nodes |> Array.map (fun node -> node :> RRBNode<int>)))

let toTransient (root : RRBFullNode<'T>) =
    let newToken = mkOwnerToken()
    let rec expandNode token (root : RRBNode<'T>) =
        if root :? RRBLeafNode<'T>
        then (root.GetEditableNode token), 0
        else
            let child, childShift = (root :?> RRBFullNode<'T>).LastChild |> expandNode token
            let thisShift = up childShift
            let root' = ((root.Expand token) :?> RRBFullNode<'T>).UpdateChild token thisShift (root.NodeSize - 1) child
            root' :> RRBNode<'T>, thisShift
    (expandNode newToken root |> fst) :?> RRBFullNode<'T>

let genTransientSmallFullTree = genSmallFullTree |> Gen.map toTransient

let genTransientSmallRelaxedTree = genSmallRelaxedTree |> Gen.map toTransient

let genTree = Gen.oneof [ genSmallFullTree; genTransientSmallFullTree; genSmallRelaxedTree; genTransientSmallRelaxedTree ]

type IsolatedNode<'T> = IsolatedNode of RRBFullNode<'T>
type IsolatedShortNode<'T> = IsolatedShortNode of RRBFullNode<'T>
type RootNode<'T> = RootNode of RRBFullNode<'T>

// TODO: Write shrinkers for nodes and for trees

type MyGenerators =
    static member arbArrayAndIdx() =
        { new Arbitrary<ArrayAndIdx>() with
            override x.Generator = genArraySimpler
            override x.Shrinker t = shrink t }
    static member arbNonEmptyArrayAndIdx() =
        { new Arbitrary<NonEmptyArrayAndIdx>() with
            override x.Generator = genNonEmptyArraySimpler
            override x.Shrinker t = shrinkNonEmpty t }
    static member arbLeaf() =
        { new Arbitrary<LeafPlusArrAndIdx>() with
            override x.Generator = genLeafPlusArrAndIdx
            override x.Shrinker t = shrinkLeaf t }
    static member arbNonEmptyLeaf() =
        { new Arbitrary<NonEmptyLeafPlusArrAndIdx>() with
            override x.Generator = genNonEmptyLeafPlusArrAndIdx
            override x.Shrinker t = shrinkNonEmptyLeaf t }
    static member arbInt() =
        { new Arbitrary<int>() with
            override x.Generator = genDebug }
    static member arbTree() =
        { new Arbitrary<RootNode<int>>() with
            override x.Generator = genTree |> Gen.map RootNode }
    static member arbNode() =
        { new Arbitrary<IsolatedNode<int>>() with
            override x.Generator = genNode |> Gen.map IsolatedNode }
    static member arbShortNode() =
        { new Arbitrary<IsolatedShortNode<int>>() with
            override x.Generator = genShortNode |> Gen.map IsolatedShortNode }

// Now we can write test properties that take an IsolatedNode<int> or a RootNode<int>

Arb.register<MyGenerators>() |> ignore
let testProp  name fn =  testPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] } name fn
let ptestProp name fn = ptestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] } name fn
let ftestProp replay name fn = ftestPropertyWithConfig replay { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] } name fn

// === Tests here ===


(*

AppendChild ch
AppendChildS ch sz
InsertChild n ch
InsertChildS n ch sz
RemoveChild n
RemoveChildS n sz?
RemoveLastChild
UpdateChild n ch'
UpdateChildSAbs n ch' sz
UpdateChildSRel n ch' relSz
KeepNLeft n -> Node
SplitAndKeepNLeft n -> Node, arr of item
SplitAndKeepNLeftS n -> Node, arr of (item * size)
KeepNRight n -> Node
SplitAndKeepNRight n -> arr of item, Node
SplitAndKeepNRightS n -> arr of (item * size), Node
AppendNChildren n seq<ch>
AppendNChildrenS n seq<ch> seq<sz>
PrependNChildren n seq<ch>
PrependNChildrenS n seq<ch> seq<sz>

*)

let mkManualNodeA (leafSizes : int []) =
    leafSizes |> Array.truncate Literals.blockSize |> Array.map (mkLeaf >> fun leaf -> leaf :> RRBNode<int>) |> mkRelaxedNode

let mkManualNode (leafSizes : int list) =
    leafSizes |> Array.ofList |> mkManualNodeA

let [<Literal>] M = Literals.blockSize  // Shorthand




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

type Fullness = CompletelyFull | FullEnough | NotFull   // Used in node properties

let nodeProperties = [
    "All twigs should be at height 1", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift (node : RRBFullNode<'T>) =
            if shift <= Literals.blockSizeShift then
                not (isEmpty node) && isTwig node
            else
                not (isEmpty node) && isNotTwig node && children node |> Seq.forall (fun n -> check (down shift) (n :?> RRBFullNode<'T>))
        if root |> isEmpty then true
        else check shift root

    // TODO: Write another one that says "All leaves should be at height 0"

    "The number of items in each node and leaf should be <= the branching factor (Literals.blockSize)", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then
                node.NodeSize <= Literals.blockSize
            else
                node.NodeSize <= Literals.blockSize &&
                children node |> Seq.forall (check (down shift))
        check shift root

    "The tree size of any node should match the total number of items its descendent leaves contain", fun (shift : int) (root : RRBFullNode<'T>) ->
        let check shift (node : RRBFullNode<'T>) =
            node |> itemCount shift = node.TreeSize shift
        check shift root

    "The tree size of any leaf should equal its node size", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0
            then node.NodeSize = node.TreeSize shift
            else children node |> Seq.forall (check (down shift))
        check shift root

    "The size table of any tree node should match the cumulative tree sizes of its children", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift (node : RRBFullNode<'T>) =
            let sizeTable = if isRelaxed node then (node :?> RRBRelaxedNode<'T>).SizeTable else node.BuildSizeTable shift node.NodeSize (node.NodeSize - 1)
                            |> Array.truncate node.NodeSize
            let expectedSizeTable = children node |> Seq.map (fun child -> child.TreeSize (down shift)) |> Seq.scan (+) 0 |> Seq.skip 1 |> Array.ofSeq
            sizeTable = expectedSizeTable
        check shift root

    "The size table of any tree node should have as many entries as its # of direct child nodes", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check (node : RRBFullNode<'T>) =
            if isRelaxed node
            then Array.length (node :?> RRBRelaxedNode<'T>).SizeTable = Array.length node.Children
            else true
        check root

    "A full node should never contain less-than-full children except as its last child", fun (shift : int) (root : RRBFullNode<'T>) ->
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

    "All nodes at shift 0 should be leaves", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift node =
            if shift <= 0 then node |> isLeaf else children node |> Seq.forall (check (down shift))
        check shift root

    "Internal nodes that are at shift > 0 should never be leaves", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift node =
            if shift > 0 then node |> isNode && children node |> Seq.forall (check (down shift)) else true
        (* if root |> isEmpty then true else *)
        check shift root

    "A tree should not contain any empty nodes", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift node =
            node |> (not << isEmpty) &&
            if shift <= 0 then true else children node |> Seq.forall (check (down shift))
        check shift root

    "No RRBRelaxedNode should contain a \"full\" size table. If the size table was full, it should have been turned into an RRBFullNode.", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then true else
            let nodeValid = if isRelaxed node then not (isSizeTableFullAtShift shift (node :?> RRBRelaxedNode<'T>).SizeTable node.NodeSize) else true
            nodeValid && children node |> Seq.forall (check (down shift))
        check shift root

    "The shift of a vector should always be a multiple of Literals.blockSizeShift", fun (shift : int) (root : RRBFullNode<'T>) ->
        shift % Literals.blockSizeShift = 0

    "The shift of a vector should always be the height from the root to the leaves, multiplied by Literals.blockSizeShift", fun (shift : int) (root : RRBFullNode<'T>) ->
        let rec height acc (node : RRBNode<'T>) =
            if isLeaf node then acc else (node :?> RRBFullNode<'T>).FirstChild |> height (acc+1)
        (height 0 root) * Literals.blockSizeShift = shift

    "ExpandedNodes (and ExpandedRRBNodes) should not appear in a tree whose root is not an expanded node variant", fun (shift : int) (root : RRBFullNode<'T>) ->
        let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
        let rec check shift (node : RRBNode<'T>) =
            if shift <= 0 then true
            elif isExpanded node then false
            else children node |> Seq.forall (check (down shift))
        if isExpanded root then true else check shift root

    "If a tree's root is an expanded Node variant, its right spine should contain expanded nodes but nothing else should", fun (shift : int) (root : RRBFullNode<'T>) ->
        let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
        let rec check shift isLast (node : RRBNode<'T>) =
            if shift <= 0 then true
            elif isExpanded node && not isLast then false
            elif isLast && not (isExpanded node) then false
            else
                let checkResultForAllButLast = children node |> Seq.take (node.NodeSize - 1) |> Seq.forall (check (down shift) false)
                let checkResultForLastChild = children node |> Seq.skip (node.NodeSize - 1) |> Seq.head |> check (down shift) true
                checkResultForAllButLast && checkResultForLastChild
        if isExpanded root then check shift true root else true
]





type PropResult = string list

let checkProperty name pred shift root =
    try
        if pred shift root then [] else [name]
    with
    | :? System.InvalidCastException ->
        ["Invalid cast while checking " + name]
    | :? System.IndexOutOfRangeException ->
        ["Index out of range while checking " + name]

let combine r1 r2 = (r1 @ r2)

let getNodePropertyResults shift root =
    nodeProperties |> List.map (fun (name,pred) -> checkProperty name pred shift root) |> List.fold combine []

let checkProperties shift root label =
    let result = getNodePropertyResults shift root
    match result with
    | [] -> ()
    | errors -> Expecto.Tests.failtestf "%s with shift=%d and root=%A\nfailed the following RRBVector invariants:\n%A" label shift root errors

let checkPropertiesSimple shift root = checkProperties shift root "Node"








let appendTests =
  testList "Append tests" [
    testCase "AppendChild on a singleton node" <| fun _ ->
        // There will be several arbitrary tests like this. Note that empty nodes are pretty much totally excluded from consideration: they're not allowed.
        let node = mkManualNode [M-2]
        checkProperties Literals.blockSizeShift node "Starting node"
        let newChild = mkLeaf (M-2)
        let result = node.AppendChild nullOwner Literals.blockSizeShift newChild
        checkProperties Literals.blockSizeShift result "Result"

    testProp "AppendChild on a generated node" <| fun (IsolatedShortNode node) ->
        // logger.debug (
        //     eventX "Node generated: {node}"
        //     >> setField "node" (sprintf "%A" node)
        // )
        checkProperties Literals.blockSizeShift node "Starting node"
        let newChild = mkLeaf (M-2)
        let result = node.AppendChild nullOwner Literals.blockSizeShift newChild
        // logger.debug (
        //     eventX "Result: {node}"
        //     >> setField "node" (sprintf "%A" result)
        // )
        checkProperties Literals.blockSizeShift result "Result"
  ]

let tests = appendTests
