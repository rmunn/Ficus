module ExpectoTemplate.RRBVectorBetterNodesExpectoTest

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Ficus.RRBVectorBetterNodes
open FsCheck
open RRBMath

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



// === Generators ===

let mkCounter() =
    let mutable i = 0
    fun () ->
        i <- i + 1
        i

let mkSimpleArr n = [| 1..n |]
let mkArr counter n = Array.init n (fun _ -> counter())
let mkSimpleLeaf n = RRBLeafNode(nullOwner, mkSimpleArr n)
let mkLeaf counter n = RRBLeafNode(nullOwner, mkArr counter n)
let fullSimpleLeaf = mkSimpleLeaf Literals.blockSize
let fullLeaf counter () = mkLeaf counter Literals.blockSize

type NodeKind =
    | Full
    | Relaxed
    | ExpandedFull
    | ExpandedRelaxed

let nodeKind (node : RRBNode<'T>) =
    match node with
    | :? RRBExpandedRelaxedNode<'T> -> ExpandedRelaxed
    | :? RRBExpandedFullNode<'T> -> ExpandedFull
    | :? RRBRelaxedNode<'T> -> Relaxed
    | _ -> Full

let genSimpleLeaf =
    Gen.oneof [ Gen.constant Literals.blockSize ; Gen.choose (Literals.blockSize / 2, Literals.blockSize) ]
    |> Gen.map mkSimpleLeaf

let genLeaf counter =
    Gen.oneof [ Gen.constant Literals.blockSize ; Gen.choose (Literals.blockSize / 2, Literals.blockSize) ]
    |> Gen.map (mkLeaf counter)

let genLeaves =
    let counter = mkCounter()
    Gen.nonEmptyListOf (genLeaf counter) |> Gen.map Array.ofList

let genFullLeaves =
    let counter = mkCounter()
    Gen.nonEmptyListOf (Gen.fresh (fullLeaf counter)) |> Gen.map Array.ofList

let genFullLeavesExceptLast =
    let allButLast = Gen.sized <| fun s -> Gen.resize (s - 1 |> max 0) genFullLeaves
    let lastLeaf = genLeaf (mkCounter()) |> Gen.map Array.singleton
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

let mkFullTwig children = RRBNode<int>.MkFullNode nullOwner children
let mkExpandedFullTwig children = RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>
let mkRelaxedTwig children = RRBNode<int>.MkNode nullOwner Literals.blockSizeShift children
let mkExpandedRelaxedTwig children =
    let sizeTable = RRBNode<int>.CreateSizeTable Literals.blockSizeShift children
    if isSizeTableFullAtShift Literals.blockSizeShift sizeTable sizeTable.Length
    then RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>
    else RRBExpandedRelaxedNode<int>(nullOwner, children, sizeTable) :> RRBNode<int>

let mkFullNode shift children = RRBNode<int>.MkFullNode nullOwner children
let mkExpandedFullNode shift children = RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>
let mkRelaxedNode shift children = RRBNode<int>.MkNode nullOwner shift children
let mkExpandedRelaxedNode shift children =
    let sizeTable = RRBNode<int>.CreateSizeTable shift children
    if isSizeTableFullAtShift shift sizeTable sizeTable.Length
    then RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>
    else RRBExpandedRelaxedNode<int>(nullOwner, children, sizeTable) :> RRBNode<int>

let genFullNode n = genLeavesForOneFullNode n |> Gen.map mkFullTwig
let genExpandedFullNode n = genLeavesForOneFullNode n |> Gen.map mkExpandedFullTwig
let genRelaxedNode n = genLeavesForOneRelaxedNode n |> Gen.map mkRelaxedTwig
let genExpandedRelaxedNode n = genLeavesForOneRelaxedNode n |> Gen.map mkExpandedRelaxedTwig

let genNode = Gen.oneof [ genFullNode Literals.blockSize; genExpandedFullNode Literals.blockSize; genRelaxedNode Literals.blockSize; genExpandedRelaxedNode Literals.blockSize ]
let genShortNode = Gen.oneof [ genFullNode (Literals.blockSize - 1); genExpandedFullNode (Literals.blockSize - 1); genRelaxedNode (Literals.blockSize - 1); genExpandedRelaxedNode (Literals.blockSize  - 1)]

let genSmallFullTree =
    genLeavesForMultipleFullNodes
    |> Gen.map (fun leafChunks -> leafChunks |> Array.truncate Literals.blockSize |> Array.map mkFullTwig)
    |> Gen.map (fun nodes -> if nodes.Length = 1 then Array.exactlyOne nodes else mkFullNode (2 * Literals.blockSizeShift) nodes)

let genSmallRelaxedTree =
    genLeavesForMultipleRelaxedNodes
    |> Gen.map (fun leafChunks -> leafChunks |> Array.truncate Literals.blockSize |> Array.map mkRelaxedTwig)
    |> Gen.map (fun nodes -> if nodes.Length = 1 then Array.exactlyOne nodes else mkRelaxedNode (2 * Literals.blockSizeShift) nodes)

let toTransient (root : RRBNode<'T>) =
    let newToken = mkOwnerToken()
    let rec expandNode token (root : RRBNode<'T>) =
        if root :? RRBLeafNode<'T>
        then (root.GetEditableNode token), 0
        else
            let child, childShift = (root :?> RRBFullNode<'T>).LastChild |> expandNode token
            let thisShift = up childShift
            let root' = ((root.Expand token) :?> RRBFullNode<'T>).UpdateChild token thisShift (root.NodeSize - 1) child
            root', thisShift
    expandNode newToken root |> fst

let genTransientSmallFullTree = genSmallFullTree |> Gen.map toTransient

let genTransientSmallRelaxedTree = genSmallRelaxedTree |> Gen.map toTransient

let genTree = Gen.oneof [ genSmallFullTree; genTransientSmallFullTree; genSmallRelaxedTree; genTransientSmallRelaxedTree ]
            //   |> Gen.map (fun node ->
            //     logger.debug (
            //         eventX "Generated node: {node}"
            //         >> setField "node" (sprintf "%A" node)
            //     )
            //     node)

type IsolatedNode<'T> = IsolatedNode of RRBFullNode<'T>
type IsolatedShortNode<'T> = IsolatedShortNode of RRBFullNode<'T>
type RootNode<'T> = RootNode of RRBFullNode<'T>
type LeafNode<'T> = LeafNode of RRBLeafNode<'T>

// TODO: Write shrinkers for nodes and for trees

type MyGenerators =
    static member arbTree() =
        { new Arbitrary<RootNode<int>>() with
            override x.Generator = genTree |> Gen.map (fun node -> RootNode (node :?> RRBFullNode<int>)) }
    static member arbLeaf() =
        { new Arbitrary<LeafNode<int>>() with
            override x.Generator = let counter = mkCounter() in genLeaf counter |> Gen.map LeafNode }
    static member arbLeaves() =
        { new Arbitrary<RRBLeafNode<int> []>() with
            override x.Generator = genLeaves }
    static member arbNode() =
        { new Arbitrary<IsolatedNode<int>>() with
            override x.Generator = genNode |> Gen.map (fun node -> IsolatedNode (node :?> RRBFullNode<int>)) }
    static member arbShortNode() =
        { new Arbitrary<IsolatedShortNode<int>>() with
            override x.Generator = genShortNode |> Gen.map (fun node -> IsolatedShortNode (node :?> RRBFullNode<int>)) }

// Now we can write test properties that take an IsolatedNode<int> or a RootNode<int>

Arb.register<MyGenerators>() |> ignore
let testProp  name fn =  testPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] } name fn
let ptestProp name fn = ptestPropertyWithConfig { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] } name fn
let ftestProp replay name fn = ftestPropertyWithConfig replay { FsCheckConfig.defaultConfig with arbitrary = [typeof<MyGenerators>] } name fn

// === Tests here ===



let mkManualNodeA (leafSizes : int []) =
    let counter = mkCounter()
    leafSizes |> Array.truncate Literals.blockSize |> Array.map (mkLeaf counter >> fun leaf -> leaf :> RRBNode<int>) |> mkRelaxedTwig

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

let rec height (node : RRBNode<'T>) =
    if isEmpty node || isLeaf node
    then 0
    else 1 + height (node :?> RRBFullNode<'T>).FirstChild

type Fullness = CompletelyFull | FullEnough | NotFull   // Used in node properties

let nodeProperties = [
    "All twigs should be at height 1", fun (shift : int) (root : RRBNode<'T>) ->
        let rec check shift (node : RRBNode<'T>) =
            if shift <= Literals.blockSizeShift then
                not (isEmpty node) && isTwig node
            else
                not (isEmpty node) && isNotTwig node && children node |> Seq.forall (fun n -> check (down shift) (n :?> RRBFullNode<'T>))
        if root |> isEmpty then true
        else check shift root

    // TODO: Write another one that says "All leaves should be at height 0"

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

    // "ExpandedNodes (and ExpandedRRBNodes) should not appear in a tree whose root is not an expanded node variant", fun (shift : int) (root : RRBNode<'T>) ->
    //     let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
    //     let rec check shift (node : RRBNode<'T>) =
    //         if shift <= 0 then true
    //         elif isExpanded node then false
    //         else children node |> Seq.forall (check (down shift))
    //     if isExpanded root then true else check shift root

    // "If a tree's root is an expanded Node variant, its right spine should contain expanded nodes but nothing else should", fun (shift : int) (root : RRBNode<'T>) ->
    //     let isExpanded (child : RRBNode<'T>) = (child :? RRBExpandedFullNode<'T>) || (child :? RRBExpandedRelaxedNode<'T>)
    //     let rec check shift isLast (node : RRBNode<'T>) =
    //         if shift <= 0 then true
    //         elif isExpanded node && not isLast then false
    //         elif isLast && not (isExpanded node) then false
    //         else
    //             let checkResultForAllButLast = children node |> Seq.take (node.NodeSize - 1) |> Seq.forall (check (down shift) false)
    //             let checkResultForLastChild = children node |> Seq.skip (node.NodeSize - 1) |> Seq.head |> check (down shift) true
    //             checkResultForAllButLast && checkResultForLastChild
    //     if isExpanded root then check shift true root else true

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





type PropResult = string list

let checkProperty name pred shift root =
    try
        // logger.debug(eventX "Checking property {name} on root {root}" >> setField "name" name >> setField "root" (sprintf "%A" root))
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






type ExpectedResult = Full | Relaxed

let inputDataForAppendTests : (int list * int * ExpectedResult * string) list =
  [ // Initial leaves, size of inserted node, expected fullness of result, partial name
    [M-2], M-2, Relaxed, "singleton"
    [M-2], M, Relaxed, "singleton"
    [M], M-2, Full, "singleton"
    [M], M, Full, "singleton"
    [M; M-2], M-2, Relaxed, "two-element"
    [M; M-2], M, Relaxed, "two-element"
    [M; M], M-2, Full, "two-element"
    [M; M], M, Full, "two-element"
    List.replicate (M-2) M, M-2, Full, "size M-2"
    List.replicate (M-2) M, M, Full, "size M-2"
    List.replicate (M-3) M @ [M-1], M-2, Relaxed, "size M-2"
    List.replicate (M-3) M @ [M-1], M, Relaxed, "size M-2"
    List.replicate (M-1) M, M-2, Full, "size M-1"
    List.replicate (M-1) M, M, Full, "size M-1"
    List.replicate (M-2) M @ [M-1], M-2, Relaxed, "size M-1"
    List.replicate (M-2) M @ [M-1], M, Relaxed, "size M-1"
  ]

let mkAppendTests (leafSizes, newLeafSize, expectedResult, namePart) =
    let counter = mkCounter()
    let nodeDesc = match expectedResult with Full -> "completely full" | Relaxed -> "nearly-full"
    let leafDesc = if newLeafSize = Literals.blockSize then "full" else "non-full"
    let isWhat = match expectedResult with Full -> isFull | Relaxed -> isRelaxed
    let isWhatStr = match expectedResult with Full -> "full" | Relaxed -> "relaxed"
    ["AppendChild"; "AppendChildS"]
    |> List.map (fun fname ->
        let test = fun _ ->
            let node = mkManualNode leafSizes :?> RRBFullNode<int>
            checkProperties Literals.blockSizeShift node "Starting node"
            let newChild = mkLeaf counter newLeafSize
            let result =
                if fname = "AppendChild" then
                    node.AppendChild nullOwner Literals.blockSizeShift newChild
                elif fname = "AppendChildS" then
                    node.AppendChildS nullOwner Literals.blockSizeShift newChild newLeafSize
                else
                    failwith <| sprintf "Unknown method name %s in test creation - fix unit tests" fname
            checkProperties Literals.blockSizeShift result "Result"
            Expect.equal (result.NodeSize) (node.NodeSize + 1) "Appending any leaf to a node should increase its node size by 1"
            Expect.equal (result.TreeSize Literals.blockSizeShift) (node.TreeSize Literals.blockSizeShift + newLeafSize) "Appending any leaf to a node should increase its tree size by the leaf's NodeSize"
            Expect.isTrue (isWhat result) (sprintf "Appending any leaf to a %s node should result in a %s node" nodeDesc isWhatStr)
        let name = sprintf "%s on a %s %s node with a %s leaf" fname nodeDesc namePart leafDesc
        testCase name test
    )

let appendTests =
    inputDataForAppendTests
    |> List.collect mkAppendTests
    |> testList "Append tests"

let appendPropertyTests =
  testList "Append property tests" [
    testProp "AppendChild on a generated node" <| fun (IsolatedShortNode node) ->
        checkProperties Literals.blockSizeShift node "Starting node"
        let newChild = mkSimpleLeaf (M-2)
        let result = node.AppendChild nullOwner Literals.blockSizeShift newChild
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize + 1

    testProp "AppendChildS on a generated node" <| fun (IsolatedShortNode node) ->
        checkProperties Literals.blockSizeShift node "Starting node"
        let newChild = mkSimpleLeaf (M-2)
        let result = node.AppendChildS nullOwner Literals.blockSizeShift newChild (M-2)
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize + 1
  ]


let inputDataForInsertTests : (int list * int * int * ExpectedResult * string) list =
  [ // Initial leaves, insert position, size of inserted node, expected fullness of result, partial name
    [M-2], 0, M-2, Relaxed, "nearly-full singleton"
    [M-2], 1, M-2, Relaxed, "nearly-full singleton"
    [M-2], 0, M, Full, "nearly-full singleton"
    [M-2], 1, M, Relaxed, "nearly-full singleton"
    [M], 0, M-2, Relaxed, "completely full singleton"
    [M], 1, M-2, Full, "completely full singleton"
    [M], 0, M, Full, "completely full singleton"
    [M], 1, M, Full, "completely full singleton"
    [M; M-2], 0, M-2, Relaxed, "nearly-full two-element"
    [M; M-2], 1, M-2, Relaxed, "nearly-full two-element"
    [M; M-2], 2, M-2, Relaxed, "nearly-full two-element"
    [M; M-2], 0, M, Full, "nearly-full two-element"
    [M; M-2], 1, M, Full, "nearly-full two-element"
    [M; M-2], 2, M, Relaxed, "nearly-full two-element"
    [M; M], 0, M-2, Relaxed, "completely full two-element"
    [M; M], 1, M-2, Relaxed, "completely full two-element"
    [M; M], 2, M-2, Full, "completely full two-element"
    [M; M], 0, M, Full, "completely full two-element"
    [M; M], 1, M, Full, "completely full two-element"
    [M; M], 2, M, Full, "completely full two-element"
    List.replicate (M-2) M, 0, M-2, Relaxed, "completely full size M-2"
    List.replicate (M-2) M, M/2, M-2, Relaxed, "completely full size M-2"
    List.replicate (M-2) M, M-2, M-2, Full, "completely full size M-2"
    List.replicate (M-2) M, 0, M, Full, "completely full size M-2"
    List.replicate (M-2) M, M/2, M, Full, "completely full size M-2"
    List.replicate (M-2) M, M-2, M, Full, "completely full size M-2"
    List.replicate (M-3) M @ [M-1], 0, M-2, Relaxed, "nearly-full size M-2"
    List.replicate (M-3) M @ [M-1], M/2, M-2, Relaxed, "nearly-full size M-2"
    List.replicate (M-3) M @ [M-1], M-2, M-2, Relaxed, "nearly-full size M-2"
    List.replicate (M-3) M @ [M-1], 0, M, Full, "nearly-full size M-2"
    List.replicate (M-3) M @ [M-1], M/2, M, Full, "nearly-full size M-2"
    List.replicate (M-3) M @ [M-1], M-2, M, Relaxed, "nearly-full size M-2"
    List.replicate (M-1) M, 0, M-2, Relaxed, "completely full size M-1"
    List.replicate (M-1) M, M/2, M-2, Relaxed, "completely full size M-1"
    List.replicate (M-1) M, M-1, M-2, Full, "completely full size M-1"
    List.replicate (M-1) M, 0, M, Full, "completely full size M-1"
    List.replicate (M-1) M, M/2, M, Full, "completely full size M-1"
    List.replicate (M-1) M, M-1, M, Full, "completely full size M-1"
    List.replicate (M-2) M @ [M-1], 0, M-2, Relaxed, "nearly-full size M-1"
    List.replicate (M-2) M @ [M-1], M/2, M-2, Relaxed, "nearly-full size M-1"
    List.replicate (M-2) M @ [M-1], M-1, M-2, Relaxed, "nearly-full size M-1"
    List.replicate (M-2) M @ [M-1], 0, M, Full, "nearly-full size M-1"
    List.replicate (M-2) M @ [M-1], M/2, M, Full, "nearly-full size M-1"
    List.replicate (M-2) M @ [M-1], M-1, M, Relaxed, "nearly-full size M-1"
  ]

let mkInsertTests (leafSizes, insertPos, newLeafSize, expectedResult, namePart) =
    // let nodeDesc = match expectedResult with Full -> "completely full" | Relaxed -> "nearly-full"
    let leafDesc = if newLeafSize = Literals.blockSize then "full" else "non-full"
    let isWhat = match expectedResult with Full -> isFull | Relaxed -> isRelaxed
    let isWhatStr = match expectedResult with Full -> "full" | Relaxed -> "relaxed"
    ["InsertChild"; "InsertChildS"]
    |> List.map (fun fname ->
        let test = fun _ ->
            let node = mkManualNode leafSizes :?> RRBFullNode<int>
            checkProperties Literals.blockSizeShift node "Starting node"
            let newChild = mkSimpleLeaf newLeafSize
            let result =
                if fname = "InsertChild" then
                    node.InsertChild nullOwner Literals.blockSizeShift insertPos newChild
                elif fname = "InsertChildS" then
                    node.InsertChildS nullOwner Literals.blockSizeShift insertPos newChild newLeafSize
                else
                    failwith <| sprintf "Unknown method name %s in test creation - fix unit tests" fname
            checkProperties Literals.blockSizeShift result "Result"
            Expect.equal (result.NodeSize) (node.NodeSize + 1) "Inserting any leaf into a node should increase its node size by 1"
            Expect.equal (result.TreeSize Literals.blockSizeShift) (node.TreeSize Literals.blockSizeShift + newLeafSize) "Inserting any leaf into a node should increase its tree size by the leaf's NodeSize"
            Expect.isTrue (isWhat result) (sprintf "Inserting a %s leaf into a %s node at position %d should result in a %s node" leafDesc namePart insertPos isWhatStr)
        let name = sprintf "%s on a %s node with a %s leaf inserted at %d" fname namePart leafDesc insertPos
        testCase name test
    )

let insertTests =
    inputDataForInsertTests
    |> List.collect mkInsertTests
    |> testList "Insert tests"

let insertPropertyTests =
  testList "Insert property tests" [
    testProp "InsertChild on a generated node" <| fun (IsolatedShortNode node) (NonNegativeInt idx) ->
        let idx = idx % (node.NodeSize + 1)
        checkProperties Literals.blockSizeShift node "Starting node"
        let newChild = mkSimpleLeaf (M-2)
        let result = node.InsertChild nullOwner Literals.blockSizeShift idx newChild
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize + 1

    testProp "InsertChildS on a generated node" <| fun (IsolatedShortNode node) (NonNegativeInt idx) ->
        let idx = idx % (node.NodeSize + 1)
        checkProperties Literals.blockSizeShift node "Starting node"
        let newChild = mkSimpleLeaf (M-2)
        let result = node.InsertChildS nullOwner Literals.blockSizeShift idx newChild (M-2)
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize + 1
  ]

let removePropertyTests =
  testList "Remove property tests" [
    testProp "RemoveChild on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (NonNegativeInt idx) ->
        node.NodeSize > 1 ==> lazy (
            let idx = idx % node.NodeSize
            checkProperties Literals.blockSizeShift node "Starting node"
            let result = node.RemoveChild nullOwner Literals.blockSizeShift idx
            checkProperties Literals.blockSizeShift result "Result"
            result.NodeSize = node.NodeSize - 1)

    testProp "RemoveLastChild on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) ->
        node.NodeSize > 1 ==> lazy (
            checkProperties Literals.blockSizeShift node "Starting node"
            let result = node.RemoveLastChild nullOwner Literals.blockSizeShift
            checkProperties Literals.blockSizeShift result "Result"
            result.NodeSize = node.NodeSize - 1)
  ]

let updatePropertyTests =
  testList "Update property tests" [
    testProp "UpdateChild on a generated node" <| fun (IsolatedNode node) (NonNegativeInt idx) ->
        let idx = idx % node.NodeSize
        let oldLeaf = node.Children.[idx]
        let newLeaf = mkSimpleLeaf (oldLeaf.NodeSize)
        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.UpdateChild nullOwner Literals.blockSizeShift idx newLeaf
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize && result.TreeSize Literals.blockSizeShift = node.TreeSize Literals.blockSizeShift

    testProp "UpdateChildSAbs on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (NonNegativeInt idx) (LeafNode newLeaf) ->
        let idx = idx % node.NodeSize
        let oldLeaf = node.Children.[idx]
        let sizeDiff = newLeaf.NodeSize - oldLeaf.NodeSize
        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.UpdateChildSAbs nullOwner Literals.blockSizeShift idx newLeaf newLeaf.NodeSize
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize && result.TreeSize Literals.blockSizeShift = node.TreeSize Literals.blockSizeShift + sizeDiff

    testProp "UpdateChildSRel on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (NonNegativeInt idx) (LeafNode newLeaf) ->
        let idx = idx % node.NodeSize
        let oldLeaf = node.Children.[idx]
        let sizeDiff = newLeaf.NodeSize - oldLeaf.NodeSize
        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.UpdateChildSRel nullOwner Literals.blockSizeShift idx newLeaf sizeDiff
        checkProperties Literals.blockSizeShift result "Result"
        result.NodeSize = node.NodeSize && result.TreeSize Literals.blockSizeShift = node.TreeSize Literals.blockSizeShift + sizeDiff
  ]

let keepPropertyTests =
  testList "KeepN(Left/Right) property tests" [
    testProp "KeepNLeft on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (PositiveInt n) ->
        let n = n % (node.NodeSize + 1) |> max 1
        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.KeepNLeft nullOwner Literals.blockSizeShift n
        checkProperties Literals.blockSizeShift result "Result"
        let keptLeaves = node.Children |> Array.truncate node.NodeSize |> Array.truncate n
        let totalKeptSize = keptLeaves |> Array.sumBy (fun leaf -> leaf.NodeSize)
        result.NodeSize = n && result.TreeSize Literals.blockSizeShift = totalKeptSize

    testProp "KeepNRight on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (PositiveInt n) ->
        let n = n % (node.NodeSize + 1) |> max 1
        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.KeepNRight nullOwner Literals.blockSizeShift n
        checkProperties Literals.blockSizeShift result "Result"
        let keptLeaves = node.Children |> Array.truncate node.NodeSize |> Array.skip (node.NodeSize - n)
        let totalKeptSize = keptLeaves |> Array.sumBy (fun leaf -> leaf.NodeSize)
        result.NodeSize = n && result.TreeSize Literals.blockSizeShift = totalKeptSize
  ]

let splitAndKeepPropertyTests =
  testList "SplitAndKeep property tests" [
    testProp "SplitAndKeepNLeft on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (PositiveInt n) ->
        let n = n % (node.NodeSize + 1) |> max 1
        let origLeavesL, origLeavesR = node.Children |> Array.truncate node.NodeSize |> Array.splitAt n
        let totalKeptL = origLeavesL |> Array.sumBy (fun leaf -> leaf.NodeSize)
        checkProperties Literals.blockSizeShift node "Starting node"
        let resultNode, resultLeavesR = node.SplitAndKeepNLeft nullOwner Literals.blockSizeShift n
        checkProperties Literals.blockSizeShift resultNode "Result"
        Expect.equal resultNode.NodeSize n "Node after split should have N items"
        Expect.equal (Array.length resultLeavesR) (Array.length origLeavesR) "Array of leaves returned from split should have (size - N) items"
        Expect.equal (resultNode.TreeSize Literals.blockSizeShift) totalKeptL "Node after split should have same tree size as total of remaining N items"

    testProp "SplitAndKeepNRight on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (PositiveInt n) ->
        let n = n % (node.NodeSize + 1) |> max 1
        let origLeavesL, origLeavesR = node.Children |> Array.truncate node.NodeSize |> Array.splitAt (node.NodeSize - n)
        let totalKeptR = origLeavesR |> Array.sumBy (fun leaf -> leaf.NodeSize)
        checkProperties Literals.blockSizeShift node "Starting node"
        let resultLeavesL, resultNode = node.SplitAndKeepNRight nullOwner Literals.blockSizeShift n
        checkProperties Literals.blockSizeShift resultNode "Result"
        Expect.equal resultNode.NodeSize n "Node after split should have N items"
        Expect.equal (Array.length resultLeavesL) (Array.length origLeavesL) "Array of leaves returned from split should have (size - N) items"
        Expect.equal (resultNode.TreeSize Literals.blockSizeShift) totalKeptR "Node after split should have same tree size as total of remaining N items"

    testProp "SplitAndKeepNLeftS on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (PositiveInt n) ->
        let n = n % (node.NodeSize + 1) |> max 1
        let origLeavesL, origLeavesR = node.Children |> Array.truncate node.NodeSize |> Array.splitAt n
        let totalKeptL = origLeavesL |> Array.sumBy (fun leaf -> leaf.NodeSize)
        checkProperties Literals.blockSizeShift node "Starting node"
        let resultNode, (resultLeavesR, resultSizesR) = node.SplitAndKeepNLeftS nullOwner Literals.blockSizeShift n
        let expectedSizesR = origLeavesR |> Seq.map (fun leaf -> leaf.NodeSize) |> Seq.scan (+) 0 |> Seq.tail |> Array.ofSeq
        checkProperties Literals.blockSizeShift resultNode "Result"
        Expect.equal resultNode.NodeSize n "Node after split should have N items"
        Expect.equal (Array.length resultLeavesR) (Array.length origLeavesR) "Array of leaves returned from split should have (size - N) items"
        Expect.equal resultSizesR expectedSizesR "Sizes returned from split should be cumulative sizes of leaves returned from split"
        Expect.equal (resultNode.TreeSize Literals.blockSizeShift) totalKeptL "Node after split should have same tree size as total of remaining N items"

    testProp "SplitAndKeepNRightS on a generated node" <| fun (IsolatedNode node : IsolatedNode<int>) (PositiveInt n) ->
        let n = n % (node.NodeSize + 1) |> max 1
        let origLeavesL, origLeavesR = node.Children |> Array.truncate node.NodeSize |> Array.splitAt (node.NodeSize - n)
        let totalKeptR = origLeavesR |> Array.sumBy (fun leaf -> leaf.NodeSize)
        checkProperties Literals.blockSizeShift node "Starting node"
        let (resultLeavesL, resultSizesL), resultNode = node.SplitAndKeepNRightS nullOwner Literals.blockSizeShift n
        let expectedSizesL = origLeavesL |> Seq.map (fun leaf -> leaf.NodeSize) |> Seq.scan (+) 0 |> Seq.tail |> Array.ofSeq
        checkProperties Literals.blockSizeShift resultNode "Result"
        Expect.equal resultNode.NodeSize n "Node after split should have N items"
        Expect.equal (Array.length resultLeavesL) (Array.length origLeavesL) "Array of leaves returned from split should have (size - N) items"
        Expect.equal resultSizesL expectedSizesL "Sizes returned from split should be cumulative sizes of leaves returned from split"
        Expect.equal (resultNode.TreeSize Literals.blockSizeShift) totalKeptR "Node after split should have same tree size as total of remaining N items"
  ]

let appendAndPrependChildrenPropertyTests =
  testList "(Ap/Pre)pendChildren property tests" [
    testProp "AppendNChildren on a generated node" <| fun (IsolatedShortNode node : IsolatedShortNode<int>) (newLeaves : RRBLeafNode<int> []) ->
        let remaining = Literals.blockSize - node.NodeSize
        let toAdd = newLeaves |> Array.truncate remaining
        let n = toAdd.Length
        let origLeafArrays = node.Children |> Array.truncate node.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        let newLeafArrays = toAdd |> Array.map (fun leaf -> leaf.Items)
        let expectedLeafArrays = Array.append origLeafArrays newLeafArrays

        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.AppendNChildren nullOwner Literals.blockSizeShift n (toAdd |> Seq.cast) :?> RRBFullNode<int>
        checkProperties Literals.blockSizeShift result "Result"
        Expect.equal result.NodeSize (node.NodeSize + n) "Node after append should have N more items"
        let actualLeafArrays = result.Children |> Array.truncate result.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        Expect.equal actualLeafArrays expectedLeafArrays "Leaves should have been placed in the correct locations"

    testProp "AppendNChildrenS on a generated node" <| fun (IsolatedShortNode node : IsolatedShortNode<int>) (newLeaves : RRBLeafNode<int> []) ->
        let remaining = Literals.blockSize - node.NodeSize
        let toAdd = newLeaves |> Array.truncate remaining
        let n = toAdd.Length
        let sizes = toAdd |> Seq.map (fun n -> n.NodeSize) |> Seq.scan (+) 0 |> Seq.tail
        let origLeafArrays = node.Children |> Array.truncate node.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        let newLeafArrays = toAdd |> Array.map (fun leaf -> leaf.Items)
        let expectedLeafArrays = Array.append origLeafArrays newLeafArrays

        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.AppendNChildrenS nullOwner Literals.blockSizeShift n (toAdd |> Seq.cast) sizes :?> RRBFullNode<int>
        checkProperties Literals.blockSizeShift result "Result"
        Expect.equal result.NodeSize (node.NodeSize + n) "Node after prepend should have N more items"
        let actualLeafArrays = result.Children |> Array.truncate result.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        Expect.equal actualLeafArrays expectedLeafArrays "Leaves should have been placed in the correct locations"

    testProp "PrependNChildren on a generated node" <| fun (IsolatedShortNode node : IsolatedShortNode<int>) (newLeaves : RRBLeafNode<int> []) ->
        let remaining = Literals.blockSize - node.NodeSize
        let toAdd = newLeaves |> Array.truncate remaining
        let n = toAdd.Length
        let origLeafArrays = node.Children |> Array.truncate node.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        let newLeafArrays = toAdd |> Array.map (fun leaf -> leaf.Items)
        let expectedLeafArrays = Array.append newLeafArrays origLeafArrays

        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.PrependNChildren nullOwner Literals.blockSizeShift n (toAdd |> Seq.cast) :?> RRBFullNode<int>
        checkProperties Literals.blockSizeShift result "Result"
        Expect.equal result.NodeSize (node.NodeSize + n) "Node after prepend should have N more items"
        let actualLeafArrays = result.Children |> Array.truncate result.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        Expect.equal actualLeafArrays expectedLeafArrays "Leaves should have been placed in the correct locations"

    testProp "PrependNChildrenS on a generated node" <| fun (IsolatedShortNode node : IsolatedShortNode<int>) (newLeaves : RRBLeafNode<int> []) ->
        let remaining = Literals.blockSize - node.NodeSize
        let toAdd = newLeaves |> Array.truncate remaining
        let n = toAdd.Length
        let sizes = toAdd |> Seq.map (fun n -> n.NodeSize) |> Seq.scan (+) 0 |> Seq.tail
        let origLeafArrays = node.Children |> Array.truncate node.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        let newLeafArrays = toAdd |> Array.map (fun leaf -> leaf.Items)
        let expectedLeafArrays = Array.append newLeafArrays origLeafArrays

        checkProperties Literals.blockSizeShift node "Starting node"
        let result = node.PrependNChildrenS nullOwner Literals.blockSizeShift n (toAdd |> Seq.cast) sizes :?> RRBFullNode<int>
        checkProperties Literals.blockSizeShift result "Result"
        Expect.equal result.NodeSize (node.NodeSize + n) "Node after prepend should have N more items"
        let actualLeafArrays = result.Children |> Array.truncate result.NodeSize |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)
        Expect.equal actualLeafArrays expectedLeafArrays "Leaves should have been placed in the correct locations"
  ]

let twigItems (node : RRBNode<'T>) =
    (node :?> RRBFullNode<'T>).Children |> Seq.truncate node.NodeSize |> Seq.cast<RRBLeafNode<'T>> |> Seq.collect (fun leaf -> leaf.Items)

let rec nodeItems shift (node : RRBNode<'T>) =
    if shift <= Literals.blockSizeShift then twigItems node else
    (node :?> RRBFullNode<'T>).Children |> Seq.truncate node.NodeSize |> Seq.cast<RRBFullNode<'T>> |> Seq.collect (nodeItems (shift - Literals.blockSizeShift))

let doRebalance2Test shift (nodeL : RRBNode<'T>) (nodeR : RRBNode<'T>) =
    let slotCountL = if shift <= Literals.blockSizeShift then nodeL.TwigSlotCount else nodeL.SlotCount
    let slotCountR = if shift <= Literals.blockSizeShift then nodeR.TwigSlotCount else nodeR.SlotCount
    let totalSize = nodeL.NodeSize + nodeR.NodeSize
    let minSize = (slotCountL + slotCountR - 1) / Literals.blockSize + 1
    let needsRebalancing = totalSize - minSize > Literals.radixSearchErrorMax
    needsRebalancing ==> lazy (
        // Need to do this before the rebalance, because after the rebalance the original nodeL may be invalid if it was an expanded node
        let expected = Seq.append (nodeItems shift nodeL) (nodeItems shift nodeR) |> Array.ofSeq
        let newL, newR = (nodeL :?> RRBFullNode<'T>).Rebalance2Plus1 nullOwner shift None (nodeR :?> RRBFullNode<'T>)
        match newR with
        | None ->
            Expect.isLessThanOrEqual minSize Literals.blockSize "If both nodes add up to a NodeSize of M or less, should end up with just one node at the end"
            Expect.isLessThanOrEqual newL.NodeSize Literals.blockSize "After rebalancing, left node should be at most M items long"
            Expect.equal (nodeItems shift newL |> Array.ofSeq) expected "Order of items should not change during rebalance"
            checkProperties shift newL "Newly-rebalanced merged node"
        | Some nodeR' ->
            Expect.equal (Seq.append (nodeItems shift newL) (nodeItems shift nodeR') |> Array.ofSeq) expected "Order of items should not change during rebalance"
            Expect.equal newL.NodeSize Literals.blockSize "After rebalancing, if a right node exists then left node should be exactly M items long"
            checkProperties shift newL "Newly-rebalanced left node"
            checkProperties shift nodeR' "Newly-rebalanced right node"
        // TODO: Probably want more here
    )


let rebalanceTestsWIP =
  testList "WIP: Rebalance tests" [
    testProp (*541726758, 296574446*) (*1359582396, 296574428*) "Try this" <| fun (IsolatedNode nodeL : IsolatedNode<int>) (IsolatedNode nodeR : IsolatedNode<int>) ->
        doRebalance2Test Literals.blockSizeShift nodeL nodeR

    testProp "NeedsRebalancing function uses correct formula" <| fun (IsolatedNode nodeL : IsolatedNode<int>) (IsolatedNode nodeR : IsolatedNode<int>) ->
        let shift = Literals.blockSizeShift
        let slotCountL = if shift <= Literals.blockSizeShift then nodeL.TwigSlotCount else nodeL.SlotCount
        let slotCountR = if shift <= Literals.blockSizeShift then nodeR.TwigSlotCount else nodeR.SlotCount
        let totalSize = nodeL.NodeSize + nodeR.NodeSize
        let minSize = (slotCountL + slotCountR - 1) / Literals.blockSize + 1
        let needsRebalancing = totalSize - minSize > Literals.radixSearchErrorMax
        Expect.equal (nodeL.NeedsRebalance2 shift nodeR) needsRebalancing <| sprintf "NeedsRebalancing was wrong for left %A and right %A" nodeL nodeR

    testProp (*500188920, 296574447*) (*801697697, 296574440*) "Concat test" <| fun (IsolatedNode nodeL : IsolatedNode<int>) (IsolatedNode nodeR : IsolatedNode<int>) ->
        let shift = Literals.blockSizeShift
        // Need to do this before the concatenation, because after the concatenation the original nodeL may be invalid if it was an expanded node
        let expected = Seq.append (nodeItems shift nodeL) (nodeItems shift nodeR) |> Array.ofSeq
        let newL, newR = nodeL.ConcatNodes nullOwner shift nodeR
        match newR with
        | None ->
            Expect.isLessThanOrEqual newL.NodeSize Literals.blockSize "After concating, left node should be at most M items long"
            Expect.equal (nodeItems shift newL |> Array.ofSeq) expected "Order of items should not change during concatenate"
            checkProperties shift newL "Newly-concatenated merged node"
        | Some nodeR' ->
            Expect.equal (Seq.append (nodeItems shift newL) (nodeItems shift nodeR') |> Array.ofSeq) expected "Order of items should not change during concatenate"
            let totalOldSize = nodeL.NodeSize + nodeR.NodeSize
            let validNewSizes = if nodeL.NeedsRebalance2 shift nodeR then [totalOldSize - 2; totalOldSize - 1] else [totalOldSize]
            let totalNewSize = newL.NodeSize + nodeR'.NodeSize
            Expect.contains validNewSizes totalNewSize "After concating, the total size should either be the same, or go down by just one or two items"
            checkProperties shift newL "Newly-concatenated left node"
            checkProperties shift nodeR' "Newly-concatenated right node"

    testProp (*1489117831, 296575371*) "Concat-with-leaf test" <| fun (IsolatedNode nodeL : IsolatedNode<int>) (LeafNode leaf : LeafNode<int>) (IsolatedNode nodeR : IsolatedNode<int>) ->
        let shift = Literals.blockSizeShift
        // Need to do this before the concatenation, because after the concatenation the original nodeL may be invalid if it was an expanded node
        let expected = Seq.concat [nodeItems shift nodeL; leaf.Items |> Seq.ofArray; nodeItems shift nodeR] |> Array.ofSeq
        if nodeL.HasRoomToMergeTheTail shift leaf nodeR then
            let newL, newR = nodeL.ConcatTwigsPlusLeaf nullOwner shift leaf nodeR
            match newR with
            | None ->
                Expect.isLessThanOrEqual newL.NodeSize Literals.blockSize "After concating, left node should be at most M items long"
                Expect.equal (nodeItems shift newL |> Array.ofSeq) expected "Order of items should not change during concatenate"
                checkProperties shift newL "Newly-concatenated merged node"
            | Some nodeR' ->
                Expect.equal (Seq.append (nodeItems shift newL) (nodeItems shift nodeR') |> Array.ofSeq) expected "Order of items should not change during concatenate"
                let totalOldSize = nodeL.NodeSize + 1 + nodeR.NodeSize
                let validNewSizes = if nodeL.NeedsRebalance2PlusLeaf shift leaf.NodeSize nodeR then [totalOldSize - 2; totalOldSize - 1] else [totalOldSize]
                let totalNewSize = newL.NodeSize + nodeR'.NodeSize
                Expect.contains validNewSizes totalNewSize "After concating, the total size should either be the same, or go down by just one or two items"
                checkProperties shift newL "Newly-concatenated left node"
                checkProperties shift nodeR' "Newly-concatenated right node"
  ]

let doIndividualMergeTestLeftTwigRightTwoNodeTree L R1 R2 =
    let counter = mkCounter()
    let shift = Literals.blockSizeShift
    let nodeL =   L |> Array.map (mkLeaf counter >> fun node -> node :> RRBNode<int>) |> RRBNode<int>.MkNode nullOwner shift
    let nodeR1 = R1 |> Array.map (mkLeaf counter >> fun node -> node :> RRBNode<int>) |> RRBNode<int>.MkNode nullOwner shift
    let nodeR2 = R2 |> Array.map (mkLeaf counter >> fun node -> node :> RRBNode<int>) |> RRBNode<int>.MkNode nullOwner shift
    let nodeR = RRBNode<int>.MkNode nullOwner (shift * 2) [|nodeR1; nodeR2|]
    let origCombined = Seq.append (nodeItems shift nodeL) (nodeItems (shift * 2) nodeR) |> Array.ofSeq
    let newL, newR = (nodeL :?> RRBFullNode<int>).MergeTree nullOwner shift None (shift * 2) (nodeR :?> RRBFullNode<int>)
    let arrL' = newL |> nodeItems (shift * 2)
    match newR with
    | Some nodeR ->
        let arrR' = nodeR |> nodeItems (shift * 2)
        let arrCombined = Seq.append arrL' arrR' |> Array.ofSeq
        Expect.equal arrCombined origCombined "Order of items should not change during merge"
        checkProperties (shift * 2) newL "Newly merged node"
        checkProperties (shift * 2) nodeR "Newly merged node"
        let newRoot = (newL :?> RRBFullNode<int>).NewParent nullOwner (shift * 2) (Some nodeR)
        checkProperties (shift * 3) newRoot "Newly merged node"
    | None ->
        Expect.equal (arrL' |> Array.ofSeq) origCombined "Order of items should not change during merge"
        checkProperties (shift * 2) newL "Newly merged node"

let mergeTreeTestsWIP =
  testList "WIP: Rebalance tests" [
    testProp "Merging twigs" <| fun (IsolatedNode nodeL : IsolatedNode<int>) (IsolatedNode nodeR : IsolatedNode<int>) ->
        let shift = Literals.blockSizeShift
        let expected = Seq.concat [nodeItems shift nodeL; nodeItems shift nodeR] |> Array.ofSeq
        let newL, newR = nodeL.MergeTree nullOwner shift None shift nodeR
        match newR with
        | None ->
            Expect.isLessThanOrEqual newL.NodeSize Literals.blockSize "After merging, left node should be at most M items long"
            Expect.equal (nodeItems shift newL |> Array.ofSeq) expected "Order of items should not change during merge"
            checkProperties shift newL "Newly merged node"
        | Some nodeR' ->
            Expect.equal (Seq.append (nodeItems shift newL) (nodeItems shift nodeR') |> Array.ofSeq) expected "Order of items should not change during merge"
            let totalOldSize = nodeL.NodeSize + nodeR.NodeSize
            let validNewSizes = if nodeL.NeedsRebalance2 shift nodeR then [totalOldSize - 2; totalOldSize - 1] else [totalOldSize]
            let totalNewSize = newL.NodeSize + nodeR'.NodeSize
            Expect.contains validNewSizes totalNewSize "After merging, the total size should either be the same, or go down by just one or two items"
            checkProperties shift newL "Newly merged left node"
            checkProperties shift nodeR' "Newly merged right node"

    testProp (*1667443237, 296576485*) (*472714474, 296577783*) "Merging left twig with right tree" <| fun (IsolatedNode nodeL : IsolatedNode<int>) (RootNode nodeR : RootNode<int>) ->
        let shiftL = Literals.blockSizeShift
        let shiftR = Literals.blockSizeShift * (height nodeR)
        if shiftR > Literals.blockSizeShift then
            logger.debug (
                eventX "Suspect node: {node}"
                >> setField "node" (sprintf "%A" nodeR)
            )
        checkProperties shiftL nodeL "Original left node"
        checkProperties shiftR nodeR "Original right node"
        let expected = Seq.concat [nodeItems shiftL nodeL; nodeItems shiftR nodeR] |> Array.ofSeq
        let newL, newR = nodeL.MergeTree nullOwner shiftL None shiftR nodeR
        let newShift = max shiftL shiftR
        match newR with
        | None ->
            Expect.equal (nodeItems newShift newL |> Array.ofSeq) expected "Order of items should not change during merge"
            checkProperties newShift newL "Newly merged node"
        | Some nodeR' ->
            Expect.equal (Seq.append (nodeItems newShift newL) (nodeItems newShift nodeR') |> Array.ofSeq) expected "Order of items should not change during merge"
            let parent = (newL :?> RRBFullNode<int>).NewParent nullOwner newShift newR
            checkProperties (up newShift) parent "Newly rooted merged tree"
            // checkProperties newShift newL "Newly merged left node"
            // checkProperties newShift nodeR' "Newly merged right node"
        // Current failure has to do with a FullNode root with two children: left is a FullNode of length 24 (but not *totally* full since it's not length 32) and right is a relaxed node.
        // The top node, being a FullNode, is counting its TreeSize as (32 * full node of down shift), which isn't actually right. TODO: Consider whether newly-made parent should actually
        // be a full node (I think it shouldn't), and if not, how do we detect this scenario at node creation time?

        // TODO: Write some individual tests with the failures from src/Ficus/test-failures-2019-03-27.txt as a guideline.

    testCase "Left full twig, right height-2 tree with one fullish, relaxed node and final full node of size 4" <| fun _ ->
        let L = Array.replicate 32 32
        let R1 = [|32; 32; 31; 26; 32; 28; 18; 31; 32; 16; 32; 32; 32; 28; 32; 24; 32; 32; 32; 29; 25; 27; 32; 32; 32; 32; 32; 32; 32; 30; 32; 28|]
        let R2 = [|32; 32; 32; 32|]
        doIndividualMergeTestLeftTwigRightTwoNodeTree L R1 R2

    ftestCase "Left relaxed, halfish-full twig, right height-2 tree with one fullish, relaxed node and final relaxed node of size 5" <| fun _ ->
        let L = [|32; 27; 32; 29; 32; 30; 32; 22; 32; 16; 32; 25; 32; 27; 32; 28; 32; 32; 17; 32; 26; 32|]
        let R1 = [|32; 16; 32; 20; 32; 19; 32; 28; 32; 22; 19; 32; 21; 32; 22; 32; 24; 32; 25; 32; 17; 32; 28; 32; 20; 32; 22; 32; 23; 32; 32; 30|]
        let R2 = [|32; 31; 32; 16; 32|]
        doIndividualMergeTestLeftTwigRightTwoNodeTree L R1 R2

// TODO: Another test case to write -- (472714474, 296577783) after 50 tests:
// Failed after 50 tests. Parameters:
//  IsolatedNode
//   ExpandedRelaxedNode(length=21, sizetable=[|27; 53; 85; 117; 149; 181; 213; 235; 263; 295; 321; 353; 373; 405; 434; 466;
//   496; 528; 560; 592; 608; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0|], children=[|L27; L26; L32; L32; L32; L32; L32; L22; L28; L32; L26; L32; L20; L32; L29; L32;
//   L30; L32; L32; L32; L16; null; null; null; null; null; null; null; null; null;
//   null; null|])
//  RootNode
//   FullNode(length=15, children=[|L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L23|])
// Result:
//  Exception
//   Expecto.AssertException: Newly rooted merged tree with shift=10 and root=ExpandedRelaxedNode(length=2, sizetable=[|608; 471; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0;
//   0; 0; 0; 0; 0; 0; 0|], children=[|RelaxedNode(length=21, sizetable=[|27; 53; 85; 117; 149; 181; 213; 235; 263; 295; 321; 353; 373; 405; 434; 466;
//   496; 528; 560; 592; 608|], children=[|L27; L26; L32; L32; L32; L32; L32; L22; L28; L32; L26; L32; L20; L32; L29; L32;
//   L30; L32; L32; L32; L16|]);
//   ExpandedFullNode(length=15, children=[|L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L32; L23;
//   null; null; null; null; null; null; null; null; null; null; null; null; null;
//   null; null; null; null|]);
//   null; null; null; null; null; null; null; null; null; null; null; null; null;
//   null; null; null; null; null; null; null; null; null; null; null; null; null;
//   null; null; null; null|])
// failed the following RRBVector invariants:
// ["The tree size of any node should match the total number of items its descendent leaves contain";
//  "The size table of any tree node should match the cumulative tree sizes of its children"]

  ]

// logger.debug (
//     eventX "Result: {node}"
//     >> setField "node" (sprintf "%A" result)
// )

let tests =
  testList "Basic node tests" [
    mergeTreeTestsWIP
    rebalanceTestsWIP
    appendAndPrependChildrenPropertyTests  // Put this first since it's so long
    appendPropertyTests
    insertPropertyTests
    removePropertyTests
    updatePropertyTests
    keepPropertyTests
    splitAndKeepPropertyTests
  ]
