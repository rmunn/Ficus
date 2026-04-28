module Ficus.Tests.RRBVectorNodesExpectoTest

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
// open Ficus.RRBVectorNodes
open RRBVectorProps
open FsCheck
open Ficus

// TODO: Look for "testProp" and make sure all those tests are being discovered and run
// We want to make sure that stress testing can stress-test *all* the properties, not just a subset

// TODO: This file and RRBVectorExpectoTest.fs are getting huge. Split them up into subcategories.
// Move the common code below into TestCommon.fs and place that first (or nearly first) in the .fsproj
// Then each test list can get its own file
// Also, property tests all should have "FsCheck" in the name of the test list, for easy filtering
// Then we can replace the complicated filter code in Main.fs with a simple "--filter-test-list FsCheck" for more effective stress testing
// (allowing us to run more FsCheck tests for longer, and also create something similar to stress.sh but that runs each test up to 1,000 times instead of 100)

// All of that will make it easier to go for advanced testing scenarios. It starts with dividing the tests into better categories and giving them better names

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

let nullOwner = OwnerTokens.NullOwner

let children (node: RRBNode<'T>) =
    (node :?> RRBFullNode<'T>).Children
    |> Seq.truncate node.NodeSize

let isEmpty (node: RRBNode<'T>) =
    node.NodeSize
    <= 0
// Note: do NOT call isNotTwig or isTwig on empty nodes!
let isLeaf (node: RRBNode<'T>) = node :? RRBLeafNode<'T>
let isNode (node: RRBNode<'T>) = not (isLeaf node)
let isRelaxed (node: RRBNode<'T>) = node :? RRBRelaxedNode<'T>

let isFull (node: RRBNode<'T>) =
    isNode node
    && not (isRelaxed node)

let isTwig (node: RRBNode<'T>) =
    isNode node
    && children node
       |> Seq.forall isLeaf

let isNotTwig (node: RRBNode<'T>) = not (isTwig node)

let rec itemCount shift (node: RRBNode<'T>) =
    if shift <= 0 then
        node.NodeSize
    else
        children node
        |> Seq.sumBy (itemCount (down shift))

let rec height (node: RRBNode<'T>) =
    if
        isEmpty node
        || isLeaf node
    then
        0
    else
        1
        + height (node :?> RRBFullNode<'T>).FirstChild


// === Generators ===

let rec digitValues =
    function
    | n when n < 0 -> digitValues (abs n)
    | n when n < 10 -> n
    | n ->
        (n % 10)
        + digitValues (n / 10)

let clampSize maxSize size =
    // Ensure size remains between 1 and 100, skewing towards larger values for numbers greater than 100 but still ensuring small numbers can sometimes show up
    //
    // First take size % 100, which will be in the range 0-99. Now add the *value of the digits* of size, which will always be at least 1, so the range is 1-(unbounded, but around 130ish when max size is 10,000)
    // Now clamp that to a 1-100 range, so that anything 100 or above gets hard-set to 100
    // Now use that size in generation code that expects a 1-100 result, such as tree-generation code
    let rem = size % 100

    min
        maxSize
        (rem
         + digitValues size)

let mkCounter () =
    let mutable i = 0

    fun () ->
        i <- i + 1
        i

let mkSimpleArr n = [| 1..n |]
let mkArr counter n = Array.init n (fun _ -> counter ())
let mkSimpleLeaf n = RRBLeafNode(nullOwner, mkSimpleArr n)
let mkLeaf counter n = RRBLeafNode(nullOwner, mkArr counter n)
let fullSimpleLeaf = mkSimpleLeaf Literals.blockSize
let fullLeaf counter () = mkLeaf counter Literals.blockSize

type NodeKind =
    | Full
    | Relaxed
    | ExpandedFull
    | ExpandedRelaxed

let nodeKind (node: RRBNode<'T>) =
    match node with
    | :? RRBExpandedRelaxedNode<'T> -> ExpandedRelaxed
    | :? RRBExpandedFullNode<'T> -> ExpandedFull
    | :? RRBRelaxedNode<'T> -> Relaxed
    | _ -> Full

let genSimpleLeaf =
    Gen.oneof [
        Gen.constant Literals.blockSize
        Gen.choose (
            Literals.blockSize
            / 2,
            Literals.blockSize
        )
    ]
    |> Gen.map mkSimpleLeaf

let genLeaf counter =
    Gen.oneof [
        Gen.constant Literals.blockSize
        Gen.choose (
            Literals.blockSize
            / 2,
            Literals.blockSize
        )
    ]
    |> Gen.map (mkLeaf counter)

let genLeaves =
    let counter = mkCounter ()

    Gen.nonEmptyListOf (genLeaf counter)
    |> Gen.map Array.ofList

let genFullLeaves =
    let counter = mkCounter ()

    Gen.nonEmptyListOf (Gen.fresh (fullLeaf counter))
    |> Gen.map Array.ofList

let genFullLeavesExceptLast =
    let allButLast =
        Gen.sized
        <| fun s ->
            Gen.resize
                (s - 1
                 |> max 0)
                genFullLeaves

    let lastLeaf =
        genLeaf (mkCounter ())
        |> Gen.map Array.singleton

    Gen.zip allButLast lastLeaf
    |> Gen.map (fun (a1, a2) -> Array.append a1 a2)

let genLeavesForOneFullNode n =
    Gen.oneof [
        genFullLeavesExceptLast
        genFullLeaves
    ]
    |> Gen.map (fun leaves ->
        leaves
        |> Array.truncate n
        |> Array.map (fun l -> l :> RRBNode<int>)
    )

let genLeavesForOneRelaxedNode n =
    genLeaves
    |> Gen.map (fun leaves ->
        leaves
        |> Array.truncate n
        |> Array.map (fun l -> l :> RRBNode<int>)
    )

let genLeavesForMultipleFullNodes =
    Gen.oneof [
        genFullLeavesExceptLast
        genFullLeaves
    ]
    |> Gen.map (fun leaves ->
        leaves
        |> Array.chunkBySize Literals.blockSize
        |> Array.map (Array.map (fun l -> l :> RRBNode<int>))
    )

let genLeavesForMultipleRelaxedNodes =
    genLeaves
    |> Gen.map (fun leaves ->
        leaves
        |> Array.chunkBySize Literals.blockSize
        |> Array.map (Array.map (fun l -> l :> RRBNode<int>))
    )

let mkFullTwig children =
    RRBNode<int>.MkFullNode(nullOwner, children)

let mkExpandedFullTwig children =
    RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>

let mkRelaxedTwig children =
    RRBNode<int>.MkNode(nullOwner, Literals.shiftSize, children)

let mkExpandedRelaxedTwig children =
    let sizeTable = RRBNode<int>.CreateSizeTable(Literals.shiftSize, children)

    if RRBMath.IsSizeTableFullAtShift(Literals.shiftSize, sizeTable, sizeTable.Length) then
        RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>
    else
        RRBExpandedRelaxedNode<int>(nullOwner, children, sizeTable) :> RRBNode<int>

let mkFullNode shift children =
    RRBNode<int>.MkFullNode(nullOwner, children)

let mkExpandedFullNode shift children =
    RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>

let mkRelaxedNode shift children =
    RRBNode<int>.MkNode(nullOwner, shift, children)

let mkExpandedRelaxedNode shift children =
    let sizeTable = RRBNode<int>.CreateSizeTable(shift, children)

    if RRBMath.IsSizeTableFullAtShift(shift, sizeTable, sizeTable.Length) then
        RRBExpandedFullNode<int>(nullOwner, children) :> RRBNode<int>
    else
        RRBExpandedRelaxedNode<int>(nullOwner, children, sizeTable) :> RRBNode<int>

let genFullNode n =
    genLeavesForOneFullNode n
    |> Gen.map mkFullTwig

let genExpandedFullNode n =
    genLeavesForOneFullNode n
    |> Gen.map mkExpandedFullTwig

let genRelaxedNode n =
    genLeavesForOneRelaxedNode n
    |> Gen.map mkRelaxedTwig

let genExpandedRelaxedNode n =
    genLeavesForOneRelaxedNode n
    |> Gen.map mkExpandedRelaxedTwig

let genNode =
    Gen.oneof [
        genFullNode Literals.blockSize
        genExpandedFullNode Literals.blockSize
        genRelaxedNode Literals.blockSize
        genExpandedRelaxedNode Literals.blockSize
    ]

let genShortNode =
    Gen.oneof [
        genFullNode (
            Literals.blockSize
            - 1
        )
        genExpandedFullNode (
            Literals.blockSize
            - 1
        )
        genRelaxedNode (
            Literals.blockSize
            - 1
        )
        genExpandedRelaxedNode (
            Literals.blockSize
            - 1
        )
    ]

let genSmallFullTree =
    genLeavesForMultipleFullNodes
    |> Gen.map (fun leafChunks ->
        leafChunks
        |> Array.truncate Literals.blockSize
        |> Array.map mkFullTwig
    )
    |> Gen.map (fun nodes ->
        if nodes.Length = 1 then
            Array.exactlyOne nodes
        else
            mkFullNode
                (2
                 * Literals.shiftSize)
                nodes
    )

let genSmallRelaxedTree =
    genLeavesForMultipleRelaxedNodes
    |> Gen.map (fun leafChunks ->
        leafChunks
        |> Array.truncate Literals.blockSize
        |> Array.map mkRelaxedTwig
    )
    |> Gen.map (fun nodes ->
        if nodes.Length = 1 then
            Array.exactlyOne nodes
        else
            mkRelaxedNode
                (2
                 * Literals.shiftSize)
                nodes
    )

let toTransient (root: RRBNode<'T>) =
    let newToken = OwnerTokens.MkOwnerToken()

    let rec expandNode token (root: RRBNode<'T>) =
        if root :? RRBLeafNode<'T> then
            (root.GetEditableNode token), 0
        else
            let child, childShift =
                (root :?> RRBFullNode<'T>).LastChild
                |> expandNode token

            let thisShift = up childShift

            let root' =
                ((root.Expand token) :?> RRBFullNode<'T>)
                    .UpdateChild(
                        token,
                        thisShift,
                        (root.NodeSize
                         - 1),
                        child
                    )

            root', thisShift

    expandNode newToken root
    |> fst

let toPersistent (root: RRBNode<'T>) =
    let rec shrinkNode token (root: RRBNode<'T>) =
        if root :? RRBLeafNode<'T> then
            (root.Shrink token), 0
        else
            let child, childShift =
                (root :?> RRBFullNode<'T>).LastChild
                |> shrinkNode token

            let thisShift = up childShift

            let root' =
                ((root.Shrink token) :?> RRBFullNode<'T>)
                    .UpdateChild(
                        token,
                        thisShift,
                        (root.NodeSize
                         - 1),
                        child
                    )

            root', thisShift

    shrinkNode nullOwner root
    |> fst

let genTransientSmallFullTree =
    genSmallFullTree
    |> Gen.map toTransient

let genTransientSmallRelaxedTree =
    genSmallRelaxedTree
    |> Gen.map toTransient

let genSmallTree =
    Gen.oneof [
        genSmallFullTree
        genTransientSmallFullTree
        genSmallRelaxedTree
        genTransientSmallRelaxedTree
    ]
//   |> Gen.map (fun node ->
//     logger.debug (
//         eventX "Generated node: {node}"
//         >> setField "node" (sprintf "%A" node)
//     )
//     node)

let genShowSizedInt =
    Gen.sized (fun s ->
        logger.debug (
            eventX "Generating item of size {n}"
            >> setField "n" s
        )

        Gen.choose (0, s)
    )

let genLeavesForLargeTrees n counter =
    Gen.listOfLength n (genLeaf counter)
    |> Gen.map (
        Seq.cast<RRBNode<int>>
        >> Array.ofSeq
    )

let genTwigForLargeTrees counter =
    gen {
        let! isFull =
            Gen.frequency [
                1, Gen.constant true
                3, Gen.constant false
            ]

        let! leafCount =
            if isFull then
                Gen.constant Literals.blockSize
            else
                Gen.choose (
                    Literals.blockSize
                    / 2,
                    Literals.blockSize
                )

        let! leaves = genLeavesForLargeTrees leafCount counter
        return RRBNode<int>.MkNode(nullOwner, Literals.shiftSize, leaves)
    }

let genBranchForLargeTrees counter =
    gen {
        let! isFull =
            Gen.frequency [
                1, Gen.constant true
                3, Gen.constant false
            ]

        let! twigCount =
            if isFull then
                Gen.constant Literals.blockSize
            else
                Gen.choose (
                    Literals.blockSize
                    / 4,
                    Literals.blockSize
                )

        let! twigs = Gen.listOfLength twigCount (genTwigForLargeTrees counter)

        return
            RRBNode<int>
                .MkNode(
                    nullOwner,
                    (Literals.shiftSize
                     * 2),
                    (Array.ofList twigs)
                )
    }

let genLimbForLargeTrees counter =
    gen {
        let! isFull =
            Gen.frequency [
                1, Gen.constant true
                3, Gen.constant false
            ]

        let! branchCount =
            if isFull then
                Gen.constant Literals.blockSize
            else
                Gen.choose (
                    Literals.blockSize
                    / 8
                    |> max 1,
                    Literals.blockSize
                )

        let! branches = Gen.listOfLength branchCount (genBranchForLargeTrees counter)

        return
            RRBNode<int>
                .MkNode(
                    nullOwner,
                    (Literals.shiftSize
                     * 3),
                    (Array.ofList branches)
                )
    }

let genTrunkForLargeTrees counter =
    gen {
        let! isFull =
            Gen.frequency [
                1, Gen.constant true
                3, Gen.constant false
            ]

        let! limbCount =
            if isFull then
                Gen.constant Literals.blockSize
            else
                Gen.choose (
                    Literals.blockSize
                    / 16
                    |> max 1,
                    Literals.blockSize
                )

        let! limbs = Gen.listOfLength limbCount (genLimbForLargeTrees counter)

        return
            RRBNode<int>
                .MkNode(
                    nullOwner,
                    (Literals.shiftSize
                     * 4),
                    (Array.ofList limbs)
                )
    }

let genMediumPersistentTree =
    // Size rules for medium trees are:
    // 1-32: number of leaves
    // 33-64: subtract 32 -> 1-32 twigs (but since it's Gen.choose(1,x), that's anywhere between 1-2 to 1-32 twigs)
    // 65-96: subtract 64 -> 1-32 branches (height 2)
    // 97-128: subtract 96 -> 1-4 limbs (height 3) for size up to 100 (non-stress), 1-32 limbs for size up to 128
    // 129-132: subtract 128 -> 1-4 trunks (height 4), can only happen in stress testing
    Gen.sized (fun s ->
        let counter = mkCounter ()

        let s = clampSize 132 s

        logger.debug (
            eventX "Generating tree of size {n}"
            >> setField "n" s
        )

        if s <= 32 then
            let leafMax = s

            gen {
                let! n = Gen.choose (1, leafMax)
                let! leaves = genLeavesForLargeTrees n counter
                return mkRelaxedTwig leaves
            }
        elif s <= 64 then
            let twigMax = s - 32

            gen {
                let! n = Gen.choose (1, twigMax)

                return!
                    Gen.listOfLength n (genTwigForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 2),
                                    arr
                                )
                        )
                    )
            }
        elif s <= 96 then
            let branchMax = s - 64

            gen {
                let! n = Gen.choose (1, branchMax)

                return!
                    Gen.listOfLength n (genBranchForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 3),
                                    arr
                                )
                        )
                    )
            }
        elif s <= 128 then
            let limbMax = s - 96

            gen {
                let! n = Gen.choose (1, limbMax)

                return!
                    Gen.listOfLength n (genLimbForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 4),
                                    arr
                                )
                        )
                    )
            }
        else
            let trunkMax = s - 128

            gen {
                let! n = Gen.choose (1, trunkMax)

                return!
                    Gen.listOfLength n (genLimbForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 5),
                                    arr
                                )
                        )
                    )
            }
    )

let genLargePersistentTree =
    // Size rules for large trees are:
    // 1-8: multiply by 4 -> number of leaves
    // 9-24: subtract 8 -> 1-16, then multiply by 2 -> 2-32 twigs (but since it's Gen.choose(1,x), that's anywhere between 1-2 to 1-32 twigs)
    // 25-56: subtract 24 -> 1-32 branches (height 2)
    // 57-88: subtract 56 -> 1-32 limbs (height 3)
    // 89-100: subtract 88 -> 1-12 trunks (height 4)
    Gen.sized (fun s ->
        let counter = mkCounter ()

        logger.debug (
            eventX "Generating tree of size {n}"
            >> setField "n" s
        )

        let s = clampSize 100 s

        if s <= 8 then
            let leafMax = s * 4 // Up to 32

            gen {
                let! n = Gen.choose (1, leafMax)
                let! leaves = genLeavesForLargeTrees n counter
                return mkRelaxedTwig leaves
            }
        elif s <= 24 then
            let twigMax =
                (s - 8)
                * 2

            gen {
                let! n = Gen.choose (1, twigMax)

                return!
                    Gen.listOfLength n (genTwigForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 2),
                                    arr
                                )
                        )
                    )
            }
        elif s <= 56 then
            let branchMax = s - 24

            gen {
                let! n = Gen.choose (1, branchMax)

                return!
                    Gen.listOfLength n (genBranchForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 3),
                                    arr
                                )
                        )
                    )
            }
        elif s <= 88 then
            let limbMax = s - 56

            gen {
                let! n = Gen.choose (1, limbMax)

                return!
                    Gen.listOfLength n (genLimbForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 4),
                                    arr
                                )
                        )
                    )
            }
        else
            let trunkMax = s - 88

            gen {
                let! n = Gen.choose (1, trunkMax)

                return!
                    Gen.listOfLength n (genTrunkForLargeTrees counter)
                    |> Gen.map (
                        Array.ofList
                        >> (fun arr ->
                            RRBNode<int>
                                .MkNode(
                                    nullOwner,
                                    (Literals.shiftSize
                                     * 5),
                                    arr
                                )
                        )
                    )
            }
    )

let genMediumTransientTree =
    genMediumPersistentTree
    |> Gen.map toTransient

let genLargeTransientTree =
    genLargePersistentTree
    |> Gen.map toTransient

let genMediumTree =
    Gen.oneof [
        genMediumPersistentTree
        genMediumTransientTree
    ]

let genLargeTree =
    Gen.oneof [
        genLargePersistentTree
        genLargeTransientTree
    ]

let genMediumOrLargeTree =
    Gen.oneof [
        genMediumTree
        genLargeTree
    ]

type IsolatedNode<'T> = IsolatedNode of RRBFullNode<'T>
type IsolatedShortNode<'T> = IsolatedShortNode of RRBFullNode<'T>
type RootNode<'T> = RootNode of RRBFullNode<'T>
type LeafNode<'T> = LeafNode of RRBLeafNode<'T>
type LargeRootNode<'T> = LargeRootNode of RRBFullNode<'T> // TODO: Use toTransient() to sometimes make transient-ish trees here
type ShowSizedInt = ShowSizedInt of int
// TODO: Write shrinkers for nodes and for trees
// TODO: REALLY REALLY TODO. Shrinkers are what I need most now, so that my regression tests can be as small as possible.

let leafWithout (n: RRBLeafNode<'T>) i =
    let newItems = RRBArrayExtensions.RRBArrayExtensions.CopyAndRemoveAt(n.Items, i)
    RRBLeafNode<'T>(n.Owner, newItems)

let shrinkLeaf (n: RRBLeafNode<'T>) =
    let size = n.NodeSize

    if size < 2 then
        Seq.empty
    else
        seq { 0 .. size - 1 }
        |> Seq.map (leafWithout n)

let shrinkLeaves (leaves: RRBLeafNode<'T>[]) =
    if leaves.Length < 2 then
        Seq.empty
    else
        seq {
            0 .. leaves.Length
                 - 1
        }
        |> Seq.map (fun i -> RRBArrayExtensions.RRBArrayExtensions.CopyAndRemoveAt(leaves, i))

let nodeWithout (n: RRBFullNode<'T>) i =
    let shift = height n
    n.RemoveChild(n.Owner, shift, i) :?> RRBFullNode<_>

let rec shrinkRRBNode (n: RRBFullNode<'T>) =
    let size = n.NodeSize

    if size < 2 then
        // Short nodes shouldn't be shrunk further
        Seq.empty
    elif
        n.Owner
        <> nullOwner
    then
        // Transient nodes update themselves in place, which is not compatible with the shrink logic that FsCheck uses (which assumes immutable data)
        // TODO: See if we can find some way to make this safe for transient nodes as well, perhaps by cloning the node before calling UpdateChild so that each shrink is using a different node after all, yet the "new" node has the same owner
        // But for now, we just don't shink transients
        Seq.empty
    else
        let shift = height n

        // For tall trees (above twig level), we can first try to shorten the *entire* tree, by grabbing one child at a time to be the new root
        let shortenTree =
            if
                shift
                <= Literals.shiftSize
            then
                // Twig nodes are the shortest root allowed, not able to shorten tree any further
                Seq.empty
            else
                n.ChildrenSeq
                |> Seq.cast<RRBFullNode<_>>
        // That's really it! Each child, in turn, is used as the new root of a tree, to shrink down to smaller counterexamples

        let removeChildren =
            seq { 0 .. size - 1 }
            |> Seq.map (nodeWithout n)

        let shrinkChildren =
            let shift = height n

            if shift < Literals.shiftSize then
                Seq.empty
            elif shift = Literals.shiftSize then
                // Twig level, children are leaves
                n.ChildrenSeq
                |> Seq.cast<RRBLeafNode<'T>>
                |> Seq.mapi (fun idx leaf -> (idx, leaf))
                |> Seq.collect (fun (idx, leaf) ->
                    let leafShrinks = shrinkLeaf leaf

                    leafShrinks
                    |> Seq.map (fun leaf' ->
                        n.UpdateChild(n.Owner, shift, idx, leaf') :?> RRBFullNode<_>
                    )
                )
            else
                // Above twig level, children are nodes
                n.ChildrenSeq
                |> Seq.mapi (fun idx child -> (idx, child))
                |> Seq.collect (fun (idx, child) ->
                    let childShrinks = shrinkRRBNode (child :?> RRBFullNode<_>)

                    childShrinks
                    |> Seq.map (fun child' ->
                        n.UpdateChild(n.Owner, shift, idx, child') :?> RRBFullNode<_>
                    )
                )

        shortenTree
        |> Seq.append removeChildren
        |> Seq.append shrinkChildren

let shrinkRootNode (RootNode n) = shrinkRRBNode n

let shrinkLeafNode (LeafNode n) = shrinkLeaf n

let shrinkLargeRootNode (LargeRootNode n) = shrinkRRBNode n

let shrinkIsolatedNode (IsolatedNode n) = shrinkRRBNode n

let shrinkIsolatedShortNode (IsolatedShortNode n) = shrinkRRBNode n

type MyGenerators =
    static member arbTree() =
        { new Arbitrary<RootNode<int>>() with
            override x.Generator =
                genSmallTree
                |> Gen.map (fun node -> RootNode(node :?> RRBFullNode<int>))

            override x.Shrinker n =
                shrinkRootNode n
                |> Seq.map RootNode
        }

    static member arbLargeTree() =
        { new Arbitrary<LargeRootNode<int>>() with
            override x.Generator =
                genMediumOrLargeTree
                |> Gen.map (fun node -> LargeRootNode(node :?> RRBFullNode<int>))

            override x.Shrinker n =
                shrinkLargeRootNode n
                |> Seq.map LargeRootNode
        }

    static member arbLeaf() =
        { new Arbitrary<LeafNode<int>>() with
            override x.Generator =
                let counter = mkCounter () in

                genLeaf counter
                |> Gen.map LeafNode

            override x.Shrinker n =
                shrinkLeafNode n
                |> Seq.map LeafNode
        }

    static member arbLeaves() =
        { new Arbitrary<RRBLeafNode<int>[]>() with
            override x.Generator = genLeaves
            override x.Shrinker leaves = shrinkLeaves leaves
        }

    static member arbNode() =
        { new Arbitrary<IsolatedNode<int>>() with
            override x.Generator =
                genNode
                |> Gen.map (fun node -> IsolatedNode(node :?> RRBFullNode<int>))

            override x.Shrinker n =
                shrinkIsolatedNode n
                |> Seq.map IsolatedNode
        }

    static member arbShortNode() =
        { new Arbitrary<IsolatedShortNode<int>>() with
            override x.Generator =
                genShortNode
                |> Gen.map (fun node -> IsolatedShortNode(node :?> RRBFullNode<int>))

            override x.Shrinker n =
                shrinkIsolatedShortNode n
                |> Seq.map IsolatedShortNode
        }

// Now we can write test properties that take an IsolatedNode<int> or a RootNode<int>

Arb.register<MyGenerators> ()
|> ignore

let testProp name fn =
    testPropertyWithConfig
        {
            FsCheckConfig.defaultConfig with
                arbitrary = [ typeof<MyGenerators> ]
        }
        name
        fn

let ptestProp name fn =
    ptestPropertyWithConfig
        {
            FsCheckConfig.defaultConfig with
                arbitrary = [ typeof<MyGenerators> ]
        }
        name
        fn

let ftestProp name fn =
    ftestPropertyWithConfig
        {
            FsCheckConfig.defaultConfig with
                arbitrary = [ typeof<MyGenerators> ]
        }
        name
        fn

let etestProp replay name fn =
    etestPropertyWithConfig
        replay
        {
            FsCheckConfig.defaultConfig with
                arbitrary = [ typeof<MyGenerators> ]
        }
        name
        fn

// === Tests here ===


let mkManualNodeA (leafSizes: int[]) =
    let counter = mkCounter ()

    leafSizes
    |> Array.truncate Literals.blockSize
    |> Array.map (
        mkLeaf counter
        >> fun leaf -> leaf :> RRBNode<int>
    )
    |> mkRelaxedTwig

let mkManualNode (leafSizes: int list) =
    leafSizes
    |> Array.ofList
    |> mkManualNodeA

[<Literal>]
let M = Literals.blockSize // Shorthand


// TODO: One test to write: inserting at the end (at index vec.Length) means the same as appending

type ExpectedResult =
    | Full
    | Relaxed

let inputDataForAppendTests: (int list * int * ExpectedResult * string) list = [ // Initial leaves, size of inserted node, expected fullness of result, partial name
    [ M - 2 ], M - 2, Relaxed, "singleton"
    [ M - 2 ], M, Relaxed, "singleton"
    [ M ], M - 2, Full, "singleton"
    [ M ], M, Full, "singleton"
    [
        M
        M - 2
    ],
    M - 2,
    Relaxed,
    "two-element"
    [
        M
        M - 2
    ],
    M,
    Relaxed,
    "two-element"
    [
        M
        M
    ],
    M - 2,
    Full,
    "two-element"
    [
        M
        M
    ],
    M,
    Full,
    "two-element"
    List.replicate (M - 2) M, M - 2, Full, "size M-2"
    List.replicate (M - 2) M, M, Full, "size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    M - 2,
    Relaxed,
    "size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    M,
    Relaxed,
    "size M-2"
    List.replicate (M - 1) M, M - 2, Full, "size M-1"
    List.replicate (M - 1) M, M, Full, "size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    M - 2,
    Relaxed,
    "size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    M,
    Relaxed,
    "size M-1"
]

let mkAppendTests (leafSizes, newLeafSize, expectedResult, namePart) =
    let counter = mkCounter ()

    let nodeDesc =
        match expectedResult with
        | Full -> "completely full"
        | Relaxed -> "nearly-full"

    let leafDesc =
        if newLeafSize = Literals.blockSize then
            "full"
        else
            "non-full"

    let isWhat =
        match expectedResult with
        | Full -> isFull
        | Relaxed -> isRelaxed

    let isWhatStr =
        match expectedResult with
        | Full -> "full"
        | Relaxed -> "relaxed"

    [
        "AppendChild"
        "AppendChildS"
    ]
    |> List.map (fun fname ->
        let test =
            fun _ ->
                let node = mkManualNode leafSizes :?> RRBFullNode<int>
                checkNodeProperties Literals.shiftSize node "Starting node"
                let newChild = mkLeaf counter newLeafSize

                let result =
                    if fname = "AppendChild" then
                        node.AppendChild(nullOwner, Literals.shiftSize, newChild)
                    elif fname = "AppendChildS" then
                        node.AppendChildS(nullOwner, Literals.shiftSize, newChild, newLeafSize)
                    else
                        failwith
                        <| sprintf "Unknown method name %s in test creation - fix unit tests" fname

                checkNodeProperties Literals.shiftSize result "Result"

                Expect.equal
                    (result.NodeSize)
                    (node.NodeSize
                     + 1)
                    "Appending any leaf to a node should increase its node size by 1"

                Expect.equal
                    (result.TreeSize Literals.shiftSize)
                    (node.TreeSize Literals.shiftSize
                     + newLeafSize)
                    "Appending any leaf to a node should increase its tree size by the leaf's NodeSize"

                Expect.isTrue
                    (isWhat result)
                    (sprintf
                        "Appending any leaf to a %s node should result in a %s node"
                        nodeDesc
                        isWhatStr)

        let name =
            sprintf "%s on a %s %s node with a %s leaf" fname nodeDesc namePart leafDesc

        testCase name test
    )

let appendTests =
    inputDataForAppendTests
    |> List.collect mkAppendTests
    |> testList "Append tests"

let appendPropertyTests =
    testList "Append property tests" [
        testProp "FsCheck AppendChild on a generated node"
        <| fun (IsolatedShortNode node) ->
            checkNodeProperties Literals.shiftSize node "Starting node"
            let newChild = mkSimpleLeaf (M - 2)
            let result = node.AppendChild(nullOwner, Literals.shiftSize, newChild)
            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
                              + 1

        testProp "FsCheck AppendChildS on a generated node"
        <| fun (IsolatedShortNode node) ->
            checkNodeProperties Literals.shiftSize node "Starting node"
            let newChild = mkSimpleLeaf (M - 2)
            let result = node.AppendChildS(nullOwner, Literals.shiftSize, newChild, (M - 2))
            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
                              + 1
    ]


let inputDataForInsertTests: (int list * int * int * ExpectedResult * string) list = [ // Initial leaves, insert position, size of inserted node, expected fullness of result, partial name
    [ M - 2 ], 0, M - 2, Relaxed, "nearly-full singleton"
    [ M - 2 ], 1, M - 2, Relaxed, "nearly-full singleton"
    [ M - 2 ], 0, M, Full, "nearly-full singleton"
    [ M - 2 ], 1, M, Relaxed, "nearly-full singleton"
    [ M ], 0, M - 2, Relaxed, "completely full singleton"
    [ M ], 1, M - 2, Full, "completely full singleton"
    [ M ], 0, M, Full, "completely full singleton"
    [ M ], 1, M, Full, "completely full singleton"
    [
        M
        M - 2
    ],
    0,
    M - 2,
    Relaxed,
    "nearly-full two-element"
    [
        M
        M - 2
    ],
    1,
    M - 2,
    Relaxed,
    "nearly-full two-element"
    [
        M
        M - 2
    ],
    2,
    M - 2,
    Relaxed,
    "nearly-full two-element"
    [
        M
        M - 2
    ],
    0,
    M,
    Full,
    "nearly-full two-element"
    [
        M
        M - 2
    ],
    1,
    M,
    Full,
    "nearly-full two-element"
    [
        M
        M - 2
    ],
    2,
    M,
    Relaxed,
    "nearly-full two-element"
    [
        M
        M
    ],
    0,
    M - 2,
    Relaxed,
    "completely full two-element"
    [
        M
        M
    ],
    1,
    M - 2,
    Relaxed,
    "completely full two-element"
    [
        M
        M
    ],
    2,
    M - 2,
    Full,
    "completely full two-element"
    [
        M
        M
    ],
    0,
    M,
    Full,
    "completely full two-element"
    [
        M
        M
    ],
    1,
    M,
    Full,
    "completely full two-element"
    [
        M
        M
    ],
    2,
    M,
    Full,
    "completely full two-element"
    List.replicate (M - 2) M, 0, M - 2, Relaxed, "completely full size M-2"
    List.replicate (M - 2) M, M / 2, M - 2, Relaxed, "completely full size M-2"
    List.replicate (M - 2) M, M - 2, M - 2, Full, "completely full size M-2"
    List.replicate (M - 2) M, 0, M, Full, "completely full size M-2"
    List.replicate (M - 2) M, M / 2, M, Full, "completely full size M-2"
    List.replicate (M - 2) M, M - 2, M, Full, "completely full size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    0,
    M - 2,
    Relaxed,
    "nearly-full size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    M / 2,
    M - 2,
    Relaxed,
    "nearly-full size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    M - 2,
    M - 2,
    Relaxed,
    "nearly-full size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    0,
    M,
    Full,
    "nearly-full size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    M / 2,
    M,
    Full,
    "nearly-full size M-2"
    List.replicate (M - 3) M
    @ [ M - 1 ],
    M - 2,
    M,
    Relaxed,
    "nearly-full size M-2"
    List.replicate (M - 1) M, 0, M - 2, Relaxed, "completely full size M-1"
    List.replicate (M - 1) M, M / 2, M - 2, Relaxed, "completely full size M-1"
    List.replicate (M - 1) M, M - 1, M - 2, Full, "completely full size M-1"
    List.replicate (M - 1) M, 0, M, Full, "completely full size M-1"
    List.replicate (M - 1) M, M / 2, M, Full, "completely full size M-1"
    List.replicate (M - 1) M, M - 1, M, Full, "completely full size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    0,
    M - 2,
    Relaxed,
    "nearly-full size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    M / 2,
    M - 2,
    Relaxed,
    "nearly-full size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    M - 1,
    M - 2,
    Relaxed,
    "nearly-full size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    0,
    M,
    Full,
    "nearly-full size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    M / 2,
    M,
    Full,
    "nearly-full size M-1"
    List.replicate (M - 2) M
    @ [ M - 1 ],
    M - 1,
    M,
    Relaxed,
    "nearly-full size M-1"
]

let mkInsertTests (leafSizes, insertPos, newLeafSize, expectedResult, namePart) =
    // let nodeDesc = match expectedResult with Full -> "completely full" | Relaxed -> "nearly-full"
    let leafDesc =
        if newLeafSize = Literals.blockSize then
            "full"
        else
            "non-full"

    let isWhat =
        match expectedResult with
        | Full -> isFull
        | Relaxed -> isRelaxed

    let isWhatStr =
        match expectedResult with
        | Full -> "full"
        | Relaxed -> "relaxed"

    [
        "InsertChild"
        "InsertChildS"
    ]
    |> List.map (fun fname ->
        let test =
            fun _ ->
                let node = mkManualNode leafSizes :?> RRBFullNode<int>
                checkNodeProperties Literals.shiftSize node "Starting node"
                let newChild = mkSimpleLeaf newLeafSize

                let result =
                    if fname = "InsertChild" then
                        node.InsertChild(nullOwner, Literals.shiftSize, insertPos, newChild)
                    elif fname = "InsertChildS" then
                        node.InsertChildS(
                            nullOwner,
                            Literals.shiftSize,
                            insertPos,
                            newChild,
                            newLeafSize
                        )
                    else
                        failwith
                        <| sprintf "Unknown method name %s in test creation - fix unit tests" fname

                checkNodeProperties Literals.shiftSize result "Result"

                Expect.equal
                    (result.NodeSize)
                    (node.NodeSize
                     + 1)
                    "Inserting any leaf into a node should increase its node size by 1"

                Expect.equal
                    (result.TreeSize Literals.shiftSize)
                    (node.TreeSize Literals.shiftSize
                     + newLeafSize)
                    "Inserting any leaf into a node should increase its tree size by the leaf's NodeSize"

                Expect.isTrue
                    (isWhat result)
                    (sprintf
                        "Inserting a %s leaf into a %s node at position %d should result in a %s node"
                        leafDesc
                        namePart
                        insertPos
                        isWhatStr)

        let name =
            sprintf
                "%s on a %s node with a %s leaf inserted at %d"
                fname
                namePart
                leafDesc
                insertPos

        testCase name test
    )

let insertTests =
    inputDataForInsertTests
    |> List.collect mkInsertTests
    |> testList "Insert tests"

let insertPropertyTests =
    testList "Insert property tests" [
        testProp "FsCheck InsertChild on a generated node"
        <| fun (IsolatedShortNode node) (NonNegativeInt idx) ->
            let idx =
                idx % (node.NodeSize
                       + 1)

            checkNodeProperties Literals.shiftSize node "Starting node"
            let newChild = mkSimpleLeaf (M - 2)
            let result = node.InsertChild(nullOwner, Literals.shiftSize, idx, newChild)
            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
                              + 1

        testProp "FsCheck InsertChildS on a generated node"
        <| fun (IsolatedShortNode node) (NonNegativeInt idx) ->
            let idx =
                idx % (node.NodeSize
                       + 1)

            checkNodeProperties Literals.shiftSize node "Starting node"
            let newChild = mkSimpleLeaf (M - 2)

            let result =
                node.InsertChildS(nullOwner, Literals.shiftSize, idx, newChild, (M - 2))

            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
                              + 1
    ]

let removePropertyTests =
    testList "Remove property tests" [
        testProp "FsCheck RemoveChild on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (NonNegativeInt idx) ->
            node.NodeSize > 1
            ==> lazy
                (let idx = idx % node.NodeSize
                 checkNodeProperties Literals.shiftSize node "Starting node"
                 let result = node.RemoveChild(nullOwner, Literals.shiftSize, idx)
                 checkNodeProperties Literals.shiftSize result "Result"

                 result.NodeSize = node.NodeSize
                                   - 1)

        testProp "FsCheck RemoveLastChild on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) ->
            node.NodeSize > 1
            ==> lazy
                (checkNodeProperties Literals.shiftSize node "Starting node"
                 let result = node.RemoveLastChild(nullOwner, Literals.shiftSize)
                 checkNodeProperties Literals.shiftSize result "Result"

                 result.NodeSize = node.NodeSize
                                   - 1)
    ]

let updatePropertyTests =
    testList "Update property tests" [
        testProp "FsCheck UpdateChild on a generated node"
        <| fun (IsolatedNode node) (NonNegativeInt idx) ->
            let idx = idx % node.NodeSize
            let oldLeaf = node.Children.[idx]
            let newLeaf = mkSimpleLeaf (oldLeaf.NodeSize)
            checkNodeProperties Literals.shiftSize node "Starting node"
            let result = node.UpdateChild(nullOwner, Literals.shiftSize, idx, newLeaf)
            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
            && result.TreeSize Literals.shiftSize = node.TreeSize Literals.shiftSize

        testProp "FsCheck UpdateChildSAbs on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (NonNegativeInt idx) (LeafNode newLeaf) ->
            let idx = idx % node.NodeSize
            let oldLeaf = node.Children.[idx]

            let sizeDiff =
                newLeaf.NodeSize
                - oldLeaf.NodeSize

            checkNodeProperties Literals.shiftSize node "Starting node"

            let result =
                node.UpdateChildSAbs(nullOwner, Literals.shiftSize, idx, newLeaf, newLeaf.NodeSize)

            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
            && result.TreeSize Literals.shiftSize = node.TreeSize Literals.shiftSize
                                                    + sizeDiff

        testProp "FsCheck UpdateChildSRel on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (NonNegativeInt idx) (LeafNode newLeaf) ->
            let idx = idx % node.NodeSize
            let oldLeaf = node.Children.[idx]

            let sizeDiff =
                newLeaf.NodeSize
                - oldLeaf.NodeSize

            checkNodeProperties Literals.shiftSize node "Starting node"

            let result =
                node.UpdateChildSRel(nullOwner, Literals.shiftSize, idx, newLeaf, sizeDiff)

            checkNodeProperties Literals.shiftSize result "Result"

            result.NodeSize = node.NodeSize
            && result.TreeSize Literals.shiftSize = node.TreeSize Literals.shiftSize
                                                    + sizeDiff
    ]

let keepPropertyTests =
    testList "KeepN(Left/Right) property tests" [
        testProp (*1913961525, 296651679*) "FsCheck KeepNLeft on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (PositiveInt n) ->
            let n =
                n % (node.NodeSize
                     + 1)
                |> max 1

            checkNodeProperties Literals.shiftSize node "Starting node"
            let result = node.KeepNLeft(nullOwner, Literals.shiftSize, n)
            checkNodeProperties Literals.shiftSize result "Result"

            let keptLeaves =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.truncate n

            let totalKeptSize =
                keptLeaves
                |> Array.sumBy (fun leaf -> leaf.NodeSize)

            result.NodeSize = n
            && result.TreeSize Literals.shiftSize = totalKeptSize

        testProp (*1914116381, 296651679*) "FsCheck KeepNRight on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (PositiveInt n) ->
            let n =
                n % (node.NodeSize
                     + 1)
                |> max 1

            checkNodeProperties Literals.shiftSize node "Starting node"
            let result = node.KeepNRight(nullOwner, Literals.shiftSize, n)
            checkNodeProperties Literals.shiftSize result "Result"

            let keptLeaves =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.skip (
                    node.NodeSize
                    - n
                )

            let totalKeptSize =
                keptLeaves
                |> Array.sumBy (fun leaf -> leaf.NodeSize)

            result.NodeSize = n
            && result.TreeSize Literals.shiftSize = totalKeptSize
    ]

let splitAndKeepPropertyTests =
    testList "SplitAndKeep property tests" [
        testProp "FsCheck SplitAndKeepNLeft on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (PositiveInt n) ->
            let n =
                n % (node.NodeSize
                     + 1)
                |> max 1

            let origLeavesL, origLeavesR =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.splitAt n

            let totalKeptL =
                origLeavesL
                |> Array.sumBy (fun leaf -> leaf.NodeSize)

            checkNodeProperties Literals.shiftSize node "Starting node"

            let struct (resultNode, resultLeavesR) =
                node.SplitAndKeepNLeft(nullOwner, Literals.shiftSize, n)

            checkNodeProperties Literals.shiftSize resultNode "Result"
            Expect.equal resultNode.NodeSize n "Node after split should have N items"

            Expect.equal
                (Array.length resultLeavesR)
                (Array.length origLeavesR)
                "Array of leaves returned from split should have (size - N) items"

            Expect.equal
                (resultNode.TreeSize Literals.shiftSize)
                totalKeptL
                "Node after split should have same tree size as total of remaining N items"

        testProp "FsCheck SplitAndKeepNRight on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (PositiveInt n) ->
            let n =
                n % (node.NodeSize
                     + 1)
                |> max 1

            let origLeavesL, origLeavesR =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.splitAt (
                    node.NodeSize
                    - n
                )

            let totalKeptR =
                origLeavesR
                |> Array.sumBy (fun leaf -> leaf.NodeSize)

            checkNodeProperties Literals.shiftSize node "Starting node"

            let struct (resultLeavesL, resultNode) =
                node.SplitAndKeepNRight(nullOwner, Literals.shiftSize, n)

            checkNodeProperties Literals.shiftSize resultNode "Result"
            Expect.equal resultNode.NodeSize n "Node after split should have N items"

            Expect.equal
                (Array.length resultLeavesL)
                (Array.length origLeavesL)
                "Array of leaves returned from split should have (size - N) items"

            Expect.equal
                (resultNode.TreeSize Literals.shiftSize)
                totalKeptR
                "Node after split should have same tree size as total of remaining N items"

        testProp "FsCheck SplitAndKeepNLeftS on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (PositiveInt n) ->
            let n =
                n % (node.NodeSize
                     + 1)
                |> max 1

            let origLeavesL, origLeavesR =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.splitAt n

            let totalKeptL =
                origLeavesL
                |> Array.sumBy (fun leaf -> leaf.NodeSize)

            checkNodeProperties Literals.shiftSize node "Starting node"

            let struct (resultNode, struct (resultLeavesR, resultSizesR)) =
                node.SplitAndKeepNLeftS(nullOwner, Literals.shiftSize, n)

            let expectedSizesR =
                origLeavesR
                |> Seq.map (fun leaf -> leaf.NodeSize)
                |> Seq.scan (+) 0
                |> Seq.tail
                |> Array.ofSeq

            checkNodeProperties Literals.shiftSize resultNode "Result"
            Expect.equal resultNode.NodeSize n "Node after split should have N items"

            Expect.equal
                (Array.length resultLeavesR)
                (Array.length origLeavesR)
                "Array of leaves returned from split should have (size - N) items"

            Expect.equal
                resultSizesR
                expectedSizesR
                "Sizes returned from split should be cumulative sizes of leaves returned from split"

            Expect.equal
                (resultNode.TreeSize Literals.shiftSize)
                totalKeptL
                "Node after split should have same tree size as total of remaining N items"

        testProp "FsCheck SplitAndKeepNRightS on a generated node"
        <| fun (IsolatedNode node: IsolatedNode<int>) (PositiveInt n) ->
            let n =
                n % (node.NodeSize
                     + 1)
                |> max 1

            let origLeavesL, origLeavesR =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.splitAt (
                    node.NodeSize
                    - n
                )

            let totalKeptR =
                origLeavesR
                |> Array.sumBy (fun leaf -> leaf.NodeSize)

            checkNodeProperties Literals.shiftSize node "Starting node"

            let struct (struct (resultLeavesL, resultSizesL), resultNode) =
                node.SplitAndKeepNRightS(nullOwner, Literals.shiftSize, n)

            let expectedSizesL =
                origLeavesL
                |> Seq.map (fun leaf -> leaf.NodeSize)
                |> Seq.scan (+) 0
                |> Seq.tail
                |> Array.ofSeq

            checkNodeProperties Literals.shiftSize resultNode "Result"
            Expect.equal resultNode.NodeSize n "Node after split should have N items"

            Expect.equal
                (Array.length resultLeavesL)
                (Array.length origLeavesL)
                "Array of leaves returned from split should have (size - N) items"

            Expect.equal
                resultSizesL
                expectedSizesL
                "Sizes returned from split should be cumulative sizes of leaves returned from split"

            Expect.equal
                (resultNode.TreeSize Literals.shiftSize)
                totalKeptR
                "Node after split should have same tree size as total of remaining N items"
    ]

let appendAndPrependChildrenPropertyTests =
    testList "(Ap/Pre)pendChildren property tests" [
        testProp "FsCheck AppendNChildren on a generated node"
        <| fun (IsolatedShortNode node: IsolatedShortNode<int>) (newLeaves: RRBLeafNode<int>[]) ->
            let remaining =
                Literals.blockSize
                - node.NodeSize

            let toAdd =
                newLeaves
                |> Array.truncate remaining

            let n = toAdd.Length

            let origLeafArrays =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            let newLeafArrays =
                toAdd
                |> Array.map (fun leaf -> leaf.Items)

            let expectedLeafArrays = Array.append origLeafArrays newLeafArrays

            checkNodeProperties Literals.shiftSize node "Starting node"

            let result =
                node.AppendNChildren(nullOwner, Literals.shiftSize, n, (Seq.cast toAdd), true)
                :?> RRBFullNode<int>

            checkNodeProperties Literals.shiftSize result "Result"

            Expect.equal
                result.NodeSize
                (node.NodeSize
                 + n)
                "Node after append should have N more items"

            let actualLeafArrays =
                result.Children
                |> Array.truncate result.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            Expect.equal
                actualLeafArrays
                expectedLeafArrays
                "Leaves should have been placed in the correct locations"

        testProp "FsCheck AppendNChildrenS on a generated node"
        <| fun (IsolatedShortNode node: IsolatedShortNode<int>) (newLeaves: RRBLeafNode<int>[]) ->
            let remaining =
                Literals.blockSize
                - node.NodeSize

            let toAdd =
                newLeaves
                |> Array.truncate remaining

            let n = toAdd.Length

            let sizes =
                toAdd
                |> Seq.map (fun n -> n.NodeSize)
                |> Seq.scan (+) 0
                |> Seq.tail

            let origLeafArrays =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            let newLeafArrays =
                toAdd
                |> Array.map (fun leaf -> leaf.Items)

            let expectedLeafArrays = Array.append origLeafArrays newLeafArrays

            checkNodeProperties Literals.shiftSize node "Starting node"

            let result =
                node.AppendNChildrenS(
                    nullOwner,
                    Literals.shiftSize,
                    n,
                    (Seq.cast toAdd),
                    sizes,
                    true
                )
                :?> RRBFullNode<int>

            checkNodeProperties Literals.shiftSize result "Result"

            Expect.equal
                result.NodeSize
                (node.NodeSize
                 + n)
                "Node after prepend should have N more items"

            let actualLeafArrays =
                result.Children
                |> Array.truncate result.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            Expect.equal
                actualLeafArrays
                expectedLeafArrays
                "Leaves should have been placed in the correct locations"

        testProp "FsCheck PrependNChildren on a generated node"
        <| fun (IsolatedShortNode node: IsolatedShortNode<int>) (newLeaves: RRBLeafNode<int>[]) ->
            let remaining =
                Literals.blockSize
                - node.NodeSize

            let toAdd =
                newLeaves
                |> Array.truncate remaining

            let n = toAdd.Length

            let origLeafArrays =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            let newLeafArrays =
                toAdd
                |> Array.map (fun leaf -> leaf.Items)

            let expectedLeafArrays = Array.append newLeafArrays origLeafArrays

            checkNodeProperties Literals.shiftSize node "Starting node"

            let result =
                node.PrependNChildren(nullOwner, Literals.shiftSize, n, (Seq.cast toAdd))
                :?> RRBFullNode<int>

            checkNodeProperties Literals.shiftSize result "Result"

            Expect.equal
                result.NodeSize
                (node.NodeSize
                 + n)
                "Node after prepend should have N more items"

            let actualLeafArrays =
                result.Children
                |> Array.truncate result.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            Expect.equal
                actualLeafArrays
                expectedLeafArrays
                "Leaves should have been placed in the correct locations"

        testProp "FsCheck PrependNChildrenS on a generated node"
        <| fun (IsolatedShortNode node: IsolatedShortNode<int>) (newLeaves: RRBLeafNode<int>[]) ->
            let remaining =
                Literals.blockSize
                - node.NodeSize

            let toAdd =
                newLeaves
                |> Array.truncate remaining

            let n = toAdd.Length

            let sizes =
                toAdd
                |> Seq.map (fun n -> n.NodeSize)
                |> Seq.scan (+) 0
                |> Seq.tail

            let origLeafArrays =
                node.Children
                |> Array.truncate node.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            let newLeafArrays =
                toAdd
                |> Array.map (fun leaf -> leaf.Items)

            let expectedLeafArrays = Array.append newLeafArrays origLeafArrays

            checkNodeProperties Literals.shiftSize node "Starting node"

            let result =
                node.PrependNChildrenS(nullOwner, Literals.shiftSize, n, (Seq.cast toAdd), sizes)
                :?> RRBFullNode<int>

            checkNodeProperties Literals.shiftSize result "Result"

            Expect.equal
                result.NodeSize
                (node.NodeSize
                 + n)
                "Node after prepend should have N more items"

            let actualLeafArrays =
                result.Children
                |> Array.truncate result.NodeSize
                |> Array.map (fun leaf -> (leaf :?> RRBLeafNode<int>).Items)

            Expect.equal
                actualLeafArrays
                expectedLeafArrays
                "Leaves should have been placed in the correct locations"
    ]

let twigItems (node: RRBNode<'T>) =
    (node :?> RRBFullNode<'T>).Children
    |> Seq.truncate node.NodeSize
    |> Seq.cast<RRBLeafNode<'T>>
    |> Seq.collect (fun leaf -> leaf.Items)

let rec nodeItems shift (node: RRBNode<'T>) =
    if
        shift
        <= Literals.shiftSize
    then
        twigItems node
    else
        (node :?> RRBFullNode<'T>).Children
        |> Seq.truncate node.NodeSize
        |> Seq.cast<RRBFullNode<'T>>
        |> Seq.collect (
            nodeItems (
                shift
                - Literals.shiftSize
            )
        )

let doRebalance2Test shift (nodeL: RRBNode<'T>) (nodeR: RRBNode<'T>) =
    let slotCountL =
        if
            shift
            <= Literals.shiftSize
        then
            nodeL.TwigSlotCount
        else
            nodeL.SlotCount

    let slotCountR =
        if
            shift
            <= Literals.shiftSize
        then
            nodeR.TwigSlotCount
        else
            nodeR.SlotCount

    let totalSize =
        nodeL.NodeSize
        + nodeR.NodeSize

    let minSize =
        (slotCountL
         + slotCountR
         - 1)
        / Literals.blockSize
        + 1

    let needsRebalancing =
        totalSize
        - minSize > Literals.radixSearchErrorMax

    needsRebalancing
    ==> lazy
        (
         // Need to do this before the rebalance, because after the rebalance the original nodeL may be invalid if it was an expanded node
         let expected =
             Seq.append (nodeItems shift nodeL) (nodeItems shift nodeR)
             |> Array.ofSeq

         let struct (newL, newR) =
             (nodeL :?> RRBFullNode<'T>)
                 .Rebalance2PlusLeaf(nullOwner, shift, null, (nodeR :?> RRBFullNode<'T>))

         match newR with
         | null ->
             Expect.isLessThanOrEqual
                 minSize
                 Literals.blockSize
                 "If both nodes add up to a NodeSize of M or less, should end up with just one node at the end"

             Expect.isLessThanOrEqual
                 newL.NodeSize
                 Literals.blockSize
                 "After rebalancing, left node should be at most M items long"

             Expect.equal
                 (nodeItems shift newL
                  |> Array.ofSeq)
                 expected
                 "Order of items should not change during rebalance"

             checkNodeProperties shift newL "Newly-rebalanced merged node"
         | nodeR' ->
             Expect.equal
                 (Seq.append (nodeItems shift newL) (nodeItems shift nodeR')
                  |> Array.ofSeq)
                 expected
                 "Order of items should not change during rebalance"

             Expect.equal
                 newL.NodeSize
                 Literals.blockSize
                 "After rebalancing, if a right node exists then left node should be exactly M items long"

             checkNodeProperties shift newL "Newly-rebalanced left node"
             checkNodeProperties shift nodeR' "Newly-rebalanced right node"
        // TODO: Probably want more here
        )

let findMergeCandidatesExhaustive (sizeSeq: #seq<int>) len =
    use e = sizeSeq.GetEnumerator()

    let sizes =
        Array.init
            len
            (fun _ ->
                if e.MoveNext() then
                    Literals.blockSize
                    - e.Current
                else
                    0
            )

    let mutable results = []

    for i = 0 to len - 1 do
        for j = i to len - 1 do
            if
                Array.sum sizes.[i..j]
                >= Literals.blockSize
            then
                results <-
                    (i, j - i + 1)
                    :: results

    results
    |> List.sortBy (fun (idx, len) -> len, idx)

let rebalanceTestsWIP =
    testList "WIP: Rebalance tests" [
        testProp "FsCheck Try this"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            doRebalance2Test Literals.shiftSize nodeL nodeR

        testProp
            "FsCheck findMergeCandidates with two passes either finds a better match than with one pass, or finds the same match if there isn't a better one"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            let sizesL =
                nodeL.ChildrenSeq
                |> Seq.map (fun n -> n.NodeSize)

            let sizesR =
                nodeR.ChildrenSeq
                |> Seq.map (fun n -> n.NodeSize)

            let sizesCombined = Seq.append sizesL sizesR

            let lenBothNodes =
                nodeL.NodeSize
                + nodeR.NodeSize

            let struct (idx1, len1) = RRBMath.FindMergeCandidates(sizesCombined, lenBothNodes)

            let struct (idx2, len2, reduction) =
                RRBMath.FindMergeCandidatesTwoPasses(sizesCombined, lenBothNodes)

            if reduction = 1 then
                Expect.equal (idx2, len2) (idx1, len1)
                <| sprintf "Found somewhere where two passes had a different result than one pass"
            else
                Expect.isGreaterThan
                    reduction
                    1
                    "Found somewhere where two passes found no reduction at all"

        testCase "findMergeCandidates performs better with two passes than with one (single)"
        <| fun _ ->
            let l = RRBVectorGen.treeReprStrToVec "M-2 M M M T1" :?> RRBPersistentVector<int>

            let r =
                RRBVectorGen.treeReprStrToVec
                    "M-4 M M-2 M M-1 M M/2 M M/2+1 M M/2+3 M M/2+4 M M/2+6 T1"
                :?> RRBPersistentVector<int>

            let sizesL =
                (l.Root :?> RRBFullNode<_>).ChildrenSeq
                |> Seq.map (fun n -> n.NodeSize)

            let sizesR =
                (r.Root :?> RRBFullNode<_>).ChildrenSeq
                |> Seq.map (fun n -> n.NodeSize)

            let sizesCombined = Seq.append sizesL sizesR

            let lenBothNodes =
                l.Root.NodeSize
                + r.Root.NodeSize

            let struct (idx1, len1) = RRBMath.FindMergeCandidates(sizesCombined, lenBothNodes)

            let struct (idx2, len2, reduction) =
                RRBMath.FindMergeCandidatesTwoPasses(sizesCombined, lenBothNodes)

            Expect.equal
                (idx1, len1)
                (8, 5)
                "findMergeCandidates finds a length-5 merge and stops there"

            Expect.equal
                (idx2, len2)
                (10, 9)
                "findMergeCandidatesTwoPasses finds a length-9 merge and uses it instead"

            Expect.equal
                reduction
                2
                "findMergeCandidatesTwoPasses finds merge that reduces size by 2"

        testCase "findMergeCandidates does not always find an optimum solution"
        <| fun _ ->
            let l =
                RRBVectorGen.treeReprStrToVec
                    "20 M 32 M 23 M 17 M 26 M 20 M 29 24 M 18 M 27 M 21 M 30 M 24 M 16 M 17 M 19 M 20 T1"
                :?> RRBPersistentVector<int>

            let r = RRBVectorGen.treeReprStrToVec "M*M T1" :?> RRBPersistentVector<int>

            let sizesL =
                (l.Root :?> RRBFullNode<_>).ChildrenSeq
                |> Seq.map (fun n -> n.NodeSize)

            let sizesR =
                (r.Root :?> RRBFullNode<_>).ChildrenSeq
                |> Seq.map (fun n -> n.NodeSize)

            let sizesCombined =
                Seq.append sizesL sizesR
                |> Array.ofSeq

            let invertedSizesCombined =
                sizesCombined
                |> Array.map (fun n ->
                    Literals.blockSize
                    - n
                )

            let lenBothNodes =
                (l.Root :?> RRBFullNode<_>).NodeSize
                + (r.Root :?> RRBFullNode<_>).NodeSize

            let struct (idx2, len2, reduction) =
                RRBMath.FindMergeCandidatesTwoPasses(sizesCombined, lenBothNodes)

            let exhaustiveResults = findMergeCandidatesExhaustive sizesCombined lenBothNodes

            let exhaustiveResultsWithReductions =
                exhaustiveResults
                |> List.map (fun (idx, len) ->
                    let exhaustiveReduction =
                        (Array.sub invertedSizesCombined idx len
                         |> Array.sum)
                        / Literals.blockSize

                    idx, len, exhaustiveReduction
                )

            let betterThanTwoPasses =
                exhaustiveResultsWithReductions
                |> List.filter (fun (idx, len, r) ->
                    r > reduction
                    && (len < len2 * 2)
                )

            Expect.equal
                (List.head betterThanTwoPasses)
                (15, 17, 3)
                "Optimal solution would reduce by 3"

            Expect.equal
                (idx2, len2, reduction)
                (23, 9, 2)
                "Two-pass solution reduces by 2 but does half the work"

        testProp "FsCheck NeedsRebalancing function uses correct formula"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            let shift = Literals.shiftSize

            let slotCountL =
                if
                    shift
                    <= Literals.shiftSize
                then
                    nodeL.TwigSlotCount
                else
                    nodeL.SlotCount

            let slotCountR =
                if
                    shift
                    <= Literals.shiftSize
                then
                    nodeR.TwigSlotCount
                else
                    nodeR.SlotCount

            let totalSize =
                nodeL.NodeSize
                + nodeR.NodeSize

            let minSize =
                (slotCountL
                 + slotCountR
                 - 1)
                / Literals.blockSize
                + 1

            let needsRebalancing =
                totalSize
                - minSize > Literals.radixSearchErrorMax

            Expect.equal (nodeL.NeedsRebalance2(shift, nodeR)) needsRebalancing
            <| sprintf "NeedsRebalancing was wrong for left %A and right %A" nodeL nodeR

        testProp "FsCheck Concat test"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            let shift = Literals.shiftSize
            // Need to do this before the concatenation, because after the concatenation the original nodeL may be invalid if it was an expanded node
            let expected =
                Seq.append (nodeItems shift nodeL) (nodeItems shift nodeR)
                |> Array.ofSeq

            let struct (newL, newR) = nodeL.ConcatNodes(nullOwner, shift, nodeR)

            match newR with
            | null ->
                Expect.isLessThanOrEqual
                    newL.NodeSize
                    Literals.blockSize
                    "After concating, left node should be at most M items long"

                Expect.equal
                    (nodeItems shift newL
                     |> Array.ofSeq)
                    expected
                    "Order of items should not change during concatenate"

                checkNodeProperties shift newL "Newly-concatenated merged node"
            | nodeR' ->
                Expect.equal
                    (Seq.append (nodeItems shift newL) (nodeItems shift nodeR')
                     |> Array.ofSeq)
                    expected
                    "Order of items should not change during concatenate"

                let totalOldSize =
                    nodeL.NodeSize
                    + nodeR.NodeSize

                let validNewSizes =
                    if nodeL.NeedsRebalance2(shift, nodeR) then
                        [
                            totalOldSize
                            - 2
                            totalOldSize
                            - 1
                        ]
                    else
                        [ totalOldSize ]

                let totalNewSize =
                    newL.NodeSize
                    + nodeR'.NodeSize

                Expect.contains
                    validNewSizes
                    totalNewSize
                    "After concating, the total size should either be the same, or go down by just one or two items"

                checkNodeProperties shift newL "Newly-concatenated left node"
                checkNodeProperties shift nodeR' "Newly-concatenated right node"

        testProp "FsCheck Concat-with-leaf test"
        <| fun
               (IsolatedNode nodeL: IsolatedNode<int>)
               (LeafNode leaf: LeafNode<int>)
               (IsolatedNode nodeR: IsolatedNode<int>) ->
            let shift = Literals.shiftSize
            // Need to do this before the concatenation, because after the concatenation the original nodeL may be invalid if it was an expanded node
            let expected =
                Seq.concat [
                    nodeItems shift nodeL
                    leaf.Items
                    |> Seq.ofArray
                    nodeItems shift nodeR
                ]
                |> Array.ofSeq

            if nodeL.HasRoomToMergeTheTail(shift, leaf, nodeR) then
                let struct (newL, newR) = nodeL.ConcatTwigsPlusLeaf(nullOwner, shift, leaf, nodeR)

                match newR with
                | null ->
                    Expect.isLessThanOrEqual
                        newL.NodeSize
                        Literals.blockSize
                        "After concating, left node should be at most M items long"

                    Expect.equal
                        (nodeItems shift newL
                         |> Array.ofSeq)
                        expected
                        "Order of items should not change during concatenate"

                    checkNodeProperties shift newL "Newly-concatenated merged node"
                | nodeR' ->
                    Expect.equal
                        (Seq.append (nodeItems shift newL) (nodeItems shift nodeR')
                         |> Array.ofSeq)
                        expected
                        "Order of items should not change during concatenate"

                    let totalOldSize =
                        nodeL.NodeSize
                        + 1
                        + nodeR.NodeSize

                    let validNewSizes =
                        if nodeL.NeedsRebalance2PlusLeaf(shift, leaf.NodeSize, nodeR) then
                            [
                                totalOldSize
                                - 2
                                totalOldSize
                                - 1
                            ]
                        else
                            [ totalOldSize ]

                    let totalNewSize =
                        newL.NodeSize
                        + nodeR'.NodeSize

                    Expect.contains
                        validNewSizes
                        totalNewSize
                        "After concating, the total size should either be the same, or go down by just one or two items"

                    checkNodeProperties shift newL "Newly-concatenated left node"
                    checkNodeProperties shift nodeR' "Newly-concatenated right node"
    ]

let doIndividualMergeTestLeftTwigRightTwoNodeTree L R1 R2 =
    let counter = mkCounter ()
    let shift = Literals.shiftSize

    let nodeL =
        L
        |> Array.map (
            mkLeaf counter
            >> fun node -> node :> RRBNode<int>
        )
        |> fun arr -> RRBNode<int>.MkNode(nullOwner, shift, arr)

    let nodeR1 =
        R1
        |> Array.map (
            mkLeaf counter
            >> fun node -> node :> RRBNode<int>
        )
        |> fun arr -> RRBNode<int>.MkNode(nullOwner, shift, arr)

    let nodeR2 =
        R2
        |> Array.map (
            mkLeaf counter
            >> fun node -> node :> RRBNode<int>
        )
        |> fun arr -> RRBNode<int>.MkNode(nullOwner, shift, arr)

    let nodeR =
        RRBNode<int>
            .MkNode(
                nullOwner,
                (shift * 2),
                [|
                    nodeR1
                    nodeR2
                |]
            )

    let origCombined =
        Seq.append (nodeItems shift nodeL) (nodeItems (shift * 2) nodeR)
        |> Array.ofSeq

    let struct (newL, newR) =
        (nodeL :?> RRBFullNode<int>)
            .MergeTree(nullOwner, shift, null, (shift * 2), (nodeR :?> RRBFullNode<int>), false)
    // TODO: let newR, _ = newR.toPersistent  ... except down in the match expression
    let arrL' =
        newL
        |> nodeItems (shift * 2)

    match newR with
    | null ->
        Expect.equal
            (arrL'
             |> Array.ofSeq)
            origCombined
            "Order of items should not change during merge"

        checkNodeProperties (shift * 2) newL "Newly merged node"
    | nodeR ->
        let nodeR = toPersistent nodeR

        let arrR' =
            nodeR
            |> nodeItems (shift * 2)

        let arrCombined =
            Seq.append arrL' arrR'
            |> Array.ofSeq

        Expect.equal arrCombined origCombined "Order of items should not change during merge"
        checkNodeProperties (shift * 2) newL "Newly merged node"
        checkNodeProperties (shift * 2) nodeR "Newly merged node"

        let newRoot =
            (newL :?> RRBFullNode<int>)
                .NewParent(
                    nullOwner,
                    (shift * 2),
                    [|
                        newL
                        nodeR
                    |]
                )

        checkNodeProperties (shift * 3) newRoot "Newly merged node"

let splitTreeTests =
    testList "Split tests" [
        testProp (*871740682, 296591768*) "FsCheck Keep"
        <| fun (IsolatedNode root: IsolatedNode<int>) (NonNegativeInt idx) ->
            let shift = Literals.shiftSize

            let keep =
                (idx % root.TreeSize shift)
                + 1
                |> min (
                    root.TreeSize shift
                    - 1
                )

            checkNodeProperties shift root "Original node"

            let expected =
                nodeItems shift root
                |> Seq.truncate keep
                |> Array.ofSeq

            let newRoot = root.KeepNTreeItems(nullOwner, shift, keep)
            checkNodeProperties shift newRoot "Root after keep"

            Expect.equal
                (nodeItems shift newRoot
                 |> Array.ofSeq)
                expected
                "Items after keep are still the same"

        testProp "FsCheck Skip"
        <| fun (IsolatedNode root: IsolatedNode<int>) (NonNegativeInt idx) ->
            let shift = Literals.shiftSize

            let skip =
                (idx % root.TreeSize shift)
                + 1
                |> min (
                    root.TreeSize shift
                    - 1
                )

            checkNodeProperties shift root "Original node"

            let expected =
                nodeItems shift root
                |> Seq.skip skip
                |> Array.ofSeq

            let newRoot = root.SkipNTreeItems(nullOwner, shift, skip)
            checkNodeProperties shift newRoot "Root after skip"

            Expect.equal
                (nodeItems shift newRoot
                 |> Array.ofSeq)
                expected
                "Items after skip are still the same"

        testProp "FsCheck Split"
        <| fun (IsolatedNode root: IsolatedNode<int>) (NonNegativeInt idx) ->
            let shift = Literals.shiftSize

            let idx =
                (idx % root.TreeSize shift)
                + 1
                |> min (
                    root.TreeSize shift
                    - 1
                )

            checkNodeProperties shift root "Original node"

            let expectedArr =
                nodeItems shift root
                |> Array.ofSeq

            let expectedL = expectedArr.[0 .. idx - 1]
            let expectedR = expectedArr.[idx..]
            let struct (newL, newR) = root.SplitTree(nullOwner, shift, idx)
            checkNodeProperties shift newL "Left node after split"
            checkNodeProperties shift newR "Right node after split"

            Expect.equal
                (nodeItems shift newL
                 |> Array.ofSeq)
                expectedL
                "Items in left split are still the same"

            Expect.equal
                (nodeItems shift newR
                 |> Array.ofSeq)
                expectedR
                "Items in right split are still the same"
    ]

let doMergeTest (shiftL, (nodeL: RRBNode<'T>)) (shiftR, nodeR) =
    let reprL =
        RRBVectorGen.nodeToTreeReprStr
            (shiftL
             / Literals.shiftSize)
            nodeL

    let reprR =
        RRBVectorGen.nodeToTreeReprStr
            (shiftR
             / Literals.shiftSize)
            nodeR

    checkNodeProperties shiftL nodeL
    <| sprintf "Original left node %d %s" shiftL reprL

    checkNodeProperties shiftR nodeR
    <| sprintf "Original right node %d %s" shiftR reprR

    let expected =
        Seq.concat [
            nodeItems shiftL nodeL
            nodeItems shiftR nodeR
        ]
        |> Array.ofSeq

    let struct (newL, newR) =
        (nodeL :?> RRBFullNode<'T>).MergeTree(nullOwner, shiftL, null, shiftR, nodeR, false)

    let newShift = max shiftL shiftR

    match newR with
    | null ->
        let newL = toPersistent newL

        Expect.equal
            (nodeItems newShift newL
             |> Array.ofSeq)
            expected
        <| sprintf
            "Order of items should not change during merge from L %d %s and R %d %s"
            shiftL
            reprL
            shiftR
            reprR

        checkNodeProperties newShift newL
        <| sprintf "Newly merged node from L %d %s and R %d %s" shiftL reprL shiftR reprR
    | nodeR' ->
        let newL = toPersistent newL
        let nodeR' = toPersistent nodeR'

        Expect.equal
            (Seq.append (nodeItems newShift newL) (nodeItems newShift nodeR')
             |> Array.ofSeq)
            expected
        <| sprintf
            "Order of items should not change during merge from L %d %s and R %d %s"
            shiftL
            reprL
            shiftR
            reprR

        let parent =
            (newL :?> RRBFullNode<'T>)
                .NewParent(
                    nullOwner,
                    newShift,
                    [|
                        newL
                        nodeR'
                    |]
                )

        checkNodeProperties (up newShift) parent
        <| sprintf "Newly rooted merged tree from L %d %s and R %d %s" shiftL reprL shiftR reprR

        checkNodeProperties newShift newL
        <| sprintf "Newly merged left node from L %d %s and R %d %s" shiftL reprL shiftR reprR

        checkNodeProperties newShift nodeR'
        <| sprintf "Newly merged right node from L %d %s and R %d %s" shiftL reprL shiftR reprR

let mergeTreeTestsWIP =
    testList "WIP: Merge tests" [
        testProp "FsCheck Merging twigs"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            let shift = Literals.shiftSize

            let expected =
                Seq.concat [
                    nodeItems shift nodeL
                    nodeItems shift nodeR
                ]
                |> Array.ofSeq

            let struct (newL, newR) =
                nodeL.MergeTree(nullOwner, shift, null, shift, nodeR, false)

            match newR with
            | null ->
                let newL = toPersistent newL

                Expect.isLessThanOrEqual
                    newL.NodeSize
                    Literals.blockSize
                    "After merging, left node should be at most M items long"

                Expect.equal
                    (nodeItems shift newL
                     |> Array.ofSeq)
                    expected
                    "Order of items should not change during merge"

                checkNodeProperties shift newL "Newly merged node"
            | nodeR' ->
                let newL = toPersistent newL
                let nodeR' = toPersistent nodeR'

                Expect.equal
                    (Seq.append (nodeItems shift newL) (nodeItems shift nodeR')
                     |> Array.ofSeq)
                    expected
                    "Order of items should not change during merge"

                let totalOldSize =
                    nodeL.NodeSize
                    + nodeR.NodeSize

                let validNewSizes =
                    if nodeL.NeedsRebalance2(shift, nodeR) then
                        [
                            totalOldSize
                            - 2
                            totalOldSize
                            - 1
                        ]
                    else
                        [ totalOldSize ]

                let totalNewSize =
                    newL.NodeSize
                    + nodeR'.NodeSize

                Expect.contains
                    validNewSizes
                    totalNewSize
                    "After merging, the total size should either be the same, or go down by just one or two items"

                checkNodeProperties shift newL "Newly merged left node"
                checkNodeProperties shift nodeR' "Newly merged right node"

        testProp (*7886235, 296578399*) "FsCheck Merging left twig with right tree"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (RootNode nodeR: RootNode<int>) ->
            let shiftL = Literals.shiftSize

            let shiftR =
                Literals.shiftSize
                * (height nodeR)

            doMergeTest (shiftL, nodeL) (shiftR, nodeR)

        testProp (*17045485, 296578399*) "FsCheck Merging left tree with right twig"
        <| fun (RootNode nodeL: RootNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            let shiftL =
                Literals.shiftSize
                * (height nodeL)

            let shiftR = Literals.shiftSize
            doMergeTest (shiftL, nodeL) (shiftR, nodeR)

        testCase
            "Regression test for merging two trees that end up growing in height but requiring right spine to be shrunk"
        <| fun _ ->
            let vecL = RRBVectorGen.treeReprStrToVec "[M*M] [M*10] T1"

            let transientL = vecL.Transient()

            let vecR =
                RRBVectorGen.treeReprStrToVec
                    "[16 M 18 M 19 M 21 M 22 M M 21 M 20 M 24 M 23 M 22 21 M 30 M 17] [23 M M 21 M M] T1"

            let transientR = vecR.Transient()

            let shift =
                Literals.shiftSize
                * 2

            let rootL = (transientL :?> RRBTransientVector<int>).Root
            let rootR = (transientR :?> RRBTransientVector<int>).Root

            let struct (mergedL, _) =
                (rootL :?> RRBFullNode<int>)
                    .MergeTree(nullOwner, shift, null, shift, rootR :?> RRBFullNode<int>, false)

            checkNodeProperties shift mergedL "Merged tree"

        testCase
            "Regression test for merging left twig with right tree where right spine needs to be shrunk"
        <| fun _ ->
            let vecL = RRBVectorGen.treeReprStrToVec "16 17 19 20 23 26 18 29 24 T1"
            let vecR = RRBVectorGen.treeReprStrToVec "[M*M] [M 26] T1"

            let shiftL = Literals.shiftSize

            let shiftR =
                Literals.shiftSize
                * 2

            let rootL = (vecL.Transient() :?> RRBTransientVector<int>).Root
            let rootR = (vecR.Transient() :?> RRBTransientVector<int>).Root :?> RRBFullNode<_>

            doMergeTest (shiftL, rootL) (shiftR, rootR)

        testProp "FsCheck Merging left tree with right tree"
        <| fun (RootNode nodeL: RootNode<int>) (RootNode nodeR: RootNode<int>) ->
            let shiftL =
                Literals.shiftSize
                * (height nodeL)

            let shiftR =
                Literals.shiftSize
                * (height nodeR)

            doMergeTest (shiftL, nodeL) (shiftR, nodeR)

        // TODO: Write some individual tests with the failures from src/Ficus/test-failures-2019-03-27.txt as a guideline.

        testCase
            "Left full twig, right height-2 tree with one fullish, relaxed node and final full node of size 4"
        <| fun _ ->
            let L = Array.replicate 32 32

            let R1 = [|
                32
                32
                31
                26
                32
                28
                18
                31
                32
                16
                32
                32
                32
                28
                32
                24
                32
                32
                32
                29
                25
                27
                32
                32
                32
                32
                32
                32
                32
                30
                32
                28
            |]

            let R2 = [|
                32
                32
                32
                32
            |]

            doIndividualMergeTestLeftTwigRightTwoNodeTree L R1 R2

        testCase
            "Left relaxed, halfish-full twig, right height-2 tree with one fullish, relaxed node and final relaxed node of size 5"
        <| fun _ ->
            let L = [|
                32
                27
                32
                29
                32
                30
                32
                22
                32
                16
                32
                25
                32
                27
                32
                28
                32
                32
                17
                32
                26
                32
            |]

            let R1 = [|
                32
                16
                32
                20
                32
                19
                32
                28
                32
                22
                19
                32
                21
                32
                22
                32
                24
                32
                25
                32
                17
                32
                28
                32
                20
                32
                22
                32
                23
                32
                32
                30
            |]

            let R2 = [|
                32
                31
                32
                16
                32
            |]

            doIndividualMergeTestLeftTwigRightTwoNodeTree L R1 R2

    ]

let largeMergeTreeTestsWIP =
    testList "WIP: Large tree-merge tests" [
        testProp (*3644257, 296578399*) "FsCheck Merging left twig with right large tree"
        <| fun (IsolatedNode nodeL: IsolatedNode<int>) (LargeRootNode nodeR: LargeRootNode<int>) ->
            let shiftL = Literals.shiftSize

            let shiftR =
                Literals.shiftSize
                * (height nodeR)

            doMergeTest (shiftL, nodeL) (shiftR, nodeR)

        testProp (*3643640, 296578399*) "FsCheck Merging left large tree with right twig"
        <| fun (LargeRootNode nodeL: LargeRootNode<int>) (IsolatedNode nodeR: IsolatedNode<int>) ->
            let shiftL =
                Literals.shiftSize
                * (height nodeL)

            let shiftR = Literals.shiftSize
            doMergeTest (shiftL, nodeL) (shiftR, nodeR)

        testProp "FsCheck Merging left large tree with right large tree"
        <| fun (LargeRootNode nodeL: LargeRootNode<int>) (LargeRootNode nodeR: LargeRootNode<int>) ->
            let shiftL =
                Literals.shiftSize
                * (height nodeL)

            let shiftR =
                Literals.shiftSize
                * (height nodeR)

            doMergeTest (shiftL, nodeL) (shiftR, nodeR)

    ]

let manualTests =
    testList "Tests of node methods that aren't covered by random testing" [
        testProp "FsCheck ToRelaxedNodeIfNeeded never changes an already-full node"
        <| fun (IsolatedNode node: IsolatedNode<int>) ->
            node
            |> isFull
            ==> fun () ->
                let maybeRelaxed =
                    (node :?> RRBFullNode<_>).ToRelaxedNodeIfNeeded Literals.shiftSize

                maybeRelaxed
                |> isFull

        testCase
            "ToRelaxedNodeIfNeeded will change a full node that has lost an item from a non-final child"
        <| fun _ ->
            let vec = RRBVectorGen.treeReprStrToVec "M M M T1" :?> RRBPersistentVector<_>

            let root = vec.Root :?> RRBFullNode<_>

            let root' =
                root.RemovedItem(
                    nullOwner,
                    vec.Shift,
                    false,
                    (Literals.blockSize
                     + 5)
                )
                :?> RRBFullNode<_>

            let maybeRelaxed = root'.ToRelaxedNodeIfNeeded Literals.shiftSize

            Expect.isTrue
                (maybeRelaxed
                 |> isRelaxed)
                "Node should have changed to be relaxed"

        testCase
            "ToRelaxedNodeIfNeeded will NOT change a full node that has lost an item from its final child"
        <| fun _ ->
            let vec = RRBVectorGen.treeReprStrToVec "M M M T1" :?> RRBPersistentVector<_>

            let root = vec.Root :?> RRBFullNode<_>

            let root' =
                root.RemovedItem(
                    nullOwner,
                    vec.Shift,
                    false,
                    (Literals.blockSize
                     * 2
                     + 5)
                )
                :?> RRBFullNode<_>
            // Note that vec.Remove would have adjusted this tree to promote a new leaf, so we use root.RemovedItem instead
            let maybeRelaxed = root'.ToRelaxedNodeIfNeeded Literals.shiftSize

            Expect.isTrue
                (maybeRelaxed
                 |> isFull)
                "Node should still be full"

        testProp "FsCheck ToFullNodeIfNeeded never changes an already-relaxed node"
        <| fun (IsolatedNode node: IsolatedNode<int>) ->
            node
            |> isRelaxed
            ==> fun () ->
                let maybeFull = (node :?> RRBRelaxedNode<_>).ToFullNodeIfNeeded Literals.shiftSize

                maybeFull
                |> isRelaxed

    // Still TODO: InsertChild on a full node, inserting a full child (let's do a leaf)
    // Check that full node should still be full after that (check node properties)
    // Check node properties on the above tests
    ]

// let debugGenTests =
//   ftestList "Debugging generators" [
//     testProp "FsCheck Sized int" <| fun (ShowSizedInt n) ->
//         ()
//   ]

// logger.debug (
//     eventX "Result: {node}"
//     >> setField "node" (sprintf "%A" result)
// )

let longRunningTests =
    testList "Long-running tests" [
        largeMergeTreeTestsWIP // This one is *extremely* long-running, in fact
    ]

[<Tests>]
let tests =
    testList "Node tests" [
        // debugGenTests

        // longRunningTests
        appendAndPrependChildrenPropertyTests // Put this first since it's so long
        splitTreeTests
        mergeTreeTestsWIP
        rebalanceTestsWIP
        appendPropertyTests
        insertPropertyTests
        removePropertyTests
        updatePropertyTests
        keepPropertyTests
        splitAndKeepPropertyTests
        manualTests
    ]
