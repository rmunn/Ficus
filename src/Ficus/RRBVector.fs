// Look for TOCHECK to find places where I'm unsure of the correct logic

/// Relaxed Radix Balanced Vector
///
/// Original concept: Phil Bagwell and Tiark Rompf
/// https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf
///
/// Partly based on work by Jean Niklas L'orange: http://hypirion.com/thesis

module rec Ficus.RRBVector

open System.Threading
open RRBArrayExtensions

module Literals =
    let [<Literal>] internal blockSizeShift = 5
    let [<Literal>] internal radixSearchErrorMax = 2  // Number of extra search steps to allow; 2 is a good balance
    let [<Literal>] internal eMaxPlusOne = 3  // radixSearchErrorMax + 1
    let [<Literal>] internal blockSize = 32  // 2 ** blockSizeShift
    let [<Literal>] internal blockIndexMask = 31  // 2 ** blockSizeShift - 1. Use with &&& to get the current level's index
    let [<Literal>] internal blockSizeMin = 31  // blockSize - (radixSearchErrorMax / 2).

[<StructuredFormatDisplay("{StringRepr}")>]
type Node(thread, array : obj[]) =
    let thread = thread
    new() = Node(ref null,Array.create Literals.blockSize null)
    with
        static member InCurrentThread() = Node(ref Thread.CurrentThread, Array.zeroCreate Literals.blockSize)
        member this.Array = array
        member this.Thread = thread
        member this.SetThread t = thread := t
        member this.StringRepr = if array.Length = 0 then sprintf "EmptyNode" else sprintf "FullNode(%A)" array

[<StructuredFormatDisplay("RRBNode({StringRepr})")>]
type RRBNode(thread, array, sizeTable : int[]) =
    inherit Node(thread, array)
    with
        static member InCurrentThread() = RRBNode(ref Thread.CurrentThread, Array.create Literals.blockSize null, Array.zeroCreate Literals.blockSize)
        member this.SizeTable = sizeTable
        member this.StringRepr = sprintf "sizeTable=%A,children=%A" sizeTable array

// Concepts:
//
// "Shift" - The height of any given node, multiplied by Literals.blockSizeShift.
//           Used to calculate indices and size tables efficiently.
// "Node index" - An index within one node's array. Should be between 0 and node.Array.Length - 1.
//                Sometimes called "local index".
// "Tree index" - An index into the subtree rooted at a given node. As you descend the tree, getting
//                closer to the leaf level, the tree index tends to get smaller. Eventually, at the
//                leaf level, the tree index will also be a node index (an index into that leaf node's array).
// "Vector index" - An index into the vector itself. If less then the tail offset, will be a tree index into the root node.
//                  Otherwise, the local index in the tail can be found by subtracting vecIdx - tailOffset.

// Other terms:
// Leaf - As you'd expect, the "tip" of the tree, where all the vector's contents are stored; shift = 0.
// Twig - The tree level just above the leaf level; shift = Literals.blockSizeShift
// (Successively higher levels of the tree could called, in order after twig: branch, limb, trunk...
// But we don't actually use those terms in the code, just "twig" and "leaf".)

module RRBHelpers =
    let mkNode array = Node(ref null, array)

    let isSameObj = LanguagePrimitives.PhysicalEquality

    let emptyNode<'T> = Node(ref null, [||])
    let inline isEmpty (node : Node) = node.Array.Length = 0

    let inline radixIndex shift treeIdx =
        (treeIdx >>> shift) &&& Literals.blockIndexMask

    let inline radixSearch curShift treeIdx (sizeTbl:int[]) =
        let mutable i = radixIndex curShift treeIdx
        while sizeTbl.[i] <= treeIdx do
            i <- i + 1
        i

    let radixIndexOrSearch shift treeIdx (node:Node) =
        match node with
        | :? RRBNode as rrbNode ->
            let localIdx = radixSearch shift treeIdx rrbNode.SizeTable
            let child = rrbNode.Array.[localIdx]
            let nextTreeIdx = if localIdx = 0 then treeIdx else treeIdx - rrbNode.SizeTable.[localIdx - 1]
            localIdx, child, nextTreeIdx
        | _ ->
            let localIdx = radixIndex shift treeIdx
            let child = node.Array.[localIdx]
            let antimask = ~~~(Literals.blockIndexMask <<< shift)
            let nextTreeIdx = treeIdx &&& antimask
            localIdx, child, nextTreeIdx

    let inline getChildNode localIdx (node:Node) = node.Array.[localIdx] :?> Node
    let inline getLastChildNode (node:Node) = node.Array |> Array.last :?> Node

    // Syntactic sugar for operations we'll use *all the time*: moving up and down the tree levels
    let inline down shift = shift - Literals.blockSizeShift
    let inline up shift = shift + Literals.blockSizeShift

    let rec getLeftmostTwig shift (node:Node) =
        if shift <= Literals.blockSizeShift then node
        else node |> getChildNode 0 |> getLeftmostTwig (down shift)

    let rec getRightmostTwig shift (node:Node) =
        if shift <= Literals.blockSizeShift then node
        else node |> getLastChildNode |> getRightmostTwig (down shift)

    // Find the leaf containing index `idx`, and the local index of `idx` in that leaf
    let rec getItemFromLeaf<'T> shift treeIdx node =
        let _, child, nextLvlIdx = radixIndexOrSearch shift treeIdx node
        if shift <= Literals.blockSizeShift then
            (child :?> 'T []).[nextLvlIdx]
        else
            getItemFromLeaf (down shift) nextLvlIdx (child :?> Node)

    let rec replaceItemAt shift treeIdx newItem (node:Node) =
        if shift <= 0 then
            node.Array |> Array.copyAndSet treeIdx newItem |> mkNode
        else
            let localIdx, child, nextIdx = radixIndexOrSearch shift treeIdx node
            let newNode = replaceItemAt (down shift) nextIdx newItem (child :?> Node)
            match node with
            | :? RRBNode as rrbNode -> RRBNode(rrbNode.Thread, rrbNode.Array |> Array.copyAndSet localIdx (box newNode), rrbNode.SizeTable) :> Node
            | fullNode -> Node(fullNode.Thread, fullNode.Array |> Array.copyAndSet localIdx (box newNode))

    let rec treeSize<'T> shift (node : obj) : int =
        if shift <= 0 then
            (node :?> 'T[]).Length
        else
            match (node :?> Node) with
            | :? RRBNode as n -> Array.last n.SizeTable
            | n ->
                // A full node is allowed to have an incomplete rightmost entry, but all but its rightmost entry must be complete.
                // Therefore, we can shortcut this calculation for most of the nodes, but we do need to calculate the rightmost node.
                if shift <= Literals.blockSizeShift then
                    ((n.Array.Length - 1) <<< shift) + ((Array.last n.Array) :?> 'T[]).Length
                else
                    ((n.Array.Length - 1) <<< shift) + treeSize (down shift) (Array.last n.Array)

    // Handy shorthand
    let inline nodeSize (node:obj) = (node :?> Node).Array.Length

    // slotCount and twigSlotCount are used in the rebalance function (and therefore in insertion, concatenation, etc.)
    let inline slotCount nodes = nodes |> Array.sumBy nodeSize
    // twigSlotCount is just a time-saving special case for nodes at the "twig" level (one level up from the leaves)
    let twigSlotCount (node : Node) =
        match node with
        | _ when node |> isEmpty -> 0
        | :? RRBNode as n -> (n.SizeTable |> Array.last)
        | n -> ((n.Array.Length - 1) <<< Literals.blockSizeShift) + nodeSize (Array.last n.Array)

    let createSizeTable<'T> shift (array:obj[]) =
        let sizeTbl = Array.zeroCreate array.Length
        let mutable total = 0
        for i = 0 to array.Length - 1 do
            total <- total + treeSize<'T> (down shift) (array.[i])
            sizeTbl.[i] <- total
        sizeTbl

    // TODO: Write some functions for updating one entry in an RRBNode, taking advantage of
    // the size table's properties so we don't have to recalculate the entire size table.
    // That might speed things up a bit. BUT... wait until we've finished implementing everything
    // and have measured performance, because we should not prematurely optimize.

    let isSizeTableFullAtShift shift sizeTbl =
        let len = Array.length sizeTbl
        if len <= 1 then true else
        let checkIdx = len - 2
        sizeTbl.[checkIdx] = ((checkIdx + 1) <<< shift)

    let mkRRBNodeWithSizeTable shift entries sizeTable =
        if isSizeTableFullAtShift shift sizeTable
        then Node(ref null, entries)
        else RRBNode(ref null, entries, sizeTable) :> Node

    let mkRRBNode<'T> shift entries = createSizeTable<'T> shift entries |> mkRRBNodeWithSizeTable shift entries

    let mkRRBNodeOrLeaf<'T> shift entries = if shift > 0 then mkRRBNode<'T> shift entries else mkNode entries
    let mkBoxedRRBNodeOrLeaf<'T> shift entries = box (mkRRBNodeOrLeaf<'T> shift entries)
    let mkNodeThenBox = mkNode >> box

    let newPath<'T> endShift node =
        let rec loop s node =
            if s >= endShift
            then node
            else let s' = (up s) in loop s' (mkRRBNode<'T> s' [|node|])
        loop Literals.blockSizeShift node  // TOCHECK: I'm pretty sure this is correct in the new "leaves are just 'T arrays" world, but let's double-check

    /// Used in replacing leaf nodes
    let copyAndAddNToSizeTable incIdx n oldST =
        let newST = Array.copy oldST
        for i = incIdx to oldST.Length - 1 do
            newST.[i] <- newST.[i] + n
        newST

    let inline copyAndSubtractNFromSizeTable decIdx n oldST =
        copyAndAddNToSizeTable decIdx (-n) oldST

    let inline fullNodeIsTrulyFull shift node =
        node |> isEmpty || shift < Literals.blockSizeShift || treeSize (down shift) (getLastChildNode node) >= (1 <<< (down shift))

    // Note: childSize should be *tree* size, not *node* size. In other words, something appropriate for the size table at this level.
    let replaceChildAt<'T> shift localIdx newChild childSize (n:Node) =
        match n with
        | :? RRBNode as n ->
            let oldSize = if localIdx = 0 then n.SizeTable.[0] else n.SizeTable.[localIdx] - n.SizeTable.[localIdx - 1]
            let sizeDiff = childSize - oldSize
            let entries' = n.Array |> Array.copyAndSet localIdx newChild
            let sizeTable' = if sizeDiff = 0 then n.SizeTable else n.SizeTable |> copyAndAddNToSizeTable localIdx sizeDiff
            mkRRBNodeWithSizeTable shift entries' sizeTable'
        | _ ->
            let childIsFull = childSize = (1 <<< shift)
            if childIsFull then
                // This is still a full node
                n.Array |> Array.copyAndSet localIdx newChild |> mkNode
            else
                // This has become an RRB node, so recalculate the size table via mkRRBNode
                n.Array |> Array.copyAndSet localIdx newChild |> mkRRBNode<'T> shift

    let appendChild<'T> shift newChild childSize (node:Node) =
        // Assumes that the node does *not* yet have blockSize children; verifying that is the job of the caller function
        match node with
        | :? RRBNode as n ->
            let entries' = n.Array |> Array.copyAndAppend newChild
            let lastSizeTableEntry = if n.SizeTable.Length = 0 then 0 else n.SizeTable |> Array.last
            let sizeTable' = n.SizeTable |> Array.copyAndAppend (childSize + lastSizeTableEntry)
            mkRRBNodeWithSizeTable shift entries' sizeTable'
        | _ ->
            // FullNodes are allowed to have their last item be non-full, so we have to check that
            if node |> fullNodeIsTrulyFull shift then
                node.Array |> Array.copyAndAppend newChild |> mkNode
            else
                node.Array |> Array.copyAndAppend newChild |> mkRRBNode<'T> shift

    let rec appendLeafWithoutGrowingTree shift (newLeaf : 'T[]) leafLen (rootNode : Node) =
        if shift <= Literals.blockSizeShift then
            if nodeSize rootNode >= Literals.blockSize then None else appendChild shift (box newLeaf) leafLen rootNode |> Some
        else
            match appendLeafWithoutGrowingTree (down shift) newLeaf leafLen (Array.last rootNode.Array :?> Node) with
            | Some result -> rootNode.Array |> Array.copyAndSetLast (box result) |> mkRRBNode<'T> shift |> Some  // Using replaceChildAt here turned out to be slower
            | None -> // Rightmost subtree was full
                if nodeSize rootNode >= Literals.blockSize then None else
                let newNode = newPath<'T> (down shift) (mkNode [|box newLeaf|])
                rootNode |> appendChild shift newNode leafLen |> Some

    let appendLeafWithGrowth shift (leaf:'T[]) (root:Node) =
        let leafLen = leaf.Length
        match appendLeafWithoutGrowingTree shift leaf leafLen root with
        | Some root' -> root', shift
        | None ->
            let left = root
            let right = newPath<'T> shift (mkNode [|box leaf|])   // TOCHECK: Make sure this is the right logic for the new "Leaves are just 'T arrays" world
            let higherShift = up shift
            match root with
            | :? RRBNode as n ->
                let oldSize = Array.last n.SizeTable
                mkRRBNodeWithSizeTable higherShift [|left; right|] [|oldSize; oldSize + leafLen|], higherShift
            | n ->
                mkNode [|left; right|], higherShift

    let pushTailDown shift (tail:'T[]) (root:Node) =
        appendLeafWithGrowth shift tail root

    // Mirror function to appendLeafWithoutGrowingTree
    let rec removeLastLeaf<'T> shift (root:Node) =
        if shift <= 0 then
            failwith "Deliberate failure at shift 0 or less"  // This proves that we're not actually using this code branch
            // TODO: Keep that deliberate failure line in here until we're *completely* finished with refactoring, in case we end up needing
            // to use removeLastLeaf for anything else. Then once we're *completely* done refactoring, delete this unnecessary if branch.
        elif shift <= Literals.blockSizeShift then
            // Children of this root node are leaves
            if root |> isEmpty then Array.empty, root else
            match root with
            | :? RRBNode as n ->
                let leaf = (Array.last n.Array) :?> 'T []
                let root' = mkRRBNodeWithSizeTable shift (Array.copyAndPop n.Array) (Array.copyAndPop n.SizeTable)
                leaf, root'
            | n ->
                let leaf = (Array.last n.Array) :?> 'T []
                let root' = Array.copyAndPop n.Array |> mkNode // Popping the last entry from a FullNode can't ever turn it into an RRBNode.
                leaf, root'
        else
            // Children are nodes, not leaves -- so we're going to take the rightmost and dig down into it.
            // And if the recursive call returns an empty node, we'll strip the entry off our node.
            let mkNewRoot = match root with
                            | :? RRBNode -> mkRRBNode<'T> shift
                            | _ -> mkNode
            let leaf, child' = removeLastLeaf (down shift) (Array.last root.Array :?> Node)
            let root' =
                if child' |> isEmpty
                then root.Array |> Array.copyAndPop |> mkNewRoot
                else root.Array |> Array.copyAndSetLast (box child') |> mkNewRoot
            leaf, root'

    let rec replaceLastLeaf shift (newLeaf:'T []) root =
        if shift <= 0 then
            failwith "Deliberate failure at shift 0 or less"  // This proves that we're not actually using this code branch
            // TODO: Keep that deliberate failure line in here until we're *completely* finished with refactoring, in case we end up needing
            // to use replaceLastLeaf for anything else. Then once we're *completely* done refactoring, delete this unnecessary if branch.
        elif shift <= Literals.blockSizeShift then
            root |> replaceChildAt shift (nodeSize root - 1) (box newLeaf) newLeaf.Length
        else
            let downShift = shift - Literals.blockSizeShift
            let lastIdx = (nodeSize root - 1)
            let newChild = root |> getChildNode lastIdx |> replaceLastLeaf downShift newLeaf
            root |> replaceChildAt shift lastIdx newChild (treeSize downShift newChild)

    // =================
    // Iteration helpers
    // =================

    // These iterate only the tree; the methods on the RRBVector class will include the tail in their interations

    let rec iterLeaves<'T> shift (arr:obj[]) : seq<'T []> =
        seq {
            if shift <= Literals.blockSizeShift then yield! (arr |> Seq.cast<'T []>)
            else
                for child in arr do
                    yield! iterLeaves (down shift) (child :?> Node).Array
        }

    let rec revIterLeaves<'T> shift (arr:obj[]) : seq<'T []> =
        seq {
            if shift <= Literals.blockSizeShift then
                for i = arr.Length - 1 downto 0 do
                    yield (arr.[i] :?> 'T [])
            else
                for i = arr.Length - 1 downto 0 do
                    yield! revIterLeaves (down shift) (arr.[i] :?> Node).Array
        }

    let seqSplitAt splitIdx (s:#seq<'a>) =
        let mutable i = 0
        let e = s.GetEnumerator()
        let head = seq { while i < splitIdx && e.MoveNext() do yield e.Current; i <- i + 1 }
        let tail = seq { while e.MoveNext() do yield e.Current }
        head, tail // NOTE: Make sure you enumerate the head sequence *first*, otherwise enumerating the tail sequence will not work correctly.

    let seqToArrayKnownSize size s =
        // Since there seems to be no built-in library function to turn a seq of known size into an array *directly*, here's one.
        let result = Array.zeroCreate size
        let mutable i = 0
        for x in s do
            result.[i] <- x
            i <- i + 1
        result

    // SPLIT / SLICE ALGORITHM

    // Terminology:  "left slice" = slice the tree and keep the  left half = "Take"
    //              "right slice" = slice the tree and keep the right half = "Skip"

    let rec leftSlice<'T> shift idx (node:Node) =
        if shift > 0 then
            let localIdx, child, nextIdx = radixIndexOrSearch shift idx node
            let child' = if nextIdx = 0 then (child :?> Node) else leftSlice (down shift) nextIdx (child :?> Node)
            let lastIdx = if nextIdx = 0 then localIdx - 1 else localIdx
            let items = node.Array.[..lastIdx]
            if nextIdx > 0 then
                items.[lastIdx] <- box child'
            match node with
            | :? RRBNode as tree ->
                let sizeTable = tree.SizeTable.[..lastIdx]
                if nextIdx > 0 then
                    let diff = treeSize (down shift) (child :?> Node) - treeSize (down shift) child'
                    sizeTable.[lastIdx] <- sizeTable.[lastIdx] - diff
                mkRRBNodeWithSizeTable shift items sizeTable
            | _ -> mkRRBNode<'T> shift items
        else
            let items = Array.sub node.Array 0 idx
            // There are no tree nodes at leaf level (shift = 0)
            mkNode items

    let rec rightSlice<'T> shift idx (node:Node) =
        if shift > 0 then
            let localIdx, child, nextIdx = radixIndexOrSearch shift idx node
            let items = node.Array.[localIdx..]
            let child' = if nextIdx = 0 then (child :?> Node) else rightSlice (down shift) nextIdx (child :?> Node)
            if nextIdx > 0 then
                items.[0] <- box child'
            match node with
            | :? RRBNode as tree ->
                let sizeTable = tree.SizeTable.[localIdx..]
                let diff = if nextIdx > 0 then sizeTable.[0] - treeSize (down shift) child'
                           elif localIdx > 0 then tree.SizeTable.[localIdx - 1]
                           else 0
                if diff > 0 then
                    for i = 0 to sizeTable.Length - 1 do
                        sizeTable.[i] <- sizeTable.[i] - diff
                mkRRBNodeWithSizeTable shift items sizeTable
            | _ -> mkRRBNode<'T> shift items
        else
            let items = node.Array.[idx..]
            // There are no tree nodes at leaf level (shift = 0)
            mkNode items


    // APPEND ALGORITHM

    // "Slide" items down in merging arrays, creating full arrays
    // I.e., if M = 4, a is [1;2;3;4] and b is [5;6;7] and aIdx is 2,
    // then the output will contain [3;4;5;6] and the new aIdx will be 2 because
    // the next node will start at b's 7.
    let slideItemsDown a aIdx b =
        let aLen = Array.length a
        let bLen = Array.length b
        let aCnt = aLen - aIdx
        if aCnt + bLen > Literals.blockSize then
            let bCnt = Literals.blockSize - aCnt
            let dest = Array.zeroCreate Literals.blockSize
            Array.blit a aIdx dest    0 aCnt
            Array.blit b    0 dest aCnt bCnt
            dest,bCnt
        else
            let dest = Array.zeroCreate (aCnt + bLen)
            Array.blit a aIdx dest    0 aCnt
            Array.blit b    0 dest aCnt bLen
            dest,-1   // -1 signals STOP to the merge algorithm

    // Rebalance a set of nodes of level `shift` (whose children therefore live at `shift - Literals.blockSizeShift`)

    let findStartIdxForRebalance (combined:obj[]) =
        let idx = combined |> Array.tryFindIndex (fun node -> nodeSize node < Literals.blockSizeMin)
        defaultArg idx -1

    let executeRebalance<'T> shift mergeStart (combined:obj[]) =
        // This function assumes you have a real mergeStart
        let destLen = Array.length combined - 1
        let dest = Array.zeroCreate destLen
        Array.blit combined 0 dest 0 mergeStart
        let mutable workIdx = mergeStart
        let mutable i = 0
        while i >= 0 && workIdx < destLen do
            let arr,newI = slideItemsDown (combined.[workIdx] :?> Node).Array i (combined.[workIdx + 1] :?> Node).Array
            dest.[workIdx] <- mkBoxedRRBNodeOrLeaf<'T> (down shift) arr
            i <- newI
            workIdx <- workIdx + 1
        Array.blit combined (workIdx+1) dest workIdx (destLen - workIdx)
        dest

    let rebalance<'T> shift (combined:obj[]) =
        if shift < Literals.blockSizeShift then invalidArg "shift" <| sprintf "rebalance should only be called at twig level (%d) or higher. It was instead called with shift=%d" Literals.blockSizeShift shift
        let mergeStart = findStartIdxForRebalance combined
        if mergeStart < 0 then combined else executeRebalance<'T> shift mergeStart combined

    // Minor optimization
    let splitAtBlockSize combined =
        if Array.length combined > Literals.blockSize then
            combined |> Array.splitAt Literals.blockSize
        else
            combined,[||]

    let inline rebalanceNeeded slotCount nodeCount =
        slotCount <= ((nodeCount - Literals.eMaxPlusOne) <<< Literals.blockSizeShift)

    let rebalanceNeeded1 nodes =
        let slots = slotCount nodes
        let nodeCount = Array.length nodes
        rebalanceNeeded slots nodeCount

    let rebalanceNeeded2 a b =
        let slots = slotCount a + slotCount b
        let nodeCount = Array.length a + Array.length b
        rebalanceNeeded slots nodeCount

    let rebalanceNeeded3 (a:Node) (tail:'T[]) (b:Node) =
        let nodeCount = a.Array.Length + 1 + b.Array.Length
        if nodeCount > 2 * Literals.blockSize
        then true // A+tail+B scenario, and we already know there'll be enough room for the tail's items in the merged+rebalanced node
        else
            let slots = twigSlotCount a + Array.length tail + twigSlotCount b
            rebalanceNeeded slots nodeCount

    let mergeArrays<'T> shift a b =
        if rebalanceNeeded2 a b then
            Array.append a b |> rebalance<'T> shift |> splitAtBlockSize
        else
            Array.append a b |> splitAtBlockSize

    let mergeWithTail shift (a:Node) (tail:'T[]) (b:Node) =
        // Will only be called if we can guarantee that there is room to merge the tail's items into either a or b.
        // TODO: Remove the following line before putting into production
        if shift <> Literals.blockSizeShift then invalidArg "shift" <| sprintf "mergeWithTail should only be called at twig level (%d). It was instead called with shift=%d" Literals.blockSizeShift shift
        if Array.isEmpty tail then mergeArrays<'T> shift a.Array b.Array else
        if rebalanceNeeded3 a tail b then
            Array.append3' a.Array (box tail) b.Array |> rebalance<'T> Literals.blockSizeShift |> splitAtBlockSize
        else
            Array.append3' a.Array (box tail) b.Array |> splitAtBlockSize

    let inline isThereRoomToMergeTheTail (aTwig:Node) (bTwig:Node) tailLength =
        // aTwig should be the rightmost twig of vector A
        // bTwig should be the  leftmost twig of vector B
        aTwig.Array.Length < Literals.blockSize
     || bTwig.Array.Length < Literals.blockSize
     || (twigSlotCount aTwig) + tailLength <= Literals.blockSize * (Literals.blockSize - 1)
     || (twigSlotCount bTwig) + tailLength <= Literals.blockSize * (Literals.blockSize - 1)

    let setOrRemoveFirstChild shift newChild parentArray =
        if newChild |> Array.isEmpty
        then parentArray |> Array.copyAndRemoveFirst
        else parentArray |> Array.copyAndSet 0 (mkRRBNode (down shift) newChild |> box)

    let setOrRemoveLastChild shift newChild parentArray =
        if newChild |> Array.isEmpty
        then parentArray |> Array.copyAndPop
        else parentArray |> Array.copyAndSetLast (mkRRBNode (down shift) newChild |> box)

    let rec mergeTree aShift (a:Node) bShift (b:Node) tail =
        if aShift <= Literals.blockSizeShift && bShift <= Literals.blockSizeShift then
            // At twig level on both nodes
            mergeWithTail aShift a tail b
        else
            if aShift < bShift then
                let aR, bL = mergeTree aShift a (down bShift) (b |> getChildNode 0) tail
                let a' = if aR |> Array.isEmpty then [||] else [|mkRRBNode (down bShift) aR |> box|]
                let b' = b.Array |> setOrRemoveFirstChild bShift bL
                mergeArrays bShift a' b'
            elif aShift > bShift then
                let aR, bL = mergeTree (down aShift) (Array.last a.Array :?> Node) bShift b tail
                let a' = a.Array |> setOrRemoveLastChild  aShift aR
                let b' = if bL |> Array.isEmpty then [||] else [|mkRRBNode (down aShift) bL |> box|]
                mergeArrays aShift a' b'
            else
                let aR,  bL  = Array.last a.Array :?> Node, b |> getChildNode 0
                let aR', bL' = mergeTree (down aShift) aR (down bShift) bL tail
                let a' = a.Array |> setOrRemoveLastChild  aShift aR'
                let b' = b.Array |> setOrRemoveFirstChild bShift bL'
                mergeArrays aShift a' b'

    // =========
    // INSERTION
    // =========

    type SlideResult<'a> =
        | SimpleInsertion of newCurrent : 'a
        | SlidItemsLeft of newLeft : 'a * newCurrent : 'a
        | SlidItemsRight of newCurrent : 'a * newRight : 'a
        | SplitNode of newCurrent : 'a * newRight : 'a

    let  (|LeftSibling|_|) (parentOpt, idx) = if idx <= 0 then None else parentOpt |> Option.map (fun p -> p |> getChildNode (idx-1))
    let (|RightSibling|_|) (parentOpt, idx) = parentOpt |> Option.bind (fun p -> if idx >= (nodeSize p)-1 then None else p |> getChildNode (idx+1) |> Some)

    let trySlideAndInsert<'T> localIdx itemToInsert (parentOpt : Node option) idxOfNodeInParent (node : Node) =
        if node.Array.Length < Literals.blockSize then
            SimpleInsertion (node.Array |> Array.copyAndInsertAt localIdx itemToInsert)
        else
            match (parentOpt, idxOfNodeInParent) with
            | LeftSibling sib when sib.Array.Length < Literals.blockSize ->
                SlidItemsLeft (Array.appendAndInsertAndSplitEvenly (localIdx + sib.Array.Length) itemToInsert sib.Array node.Array)
            | RightSibling sib when sib.Array.Length < Literals.blockSize ->
                SlidItemsRight (Array.appendAndInsertAndSplitEvenly localIdx itemToInsert node.Array sib.Array)
            | _ ->
                SplitNode (node.Array |> Array.insertAndSplitEvenly localIdx itemToInsert)

    let rec insertIntoTree shift thisLvlIdx (item : 'T) (parentOpt : Node option) idxOfNodeInParent (node : Node) =
        let localIdx, child, nextLvlIdx = radixIndexOrSearch shift thisLvlIdx node
        let arr = node.Array

        let insertResult = if shift > Literals.blockSizeShift
                           then insertIntoTree (down shift) nextLvlIdx item (Some node) localIdx (child :?> Node)
                           else trySlideAndInsert<'T>           nextLvlIdx item (Some node) localIdx (child :?> Node)
        match insertResult with

        | SimpleInsertion childItems' ->
            SimpleInsertion (arr |> Array.copyAndSet localIdx (mkBoxedRRBNodeOrLeaf<'T> (down shift) childItems'))

        | SlidItemsLeft (leftItems', childItems') ->
            let arr' = arr |> Array.copyAndSet localIdx (mkBoxedRRBNodeOrLeaf<'T> (down shift) childItems')
            arr'.[localIdx - 1] <- mkBoxedRRBNodeOrLeaf<'T> (down shift) leftItems'
            SimpleInsertion arr'

        | SlidItemsRight (childItems', rightItems') ->
            let arr' = arr |> Array.copyAndSet localIdx (mkBoxedRRBNodeOrLeaf (down shift) childItems')
            arr'.[localIdx + 1] <- mkBoxedRRBNodeOrLeaf<'T> (down shift) rightItems'
            SimpleInsertion arr'

        | SplitNode (childItems', newSiblingItems) ->
            let child' = mkBoxedRRBNodeOrLeaf<'T> (down shift) childItems'
            let newSibling = mkBoxedRRBNodeOrLeaf<'T> (down shift) newSiblingItems
            let overstuffedEntries = arr |> Array.copyAndInsertAt (localIdx + 1) newSibling
            overstuffedEntries.[localIdx] <- child'
            // TODO: Might be able to be clever with twigSlotCount if we know we're at the twig level
            if rebalanceNeeded1 overstuffedEntries then
                SimpleInsertion (rebalance<'T> shift overstuffedEntries)
            else
                let arr' = arr |> Array.copyAndSet localIdx (mkBoxedRRBNodeOrLeaf<'T> (down shift) childItems')
                trySlideAndInsert (localIdx + 1) newSibling parentOpt idxOfNodeInParent (mkNode arr')

    // ========
    // DELETION
    // ========

    let rec removeFromTree<'T> shift shouldCheckForRebalancing thisLvlIdx thisNode =
        let childIdx, childNode, nextLvlIdx = radixIndexOrSearch shift thisLvlIdx thisNode
        let result =
            if shift <= Literals.blockSizeShift
            then Array.copyAndRemoveAt nextLvlIdx (childNode :?> Node).Array |> mkNode
            else removeFromTree (down shift) shouldCheckForRebalancing nextLvlIdx (childNode :?> Node) |> mkRRBNode (down shift)
            // TODO: Can probably leverage mkRRBNodeWithSizeTable here for greater efficiency: subtract 1 from size table and we have it
        let resultLen = Array.length result.Array
        if resultLen <= 0 then
            // Child vanished
            Array.copyAndRemoveAt childIdx thisNode.Array
        elif resultLen < nodeSize childNode then
            // Child shrank: check if rebalance needed
            let slotCount' = slotCount thisNode.Array - 1
            if shouldCheckForRebalancing && rebalanceNeeded slotCount' (nodeSize thisNode) then
                Array.copyAndSet childIdx (box result) thisNode.Array |> rebalance<'T> shift
            else
                Array.copyAndSet childIdx (box result) thisNode.Array
        else
            // Child did not shrink
            Array.copyAndSet childIdx (box result) thisNode.Array

    // ==============
    // BUILDING TREES from various sources (arrays, sequences of known size, etc) in an efficient way
    // ==============

    let rec buildRoot shift nodes =
        let inputLen = Array.length nodes
        if inputLen <= Literals.blockSize then
            shift, nodes |> mkNode
        else
            let nodeCount = inputLen >>> Literals.blockSizeShift
            let leftovers = inputLen - (nodeCount <<< Literals.blockSizeShift)
            let resultLen = if leftovers <= 0 then nodeCount else nodeCount + 1
            let result = Array.zeroCreate resultLen
            for i=0 to nodeCount - 1 do
                result.[i] <- Array.sub nodes (i <<< Literals.blockSizeShift) Literals.blockSize |> mkNode |> box
            if leftovers > 0 then
                result.[nodeCount] <- Array.sub nodes (nodeCount <<< Literals.blockSizeShift) leftovers |> mkNode |> box
            buildRoot (up shift) result

    let buildTree (items : 'T[]) =
        let itemsLen = Array.length items
        if itemsLen <= Literals.blockSize then
            RRBSapling<'T>(itemsLen, 0, Array.empty, items, 0) :> RRBVector<'T>
        elif itemsLen <= Literals.blockSize * 2 then
            let root,tail = items |> Array.splitAt Literals.blockSize
            RRBSapling<'T>(itemsLen, 0, root, tail, Literals.blockSize) :> RRBVector<'T>
        else
            let twigCount = (itemsLen - 1) >>> Literals.blockSizeShift
            let tailOffset = twigCount <<< Literals.blockSizeShift
            let remaining = itemsLen - tailOffset
            let twigArray = Array.zeroCreate twigCount
            for i=0 to twigCount-1 do
                twigArray.[i] <- Array.sub items (i <<< Literals.blockSizeShift) Literals.blockSize |> box
            let tail = Array.sub items tailOffset remaining
            let shift, root = buildRoot Literals.blockSizeShift twigArray
            RRBTree<'T>(itemsLen, shift, root, tail, tailOffset) :> RRBVector<'T>

    let rec buildRootOfSeqWithKnownSize (shift : int) (children : seq<obj>) (inputLen : int) =
        if inputLen <= Literals.blockSize then
            shift, children |> seqToArrayKnownSize inputLen |> mkNode
        else
            let nodeCount = inputLen >>> Literals.blockSizeShift
            let leftovers = inputLen - (nodeCount <<< Literals.blockSizeShift)
            let resultLen = if leftovers <= 0 then nodeCount else nodeCount + 1
            let result = children |> Seq.chunkBySize Literals.blockSize |> Seq.map mkNodeThenBox
            buildRootOfSeqWithKnownSize (up shift) result resultLen

    let buildTreeOfSeqWithKnownSize itemsLen (items : seq<'T>) =
        if itemsLen <= Literals.blockSize then
            RRBSapling<'T>(itemsLen, 0, Array.empty, items |> seqToArrayKnownSize itemsLen, 0) :> RRBVector<'T>
        elif itemsLen <= Literals.blockSize * 2 then
            let rootSeq, tailSeq = items |> seqSplitAt Literals.blockSize
            let root = rootSeq |> seqToArrayKnownSize Literals.blockSize
            let tail = tailSeq |> seqToArrayKnownSize (itemsLen - Literals.blockSize)
            RRBSapling<'T>(itemsLen, 0, root, tail, 0) :> RRBVector<'T>
        else
            let leafCount = (itemsLen - 1) >>> Literals.blockSizeShift
            let tailOffset = leafCount <<< Literals.blockSizeShift
            let tailCount = itemsLen - tailOffset
            let treeItems, tailItems = items |> seqSplitAt tailOffset
            let leaves = treeItems |> Seq.chunkBySize Literals.blockSize |> Seq.map box
            let shift, root = buildRootOfSeqWithKnownSize Literals.blockSizeShift leaves (tailOffset >>> Literals.blockSizeShift)
            RRBTree<'T>(itemsLen, shift, root, tailItems |> seqToArrayKnownSize tailCount, tailOffset) :> RRBVector<'T>

    // Helper function for RRBVector.Append (optimized construction of vector from two "saplings" - root+tail vectors)
    let buildTreeFromTwoSaplings (aRoot : 'T[]) (aTail : 'T[]) (bRoot : 'T[]) (bTail : 'T[]) =
        if aRoot.Length = Literals.blockSize then
            // Can reuse the first node, but not the rest
            let items = Array.append3 aTail bRoot bTail
            let len = items.Length
            if len <= Literals.blockSize then
                RRBSapling<'T>(len + Literals.blockSize, 0, aRoot, items, Literals.blockSize) :> RRBVector<'T>
            elif len <= Literals.blockSize * 2 then
                let newRoot = mkNode [|box aRoot; Array.sub items 0 Literals.blockSize |> box|]
                RRBTree<'T>(len + Literals.blockSize, Literals.blockSizeShift, newRoot, items.[Literals.blockSize..], Literals.blockSize * 2) :> RRBVector<'T>
            else
                let newRoot = mkNode [|box aRoot
                                       Array.sub items 0 Literals.blockSize |> box
                                       Array.sub items Literals.blockSize Literals.blockSize |> box|]
                RRBTree<'T>(len + Literals.blockSize, Literals.blockSizeShift, newRoot, items.[Literals.blockSize*2..], Literals.blockSize * 3) :> RRBVector<'T>
        else
            // Can't reuse any nodes
            let items = Array.append4 aRoot aTail bRoot bTail
            let len = items.Length
            if len <= Literals.blockSize then
                RRBSapling<'T>(len, 0, Array.empty, items, 0) :> RRBVector<'T>
            elif len <= Literals.blockSize * 2 then
                let root', tail' = items |> Array.splitAt Literals.blockSize
                RRBSapling<'T>(len, 0, root', tail', Literals.blockSize) :> RRBVector<'T>
            elif len <= Literals.blockSize * 3 then
                let newRoot = mkNode [|box (Array.sub items 0 Literals.blockSize)
                                       box (Array.sub items Literals.blockSize Literals.blockSize)|]
                RRBTree<'T>(len, Literals.blockSizeShift, newRoot, items.[Literals.blockSize*2..], Literals.blockSize * 2) :> RRBVector<'T>
            else
                let newRoot = mkNode [|box (Array.sub items 0 Literals.blockSize)
                                       box (Array.sub items Literals.blockSize Literals.blockSize)
                                       box (Array.sub items (Literals.blockSize * 2) Literals.blockSize)|]
                RRBTree<'T>(len, Literals.blockSizeShift, newRoot, items.[Literals.blockSize*3..], Literals.blockSize * 3) :> RRBVector<'T>

    // ==============
    // TREE-ADJUSTING functions (shortenTree, etc.)
    // ==============

    // Any operation that might have changed the tree height will call one of these (usually adjustTree) to make sure the tree is still well-formed
    // Invariant we need to keep at all times: rightmost leaf is full if its parent is full. This is called "the invariant" in comments elsewhere.
    // By keeping this invariant, the most common operation (adding one item at the end of the vector, a.k.a. "push") can be O(1) since it can always
    // safely push a full tail down into the tree to become its newest leaf. The cost is that other functions have to beware: when pushing a tail down
    // that might have NOT been full, we have to first try to make it full (by adding items to it from other nodes). If that's not an option, then we
    // have to post-process the tree by shifting some nodes from the new tail into the new rightmost leaf.


[<AbstractClass>]
[<StructuredFormatDisplay("{StringRepr}")>]
type RRBVector<'T>() =
    abstract member Empty : RRBVector<'T>  // Or maybe it should be unit -> RRBVector<'T>
    abstract member IsEmpty : unit -> bool
    abstract member StringRepr : string
    abstract member Length : int
    abstract member IterLeaves : unit -> seq<'T []>
    abstract member RevIterLeaves : unit -> seq<'T []>
    abstract member IterItems : unit -> seq<'T>
    abstract member RevIterItems : unit -> seq<'T>
    // abstract member GetEnumerator : unit -> IEnumerator<'T>
    abstract member Push : 'T -> RRBVector<'T>
    abstract member Peek : unit -> 'T
    abstract member Pop : unit -> RRBVector<'T>
    abstract member Take : int -> RRBVector<'T>
    abstract member Skip : int -> RRBVector<'T>
    abstract member Split : int -> RRBVector<'T> * RRBVector<'T>
    abstract member Slice : int * int -> RRBVector<'T>
    abstract member GetSlice : int option * int option -> RRBVector<'T>
    abstract member Append : RRBVector<'T> -> RRBVector<'T>
    abstract member Insert : int -> 'T -> RRBVector<'T>
    abstract member Remove : int -> RRBVector<'T>
    abstract member Update : int -> 'T -> RRBVector<'T>
    abstract member GetItem : int -> 'T

    interface System.Collections.Generic.IEnumerable<'T> with
        member this.GetEnumerator () = this.IterItems().GetEnumerator()

    interface System.Collections.IEnumerable with
        member this.GetEnumerator () = (this.IterItems().GetEnumerator()) :> System.Collections.IEnumerator

    member this.Item with get idx = this.GetItem idx

type internal IRRBInternal<'T> =
    abstract member InsertIntoTail : int -> 'T -> RRBVector<'T>
    abstract member RemoveFromTailAtTailIdx : int -> RRBVector<'T>
    abstract member RemoveImpl : bool -> int -> RRBVector<'T>
    abstract member RemoveWithoutRebalance : int -> RRBVector<'T>

type RRBSapling<'T> internal (count, shift : int, root : 'T [], tail : 'T [], tailOffset : int)  =
    inherit RRBVector<'T>()
    // A "sapling" is a short tree, with shift = 0 (e.g., no more than two leaves, one in the root and one in the tail)
    let hashCode = ref None
    static member EmptyTree : RRBSapling<'T> = RRBSapling<'T>(0,0,Array.empty,Array.empty,0)

    override this.GetHashCode() =
        // This MUST follow the same algorithm as the GetHashCode() method from PersistentVector so that the Equals() logic will be valid
        match !hashCode with
        | None ->
            let mutable hash = 1
            for x in this.IterItems() do
                hash <- 31 * hash + Unchecked.hash x
            hashCode := Some hash
            hash
        | Some hash -> hash

    override this.Equals(other) =
        match other with
        | :? RRBSapling<'T> as other ->
            if this.Length <> other.Length then false else
            if this.GetHashCode() <> other.GetHashCode() then false else
            Seq.forall2 (Unchecked.equals) (this.IterItems()) (other.IterItems())
        | _ -> false

    override this.ToString() =
        sprintf "RRBSapling<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" count shift tailOffset root tail

    override this.StringRepr = this.ToString()

    member internal this.Shift = shift
    member internal this.Root = root
    member internal this.Tail = tail
    member internal this.TailOffset = tailOffset

    override this.Empty = RRBSapling<'T>(0,0,Array.empty,Array.empty,0) :> RRBVector<'T>
    override this.IsEmpty() = count = 0
    override this.Length = count
    override this.IterLeaves() = if root.Length = 0 then Seq.singleton tail else seq { yield root; yield tail }
    override this.RevIterLeaves() = if root.Length = 0 then Seq.singleton tail else seq { yield tail; yield root }
    override this.IterItems() = seq { yield! root; yield! tail }
    override this.RevIterItems() = seq {
        for i = tail.Length - 1 downto 0 do
            yield tail.[i]
        for i = root.Length - 1 downto 0 do
            yield root.[i] }

    override this.Push item =
        let tailLen = count - tailOffset
        if tailLen < Literals.blockSize then
            // Easy: just add new item in tail and we're done
            RRBSapling<'T>(count + 1, shift, root, tail |> Array.copyAndAppend item, tailOffset) :> RRBVector<'T>
        elif root.Length < Literals.blockSize then
            // While we're building a new root anyway, let's ensure that it's a full array
            let root' = Array.zeroCreate Literals.blockSize
            let rootLen = root.Length
            let extraRootItems = Literals.blockSize - rootLen
            Array.blit root 0 root' 0 rootLen
            Array.blit tail 0 root' rootLen extraRootItems
            let tailLenMinusExtra = tailLen - extraRootItems
            let tail' = Array.zeroCreate (tailLenMinusExtra + 1)
            Array.blit tail extraRootItems tail' 0 tailLenMinusExtra
            tail'.[tailLenMinusExtra] <- item
            RRBSapling<'T>(count + 1, shift, root', tail', Literals.blockSize) :> RRBVector<'T>
        else
            // Full root and full tail means we're upgrading to a full tree
            let treeRoot = Node(ref null, [|box root; box tail|])
            RRBTree<'T>(count + 1, Literals.blockSizeShift, treeRoot, [|item|], Literals.blockSize * 2) :> RRBVector<'T>

    override this.Peek() = if count = 0 then failwith "Can't peek an empty vector" else tail |> Array.last

    override this.Pop() =
        if count <= 0 then invalidOp "Can't pop from an empty vector" else
        if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
        if count - tailOffset > 1 then
            RRBSapling<'T>(count - 1, shift, root, Array.copyAndPop tail, tailOffset) :> RRBVector<'T>
        else
            RRBSapling<'T>(count - 1, shift, Array.empty, root, 0) :> RRBVector<'T>

    override this.Take takeCount =
        let newLen = Operators.min count takeCount
        if takeCount < tailOffset then
            RRBSapling<'T>(newLen, 0, Array.empty, root |> Array.truncate newLen, 0) :> RRBVector<'T>
        elif takeCount = tailOffset then
            RRBSapling<'T>(newLen, 0, Array.empty, root, 0) :> RRBVector<'T>
        else
            RRBSapling<'T>(newLen, 0, root, tail |> Array.truncate (newLen - tailOffset), tailOffset) :> RRBVector<'T>

    override this.Skip skipCount =
        if skipCount < tailOffset then
            // TODO: Should we rebalance the root here so that it's a full blockSize items? Answer: No, that invariant isn't needed for saplings
            RRBSapling<'T>(count - skipCount, 0, root |> Array.skip skipCount, tail, tailOffset - skipCount) :> RRBVector<'T>
        elif skipCount = tailOffset then
            RRBSapling<'T>(count - skipCount, 0, Array.empty, tail, 0) :> RRBVector<'T>
        else
            RRBSapling<'T>(count - skipCount, 0, Array.empty, tail |> Array.skip (skipCount - tailOffset), 0) :> RRBVector<'T>

    override this.Split splitIdx = this.Take splitIdx, this.Skip splitIdx

    override this.Slice (fromIdx, toIdx) =
        // TODO: Turn this into a single operation, which should be a little bit more efficient
        (this.Skip fromIdx).Take (toIdx - fromIdx + 1)

    override this.GetSlice (fromIdx, toIdx) =
        // TODO: Turn this into a single operation, which should be a little bit more efficient
        let step1 = match fromIdx with
                    | None -> this :> RRBVector<'T>
                    | Some idx -> this.Skip idx
        let step2 = match toIdx with
                    | None -> step1
                    | Some idx -> step1.Take (idx - (defaultArg fromIdx 0) + 1)
        step2

    override this.Append otherVec =
        if count = 0 then otherVec
        elif otherVec.Length = 0 then this :> RRBVector<'T>
        else
            // This will be complicated. TOCHECHECK: Test thoroughly since I'm not certain I got it all
            match otherVec with
            | :? RRBSapling<'T> as b ->
                if b.Root.Length = 0 then
                    if tail.Length + b.Tail.Length <= Literals.blockSize then
                        RRBSapling<'T>(count + b.Length, shift, root, Array.append tail b.Tail, tailOffset) :> RRBVector<'T>
                    elif root.Length = 0 then
                        // Two tail-only vectors can always turn into a single sapling
                        let root', tail' = Array.appendAndSplitAt Literals.blockSize tail b.Tail
                        RRBSapling<'T>(count + b.Length, shift, root', tail', Literals.blockSize) :> RRBVector<'T>
                    else
                        RRBHelpers.buildTreeFromTwoSaplings root tail b.Root b.Tail
                else
                    RRBHelpers.buildTreeFromTwoSaplings root tail b.Root b.Tail
            | :? RRBTree<'T> as b ->
                // Left side sapling, right side full tree. Convert left side to a "full" tree before appending
                let treeRoot = RRBHelpers.mkRRBNode Literals.blockSizeShift [|box root|]
                // Occasionally, we can end up with a vector that needs adjusting (e.g., because the tail that was pushed down was short, so the invariant is no longer true).
                match RRBHelpers.mergeTree Literals.blockSizeShift treeRoot b.Shift b.Root tail with
                | [||], rootItems
                | rootItems, [||] -> RRBTree<'T>(count + b.Length, b.Shift, RRBHelpers.mkRRBNode b.Shift rootItems, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>
                | rootItemsL, rootItemsR ->
                    let rootL = RRBHelpers.mkRRBNode b.Shift rootItemsL
                    let rootR = RRBHelpers.mkRRBNode b.Shift rootItemsR
                    let newShift = RRBHelpers.up b.Shift
                    let newRoot = RRBHelpers.mkRRBNode newShift [|box rootL; box rootR|]
                    RRBTree<'T>(count + b.Length, newShift, newRoot, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>

    override this.Insert idx item =
        let idx = if idx < 0 then idx + count else idx
        if idx > count then
            invalidArg "idx" "Tried to insert past the end of the vector"
        elif idx = count then
            this.Push item
        elif idx >= tailOffset then
            (this :> IRRBInternal<'T>).InsertIntoTail (idx - tailOffset) item
        else
            // We already know the root is not empty, otherwise we would have hit "idx >= tailOffset" just above
            if root.Length < Literals.blockSize then
                let root' = root |> Array.copyAndInsertAt idx item
                RRBSapling<'T>(count + 1, 0, root', tail, tailOffset + 1) :> RRBVector<'T>
            elif tail.Length < Literals.blockSize then
                let newRootItems = root |> Array.copyAndInsertAt idx item
                let root' = Array.sub newRootItems 0 Literals.blockSize
                let tail' = tail |> Array.copyAndInsertAt 0 (Array.last newRootItems)
                RRBSapling<'T>(count + 1, 0, root', tail', tailOffset) :> RRBVector<'T>
            else
                // Full root and full tail at shift 0: handle this case specially for efficiency's sake
                let newItems = Array.appendAndInsertAt idx item root tail
                let a = newItems.[..Literals.blockSize-1]
                let b = newItems.[Literals.blockSize..Literals.blockSize*2-1]
                let tail' = newItems.[Literals.blockSize*2..]
                let root' = RRBHelpers.mkNode [|box a; box b|]
                RRBTree<'T>(count + 1, Literals.blockSizeShift, root', tail', Literals.blockSize*2) :> RRBVector<'T>

    override this.Remove idx = (this :> IRRBInternal<'T>).RemoveImpl true idx

    override this.Update idx newItem =
        let idx = if idx < 0 then idx + count else idx
        if idx >= count then
            invalidArg "idx" "Tried to update item past the end of the vector"
        if idx >= tailOffset then
            let newTail = tail |> Array.copyAndSet (idx - tailOffset) newItem
            RRBSapling<'T>(count, shift, root, newTail, tailOffset) :> RRBVector<'T>
        else
            let newRoot = root |> Array.copyAndSet idx newItem
            RRBSapling<'T>(count, shift, newRoot, tail, tailOffset) :> RRBVector<'T>

    override this.GetItem idx =
        if idx < tailOffset
        then root.[idx]
        else tail.[idx - tailOffset]

    interface IRRBInternal<'T> with

        member this.InsertIntoTail tailIdx item =
            if count - tailOffset < Literals.blockSize then
                // Tail isn't full yet: easy
                RRBSapling<'T>(count + 1, shift, root, tail |> Array.copyAndInsertAt tailIdx item, tailOffset) :> RRBVector<'T>
            elif count + 1 < Literals.blockSize * 2 then
                // Short root, so rebalance to make a full root so we don't have to do that again next time
                // TODO: Write an array extension function to do this in a single step
                let fatTail = tail |> Array.copyAndInsertAt tailIdx item
                let root', tail' = Array.appendAndSplitAt Literals.blockSize root fatTail
                RRBSapling<'T>(count + 1, shift, root', tail', tailOffset) :> RRBVector<'T>
            else
                // Full root and tail
                RRBHelpers.buildTreeFromTwoSaplings root tail [|item|] Array.empty

        member this.RemoveFromTailAtTailIdx idx =
            if count <= 0 then invalidOp "Can't remove from an empty vector" else
            if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
            if count - tailOffset > 1 then
                RRBSapling<'T>(count - 1, shift, root, Array.copyAndRemoveAt idx tail, tailOffset) :> RRBVector<'T>
            else
                RRBSapling<'T>(count - 1, shift, Array.empty, root, 0) :> RRBVector<'T>

        member this.RemoveImpl _ idx =
            // RRBSaplings don't need the shouldCheckForRebalancing parameter
            let idx = if idx < 0 then idx + count else idx
            if idx >= count then
                invalidArg "idx" "Tried to remove past the end of the vector"
            elif count = 0 then
                failwith "Can't remove from an empty vector"
            elif idx >= tailOffset then
                (this :> IRRBInternal<'T>).RemoveFromTailAtTailIdx(idx - tailOffset)
            else
                RRBSapling<'T>(count - 1, 0, root |> Array.copyAndRemoveAt idx, tail, tailOffset - 1) :> RRBVector<'T>

        member this.RemoveWithoutRebalance idx = (this :> IRRBInternal<'T>).RemoveImpl false idx // Will be used in RRBVector.windowed implementation

    // override this.Update : int -> 'T -> RRBVector<'T>
    // override this.Item : int -> 'T


and [<StructuredFormatDisplay("{StringRepr}")>] RRBTree<'T> internal (count, shift : int, root : Node, tail : 'T [], tailOffset : int)  =
    inherit RRBVector<'T>()
    let hashCode = ref None
    // static member Empty() : RRBTree<'T> = RRBTree<'T>(0,0,RRBHelpers.emptyNode,Array.empty,0)
    override this.Empty = RRBSapling<'T>(0,0,Array.empty,Array.empty,0) :> RRBVector<'T>
    override this.IsEmpty() = count = 0

    new (vecLen,shift:int,root:Node,tail:'T[]) =
        let tailLen = Array.length tail
        RRBTree<'T>(vecLen, shift, root, tail, vecLen - tailLen)

    override this.GetHashCode() =
        // This MUST follow the same algorithm as the GetHashCode() method from PersistentVector so that the Equals() logic will be valid
        match !hashCode with
        | None ->
            let mutable hash = 1
            for x in this do
                hash <- 31 * hash + Unchecked.hash x
            hashCode := Some hash
            hash
        | Some hash -> hash

    override this.Equals(other) =
        match other with
        | :? RRBTree<'T> as other ->
            if this.Length <> other.Length then false else
            if this.GetHashCode() <> other.GetHashCode() then false else
            Seq.forall2 (Unchecked.equals) this other
        | _ -> false

    override this.ToString() =
        sprintf "RRBVector<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" count shift tailOffset root tail

    override this.StringRepr = this.ToString()
    member internal this.Shift = shift
    member internal this.Root = root
    member internal this.Tail = tail
    member internal this.TailOffset = tailOffset

    interface System.Collections.Generic.IEnumerable<'T> with
        member this.GetEnumerator() = this.IterItems().GetEnumerator()

    interface System.Collections.IEnumerable with
        member this.GetEnumerator() = this.IterItems().GetEnumerator() :> System.Collections.IEnumerator

    override this.Length = count

    override this.IterLeaves() =
        seq { yield! RRBHelpers.iterLeaves shift root.Array
              yield tail }

    override this.RevIterLeaves() =
        seq { yield tail
              yield! RRBHelpers.revIterLeaves shift root.Array }

    override this.IterItems() = seq { for arr in this.IterLeaves() do yield! arr }

    override this.RevIterItems() =
        seq {
            for arr in this.RevIterLeaves() do
                for i = (arr.Length - 1) downto 0 do
                    yield arr.[i]
        }

    override this.Push item =
        if count - tailOffset < Literals.blockSize then
            // Easy: just add new item in tail and we're done
            RRBTree<'T>(count + 1, shift, root, tail |> Array.copyAndAppend item) :> RRBVector<'T>
        else
            let root', shift' = RRBHelpers.pushTailDown shift tail root  // This does all the work
            RRBTree<'T>(count + 1, shift', root', [|item|]) :> RRBVector<'T>

    override this.Peek() =
        if count = 0
        then failwith "Can't peek an empty vector"
        else tail |> Array.last

    override this.Pop() =
        if count <= 0 then invalidOp "Can't pop from an empty vector" else
        if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
        if count - tailOffset > 1 then
            RRBTree<'T>(count - 1, shift, root, Array.copyAndPop tail) :> RRBVector<'T>
        else
            RRBTree<'T>.promoteTail shift root (count - 1) :> RRBVector<'T>

    override this.Take takeCount =
        if takeCount >= count then this :> RRBVector<'T>
        elif takeCount <= 0 then RRBSapling<'T>.EmptyTree :> RRBVector<'T>   // TODO: Decide whether a negative count in Take() should be an exception or not
        elif takeCount > tailOffset then
            let tailCount = takeCount - tailOffset
            RRBTree<'T>(takeCount, shift, root, Array.sub tail 0 tailCount, tailOffset) :> RRBVector<'T>
        elif takeCount = tailOffset then
            // We don't have to slice into the tree at all: just promote new tail as if we'd just popped the last item from the old tail
            RRBTree<'T>.promoteTail shift root takeCount :> RRBVector<'T>
        else
            let newRoot = RRBHelpers.leftSlice shift takeCount root
            RRBTree<'T>.promoteTail shift newRoot takeCount :> RRBVector<'T>

    override this.Skip skipCount =
        if skipCount >= count then RRBSapling<'T>.EmptyTree :> RRBVector<'T>
        elif skipCount <= 0 then this :> RRBVector<'T>   // TODO: Decide whether a negative count in Skip() should be an exception or not
        elif skipCount > tailOffset then
            let tailStart = skipCount - tailOffset
            RRBTree<'T>(count - skipCount, 0, RRBHelpers.emptyNode, tail.[tailStart..], 0) :> RRBVector<'T>
        elif skipCount = tailOffset then
            RRBTree<'T>(count - skipCount, 0, RRBHelpers.emptyNode, tail, 0) :> RRBVector<'T>
        else
            let newRoot = RRBHelpers.rightSlice shift skipCount root
            RRBTree<'T>(count - skipCount, shift, newRoot, tail, tailOffset - skipCount).AdjustTree() // :> RRBVector<'T>

    override this.Split splitIdx =
        this.Take splitIdx, this.Skip splitIdx

    override this.Slice (fromIdx, toIdx) =
        (this.Skip fromIdx).Take (toIdx - fromIdx + 1)

    // Slice notation vec.[from..to] turns into a this.GetSlice call with option indices
    override this.GetSlice (fromIdx, toIdx) =
        let step1 = match fromIdx with
                    | None -> this :> RRBVector<'T>
                    | Some idx -> this.Skip idx
        let step2 = match toIdx with
                    | None -> step1
                    | Some idx -> step1.Take (idx - (defaultArg fromIdx 0) + 1)
        step2

    override this.Append (otherVec : RRBVector<'T>) =  // Appends vector B at the end of this vector
        if count = 0 then otherVec
        elif otherVec.Length = 0 then this :> RRBVector<'T>
        else
            // This will be complicated. TOCHECK: Test thoroughly since I'm not certain I got it all
            match otherVec with
            | :? RRBSapling<'T> as b ->
                if b.Root.Length = 0 then
                    if tail.Length + b.Tail.Length <= Literals.blockSize then
                        RRBTree<'T>(count + b.Length, shift, root, Array.append tail b.Tail, tailOffset) :> RRBVector<'T>
                    else
                        let newLeaf, newTail = Array.appendAndSplitAt Literals.blockSize tail b.Tail
                        let newRoot, newShift = RRBHelpers.pushTailDown shift newLeaf root
                        RRBTree<'T>(count + b.Length, newShift, newRoot, newTail) :> RRBVector<'T>
                else
                    let lastTwig = RRBHelpers.getRightmostTwig shift root
                    let tLen = tail.Length
                    if (lastTwig :? RRBNode && lastTwig.Array.Length < Literals.blockSize) || tLen = Literals.blockSize then
                        // Can safely push tail without messing up the invariant
                        let tempRoot, tempShift = root |> RRBHelpers.pushTailDown shift tail
                        let newRoot, newShift = tempRoot |> RRBHelpers.appendLeafWithGrowth tempShift b.Root
                        RRBTree<'T>(count + b.Length, newShift, newRoot, b.Tail) :> RRBVector<'T>
                    else
                        // Have to shift some nodes, so there's no advantage in keeping the right tree's root and tail intact.
                        let items = Array.append3 tail b.Root b.Tail
                        let len = items.Length
                        if len <= Literals.blockSize then
                            RRBTree<'T>(count + b.Length, shift, root, items, tailOffset) :> RRBVector<'T>
                        elif len <= Literals.blockSize * 2 then
                            let newLeaf, newTail = items |> Array.splitAt Literals.blockSize
                            let newRoot, newShift = root |> RRBHelpers.pushTailDown shift newLeaf
                            RRBTree<'T>(count + b.Length, newShift, newRoot, newTail) :> RRBVector<'T>
                        else
                            let newLeaf1, rest = items |> Array.splitAt Literals.blockSize
                            let newLeaf2, newTail = rest |> Array.splitAt Literals.blockSize
                            let tempRoot, tempShift = root |> RRBHelpers.pushTailDown shift newLeaf1
                            let newRoot, newShift = tempRoot |> RRBHelpers.pushTailDown tempShift newLeaf2
                            RRBTree<'T>(count + b.Length, newShift, newRoot, newTail) :> RRBVector<'T>
            | :? RRBTree<'T> as b ->
                let aRoot, aShift = root, shift
                let aTwig = RRBHelpers.getRightmostTwig shift root
                let bTwig = RRBHelpers.getLeftmostTwig b.Shift b.Root
                let aRoot, aShift, aTail =
                    if RRBHelpers.isThereRoomToMergeTheTail aTwig bTwig tail.Length then
                        root, shift, tail
                    else
                        // Push a's tail down, then merge the resulting tree
                        let thisRootAfterPush, shift' = RRBHelpers.pushTailDown shift tail root
                        thisRootAfterPush, shift', [||]
                let higherShift = max aShift b.Shift
                // Occasionally, we can end up with a vector that needs adjusting (e.g., because the tail that was pushed down was short, so the invariant is no longer true).
                match RRBHelpers.mergeTree aShift aRoot b.Shift b.Root aTail with
                | [||], rootItems
                | rootItems, [||] -> RRBTree<'T>(count + b.Length, higherShift, RRBHelpers.mkRRBNode higherShift rootItems, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>
                | rootItemsL, rootItemsR ->
                    let rootL = RRBHelpers.mkRRBNode higherShift rootItemsL
                    let rootR = RRBHelpers.mkRRBNode higherShift rootItemsR
                    let newShift = RRBHelpers.up higherShift
                    let newRoot = RRBHelpers.mkRRBNode newShift [|box rootL; box rootR|]
                    RRBTree<'T>(count + b.Length, newShift, newRoot, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>

    override this.Insert idx (item : 'T) =
        let idx = if idx < 0 then idx + count else idx
        if idx > count then
            invalidArg "idx" "Tried to insert past the end of the vector"
        elif idx = count then
            this.Push item
        elif idx >= tailOffset then
            (this :> IRRBInternal<'T>).InsertIntoTail (idx - tailOffset) item
        else
            match RRBHelpers.insertIntoTree shift idx item None 0 root with
            | RRBHelpers.SlideResult.SimpleInsertion newRootItems ->
                RRBTree<'T>(count + 1, shift, RRBHelpers.mkRRBNode<'T> shift newRootItems, tail, tailOffset + 1).AdjustTree() // :> RRBVector<'T>
            | RRBHelpers.SlideResult.SlidItemsLeft (l,r)
            | RRBHelpers.SlideResult.SlidItemsRight (l,r)
            | RRBHelpers.SlideResult.SplitNode (l,r) ->
                let a = RRBHelpers.mkRRBNode<'T> shift l
                let b = RRBHelpers.mkRRBNode<'T> shift r
                RRBTree<'T>(count + 1, (RRBHelpers.up shift), RRBHelpers.mkRRBNode (RRBHelpers.up shift) [|a;b|], tail, tailOffset + 1).AdjustTree() // :> RRBVector<'T>

    member internal this.AdjustTree() =
        let shorter : RRBVector<'T> = this.ShortenTree()
        match shorter with
        | :? RRBTree<'T> as tree -> tree.ShiftNodesFromTailIfNeeded()
        | _ -> shorter // No need to shift nodes on a sapling

    member internal this.ShortenTree() =
        // TODO: Convert to sapling if shift hits 0
        if shift <= 0 then this :> RRBVector<'T>
        else
            if root.Array.Length > 1 then this :> RRBVector<'T>
            elif root.Array.Length = 0 then RRBTree<'T>(count, 0, root, tail, tailOffset) :> RRBVector<'T>
            else RRBTree<'T>(count, RRBHelpers.down shift, (root |> RRBHelpers.getChildNode 0), tail, tailOffset).ShortenTree()

    member internal this.ShiftNodesFromTailIfNeeded() =
        // Minor optimization: save most-often-needed thistor properties in locals so we don't hit property accesses over and over
        let shift = this.Shift
        let root : Node = this.Root
        if shift < Literals.blockSizeShift then
            // Can't happen now that we've split RRBTrees and RRBSaplings
            this :> RRBVector<'T>
            // let len = root.Array.Length
            // if len = 0 || len = Literals.blockSize then this else
            // let items = Array.append root.Array this.Tail
            // if items.Length <= Literals.blockSize then
            //     RRBTree<'T>(this.Length, 0, emptyNode, items, 0)
            // else
            //     let root', tail' = items |> Array.splitAt Literals.blockSize
            //     RRBTree<'T>(this.Length, 0, mkNode root', tail', Literals.blockSize)
        else
            let lastTwig = RRBHelpers.getRightmostTwig shift root
            match lastTwig with
            | :? RRBNode -> this :> RRBVector<'T>  // Only need to shift nodes from tail if the last twig was a full node
            | _ ->
                let tail = this.Tail
                let lastLeaf = (Array.last lastTwig.Array) :?> 'T []
                let shiftCount = Literals.blockSize - lastLeaf.Length
                if shiftCount = 0 then
                    this :> RRBVector<'T>
                elif shiftCount >= tail.Length then
                    // Would shift everything out of the tail, so instead we'll promote a new tail
                    let lastLeaf, root' = root |> RRBHelpers.removeLastLeaf shift
                    let tail' = tail |> Array.append lastLeaf
                    // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
                    RRBTree<'T>(this.Length, shift, root', tail').AdjustTree()
                else
                    let nodesToShift, tail' = tail |> Array.splitAt shiftCount
                    let lastLeaf' = Array.append lastLeaf nodesToShift
                    let root' = root |> RRBHelpers.replaceLastLeaf shift lastLeaf'
                    RRBTree<'T>(this.Length, shift, root', tail') :> RRBVector<'T>

    static member internal promoteTail shift root newLength =
        let newTail, newRoot = RRBHelpers.removeLastLeaf shift root
        RRBTree<'T>(newLength, shift, newRoot, newTail).AdjustTree()

    interface IRRBInternal<'T> with

        member this.InsertIntoTail tailIdx (item : 'T) =
            if count - tailOffset < Literals.blockSize then
                // Easy: just add new item in tail and we're done
                RRBTree<'T>(count + 1, shift, root, tail |> Array.copyAndInsertAt tailIdx item) :> RRBVector<'T>
            else
                let tailItemsToPush, remaining =
                    tail
                    |> Array.copyAndInsertAt tailIdx item
                    |> Array.splitAt Literals.blockSize
                let root', shift' = RRBHelpers.pushTailDown shift tailItemsToPush root  // This does all the work
                RRBTree<'T>(count + 1, shift', root', remaining) :> RRBVector<'T>

        member this.RemoveFromTailAtTailIdx idx =
            if count <= 0 then invalidOp "Can't remove from an empty vector" else
            if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
            if count - tailOffset > 1 then
                RRBTree<'T>(count - 1, shift, root, Array.copyAndRemoveAt idx tail) :> RRBVector<'T>
            else
                RRBTree<'T>.promoteTail shift root (count - 1) // :> RRBVector<'T>

        member this.RemoveImpl shouldCheckForRebalancing idx =
            let idx = if idx < 0 then idx + count else idx
            if idx >= count then
                invalidArg "idx" "Tried to remove past the end of the vector"
            elif count = 0 then
                failwith "Can't remove from an empty vector"
            elif idx >= tailOffset then
                (this :> IRRBInternal<'T>).RemoveFromTailAtTailIdx(idx - tailOffset)
            elif shift = 0 then
                let newRoot = root.Array |> Array.copyAndRemoveAt idx |> RRBHelpers.mkNode
                RRBTree<'T>(count - 1, 0, newRoot, tail, tailOffset - 1).ShortenTree() // :> RRBVector<'T>  // No need for adjustTree at 0 shift
            else
                let newRoot = RRBHelpers.removeFromTree shift shouldCheckForRebalancing idx root |> RRBHelpers.mkRRBNode<'T> shift
                RRBTree<'T>(count - 1, shift, newRoot, tail, tailOffset - 1).AdjustTree() // :> RRBVector<'T>

        member this.RemoveWithoutRebalance idx = (this :> IRRBInternal<'T>).RemoveImpl false idx // Will be used in RRBVector.windowed implementation

    override this.Remove idx = (this :> IRRBInternal<'T>).RemoveImpl true idx

    override this.Update idx newItem =
        let idx = if idx < 0 then idx + count else idx
        if idx >= count then
            invalidArg "idx" "Tried to update item past the end of the vector"
        if idx >= tailOffset then
            let newTail = tail |> Array.copyAndSet (idx - tailOffset) newItem
            RRBTree<'T>(count, shift, root, newTail, tailOffset) :> RRBVector<'T>
        else
            let newRoot = root |> RRBHelpers.replaceItemAt shift idx newItem
            RRBTree<'T>(count, shift, newRoot, tail, tailOffset) :> RRBVector<'T>

    override this.GetItem idx =
        if idx < tailOffset
        then RRBHelpers.getItemFromLeaf shift idx root
        else tail.[idx - tailOffset]


// BASIC API

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RRBVector =
    let inline nth idx (vec : RRBVector<'T>) = vec.[idx]
    let inline peek (vec : RRBVector<'T>) = vec.Peek()
    let inline pop (vec : RRBVector<'T>) = vec.Pop()
    let inline push (item : 'T) (vec : RRBVector<'T>) = vec.Push item
    let inline append (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) = vec1.Append(vec2)
    let inline split idx (vec : RRBVector<'T>) = vec.Split idx
    let inline remove idx (vec : RRBVector<'T>) = vec.Remove idx
    let inline insert idx (item : 'T) (vec : RRBVector<'T>) = vec.Insert idx item

    let internal flip f a b = f b a
    let internal flip3 f a b c = f b c a

    let toArray (vec : RRBVector<'T>) =
        let result : 'T[] = Array.zeroCreate vec.Length
        let mutable i = 0
        for leaf in vec.IterLeaves() do
            leaf.CopyTo(result, i)
            i <- i + leaf.Length
        result

    let inline toSeq (vec : RRBVector<'T>) = vec :> System.Collections.Generic.IEnumerable<'T>
    let inline toList (vec : RRBVector<'T>) = vec |> List.ofSeq
    let inline ofArray (a : 'T[]) = RRBHelpers.buildTree a
    let inline ofSeq (s : seq<'T>) = s |> Array.ofSeq |> RRBHelpers.buildTree  // TODO: Implement transient vectors so this can be more efficient
    let inline ofList (l : 'T list) = l |> Seq.ofList |> ofSeq
    let inline ofPersistentVector (vec : PersistentVector<'T>) = vec |> ofSeq  // TODO: Get rid of this and replace its uses with transients
    // TODO: Try improving average and averageBy by using iterLeafArrays(), summing up each array, and then dividing by count at the end. MIGHT be faster than Seq.average.
    let inline average (vec : RRBVector<'T>) = vec |> toSeq |> Seq.average
    let inline averageBy f (vec : RRBVector<'T>) = vec |> toSeq |> Seq.averageBy f
    let choose (chooser : 'T -> 'U option) (vec : RRBVector<'T>) : RRBVector<'U> =
        let mutable transient = TransientVector<'U>()
        for item in vec do
            match chooser item with
            | None -> ()
            | Some value -> transient <- transient.conj value
        transient.persistent() |> ofPersistentVector

    let chunkBySize chunkSize (vec : RRBVector<'T>) =
        let mutable transient = TransientVector()
        let mutable remaining = vec
        while remaining.Length > 0 do
            transient <- transient.conj (remaining.Take chunkSize)
            remaining <- remaining.Skip chunkSize
        transient.persistent() |> ofPersistentVector

    let concat (vecs : seq<RRBVector<'T>>) =
        // TODO: Implement transient RRBVectors so this will be faster (no need to build and throw away so many intermediate result vectors)
        let mutable result = RRBSapling<'T>.EmptyTree :> RRBVector<'T>
        for vec in vecs do
            result <- result.Append vec
        result

    let inline collect (f : 'T -> RRBVector<'T>) (vec : RRBVector<'T>) = vec |> Seq.map f |> concat
    let inline compareWith f (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) = (vec1, vec2) ||> Seq.compareWith f
    let inline countBy f (vec : RRBVector<'T>) = vec |> toArray |> Array.countBy f |> ofArray  // TODO: Measure whether this is faster than using Seq.countBy
    let inline contains item (vec : RRBVector<'T>) = vec |> Seq.contains item
    let inline distinct (vec : RRBVector<'T>) = vec |> Seq.distinct |> ofSeq
    let inline distinctBy f (vec : RRBVector<'T>) = vec |> Seq.distinctBy f |> ofSeq
    let inline empty<'T> = RRBSapling<'T>.EmptyTree :> RRBVector<'T>
    let exactlyOne (vec : RRBVector<'T>) =
        if vec.Length <> 1 then invalidArg "vec" <| sprintf "exactlyOne called on a vector of %d items (requires a vector of exactly 1 item)" vec.Length
        match vec with
        | :? RRBSapling<'T> as vec -> vec.Tail.[0]
        | :? RRBTree<'T> as vec -> vec.Tail.[0]
    let inline exists f (vec : RRBVector<'T>) = vec |> Seq.exists f
    let inline exists2 f (vec1 : RRBVector<'T>) (vec2 : RRBVector<'U>) = (vec1, vec2) ||> Seq.exists2 f
    let filter pred (vec : RRBVector<'T>) =
        let mutable transient = TransientVector<'T>()
        for item in vec do
            if pred item then transient <- transient.conj item
        transient.persistent() |> ofPersistentVector
    let except (vec : RRBVector<'T>) (excludedVec : RRBVector<'T>) =
        let excludedSet = System.Collections.Generic.HashSet<'T>(excludedVec)
        let mutable transient = TransientVector<'T>()
        for item in vec do
            if not (excludedSet.Contains item) then transient <- transient.conj item
        transient.persistent() |> ofPersistentVector
    let inline find f (vec : RRBVector<'T>) = vec |> Seq.find f
    let inline findBack f (vec : RRBVector<'T>) = vec.RevIterItems() |> Seq.find f
    let inline findIndex f (vec : RRBVector<'T>) = vec |> Seq.findIndex f
    let inline findIndexBack f (vec : RRBVector<'T>) = vec.RevIterItems() |> Seq.findIndex f
    let inline fold folder (initState : 'State) (vec : RRBVector<'T>) = vec |> Seq.fold folder initState
    let inline fold2 folder (initState : 'State) (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1, vec2) ||> Seq.fold2 folder initState
    let inline foldBack (initState : 'State) folder (vec : RRBVector<'T>) = vec.RevIterItems() |> Seq.fold (fun a b -> folder b a) initState
    let inline foldBack2 (initState : 'State) folder (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1.RevIterItems(), vec2.RevIterItems()) ||> Seq.fold2 (fun a b c -> folder b c a) initState
    let inline forall f (vec : RRBVector<'T>) = vec |> Seq.forall f
    let inline forall2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1, vec2) ||> Seq.forall2 f
    let inline groupBy f (vec : RRBVector<'T>) = vec |> Seq.groupBy f |> ofSeq
    let head (vec : RRBVector<'T>) =
        if vec.Length = 0 then invalidArg "vec" "Can't get head of empty vector"
        vec.[0]
    let inline indexed (vec : RRBVector<'T>) = vec |> Seq.indexed |> RRBHelpers.buildTreeOfSeqWithKnownSize vec.Length
    let inline init size f = Seq.init size f |> RRBHelpers.buildTreeOfSeqWithKnownSize size
    let inline isEmpty (vec : RRBVector<'T>) = vec.Length = 0
    let inline item idx (vec : RRBVector<'T>) = vec.[idx]
    let inline iter f (vec : RRBVector<'T>) = vec |> Seq.iter f
    let inline iter2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1, vec2) ||> Seq.iter2 f
    let inline iteri f (vec : RRBVector<'T>) = vec |> Seq.iteri f
    let inline iteri2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1, vec2) ||> Seq.iteri2 f
    let inline last (vec : RRBVector<'T>) =
        if vec.Length = 0 then invalidArg "vec" "Can't get last item of empty vector"
        vec.[vec.Length - 1]
    let inline length (vec : RRBVector<'T>) = vec.Length
    let inline map f (vec : RRBVector<'T>) = vec |> Seq.map f |> RRBHelpers.buildTreeOfSeqWithKnownSize vec.Length
    let inline map2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        let minLength = Operators.min vec1.Length vec2.Length
        (vec1, vec2) ||> Seq.map2 f |> RRBHelpers.buildTreeOfSeqWithKnownSize minLength
    let inline map3 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) (vec3 : RRBVector<'T3>) =
        let minLength = Operators.min (Operators.min vec1.Length vec2.Length) vec3.Length
        (vec1, vec2, vec3) |||> Seq.map3 f |> RRBHelpers.buildTreeOfSeqWithKnownSize minLength
    let inline mapi f (vec : RRBVector<'T>) = vec |> Seq.mapi f |> RRBHelpers.buildTreeOfSeqWithKnownSize vec.Length
    let inline mapi2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        let minLength = Operators.min vec1.Length vec2.Length
        (vec1, vec2) ||> Seq.mapi2 f |> RRBHelpers.buildTreeOfSeqWithKnownSize minLength

    let mapFold folder initState (vec : RRBVector<'T>) =
        if isEmpty vec then empty<'T> else
        let mutable transient = TransientVector<'T>()
        let mutable state = initState
        for item in vec do
            let item',state' = folder state item
            transient <- transient.conj item'
            state <- state'
        transient.persistent() |> ofPersistentVector

    let mapFoldBack folder (vec : RRBVector<'T>) initState =
        if isEmpty vec then empty<'T> else
        let mutable transient = TransientVector<'T>()
        let mutable state = initState
        for item in vec.RevIterItems() do
            let item',state' = folder item state
            transient <- transient.conj item'
            state <- state'
        transient.persistent() |> ofPersistentVector

    let inline max (vec : RRBVector<'T>) = vec |> Seq.max
    let inline maxBy f (vec : RRBVector<'T>) = vec |> Seq.maxBy f
    let inline min (vec : RRBVector<'T>) = vec |> Seq.min
    let inline minBy f (vec : RRBVector<'T>) = vec |> Seq.minBy f
    let inline pairwise (vec : RRBVector<'T>) = vec |> Seq.pairwise |> RRBHelpers.buildTreeOfSeqWithKnownSize (Operators.max 0 (vec.Length - 1)) :> RRBVector<_>
    let partition pred (vec : RRBVector<'T>) =
        let mutable trueItems = TransientVector<'T>()
        let mutable falseItems = TransientVector<'T>()
        for item in vec do
            if pred item then trueItems <- trueItems.conj item else falseItems <- falseItems.conj item
        trueItems.persistent() |> ofPersistentVector, falseItems.persistent() |> ofPersistentVector

    let permute f (vec : RRBVector<'T>) = // TODO: Implement a better version once we have transient RRBVectors, so we don't have to build an intermediate array
        let arr = Array.zeroCreate vec.Length
        let items = (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()
        let mutable i = 0
        while items.MoveNext() do
            arr.[f i] <- items.Current
            i <- i + 1
        arr |> ofArray

    let inline pick f (vec : RRBVector<'T>) = vec |> Seq.pick f
    let inline reduce f (vec : RRBVector<'T>) = vec |> Seq.reduce f
    let reduceBack f (vec : RRBVector<'T>) = let f' = flip f in vec.RevIterItems() |> Seq.reduce f'
    let inline replicate count value (vec : RRBVector<'T>) = // TODO: Implement this better once we have transient RRBVectors (or once we can do updates on transient PersistentVectors)
        Array.create count value |> ofArray
    let inline rev (vec : RRBVector<'T>) =  vec.RevIterItems() |> RRBHelpers.buildTreeOfSeqWithKnownSize vec.Length
    let inline scan f initState (vec : RRBVector<'T>) = vec |> Seq.scan f initState |> RRBHelpers.buildTreeOfSeqWithKnownSize (vec.Length + 1)
    let scanBack initState f (vec : RRBVector<'T>) = let f' = flip f in vec.RevIterItems() |> Seq.scan f' initState |> RRBHelpers.buildTreeOfSeqWithKnownSize (vec.Length + 1)
    // TODO: Why does this cause a type error?
    // let singleton (item : 'T) = RRBSapling<'T>(1, 0, Array.empty<'T>, Array.singleton item, 0) :> RRBVector<'T>
    let inline skip count (vec : RRBVector<'T>) = vec.Skip count
    let skipWhile pred (vec : RRBVector<'T>) = // TODO: Test this
        let rec loop pred n =
            if n >= vec.Length then empty<'T>
            elif pred vec.[n] then loop pred (n+1)
            else vec.Skip n
        loop pred 0
    let sort (vec : RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlace arr
        arr |> ofArray
    let sortBy f (vec : RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlaceBy f arr
        arr |> ofArray
    let sortWith f (vec : RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlaceWith f arr
        arr |> ofArray
    let sortDescending (vec : RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlace arr
        System.Array.Reverse arr  // Reverse in-place, more efficient than Array.rev since it doesn't create an intermediate array
        arr |> ofArray
    let sortByDescending f (vec : RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlaceBy f arr
        System.Array.Reverse arr  // Reverse in-place, more efficient than Array.rev since it doesn't create an intermediate array
        arr |> ofArray
    let inline splitAt idx (vec : RRBVector<'T>) = vec.Split idx
    let inline splitInto splitCount (vec : RRBVector<'T>) = chunkBySize (vec.Length / splitCount) vec  // TODO: Test that splits have the expected size
    let inline sum (vec : RRBVector<'T>) = vec |> Seq.sum
    let inline sumBy f (vec : RRBVector<'T>) = vec |> Seq.sumBy f
    let inline tail (vec : RRBVector<'T>) = vec.Remove 0
    let inline take n (vec : RRBVector<'T>) =
        if n > vec.Length then invalidArg "n" <| sprintf "Cannot take more items than a vector's length. Tried to take %d items from a vector of length %d" n vec.Length
        vec.Take n
    let inline takeWhile pred (vec : RRBVector<'T>) =  // TODO: Try a version with vec.IterItems() and a counter, and see if that runs faster. Also update skipWhile if it does.
        let rec loop pred n =
            if n >= vec.Length then vec
            elif pred vec.[n] then loop pred (n+1)
            else vec.Take n
        loop pred 0
    let inline truncate n (vec : RRBVector<'T>) = vec.Take n
    let inline tryFind f (vec : RRBVector<'T>) = vec |> Seq.tryFind f
    let inline tryFindBack f (vec : RRBVector<'T>) = vec.RevIterItems() |> Seq.tryFindBack f
    let inline tryFindIndex f (vec : RRBVector<'T>) = vec |> Seq.tryFindIndex f
    let inline tryFindIndexBack f (vec : RRBVector<'T>) = vec.RevIterItems() |> Seq.tryFindIndexBack f
    let tryHead (vec : RRBVector<'T>) = if vec.Length = 0 then None else Some vec.[0]
    let inline tryItem idx (vec : RRBVector<'T>) =
        let idx = if idx < 0 then idx + vec.Length else idx
        if idx < 0 || idx >= vec.Length then None else Some vec.[idx]
    let inline tryLast (vec : RRBVector<'T>) =
        if vec.Length = 0 then None else Some (last vec)
    let inline tryPick f (vec : RRBVector<'T>) = vec |> Seq.tryPick f
    let inline unfold f initState = Seq.unfold f initState |> ofSeq
    let unzip (vec : RRBVector<'T1 * 'T2>) =
        let mutable vec1 = TransientVector<'T1>()
        let mutable vec2 = TransientVector<'T2>()
        for a, b in vec do
            vec1 <- vec1.conj a
            vec2 <- vec2.conj b
        vec1.persistent() |> ofPersistentVector, vec2.persistent() |> ofPersistentVector
    let unzip3 (vec : RRBVector<'T1 * 'T2 * 'T3>) =
        let mutable vec1 = TransientVector<'T1>()
        let mutable vec2 = TransientVector<'T2>()
        let mutable vec3 = TransientVector<'T3>()
        for a, b, c in vec do
            vec1 <- vec1.conj a
            vec2 <- vec2.conj b
            vec3 <- vec3.conj c
        vec1.persistent() |> ofPersistentVector, vec2.persistent() |> ofPersistentVector, vec3.persistent() |> ofPersistentVector
    let inline where pred (vec : RRBVector<'T>) = filter pred vec
    // TODO: Why does this cause a type error?
(* Disabled until I can figure out why this causes a "Type parameter would escape its scope" error
    let windowed windowSize (vec : RRBVector<'T>) =
        if windowSize <= Literals.blockSize then
            // Sequence of tail-only vectors
            seq {
                let mutable tail = Array.zeroCreate windowSize
                let mutable count = 0
                let itemEnumerator = (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()
                while count < windowSize && itemEnumerator.MoveNext() do
                    tail.[count] <- itemEnumerator.Current
                    count <- count + 1
                yield RRBSapling<'T>(windowSize, 0, Array.empty, tail, windowSize) :> RRBVector<'T>
                while itemEnumerator.MoveNext() do
                    tail <- tail |> Array.popFirstAndPush itemEnumerator.Current
                    yield RRBSapling<'T>(windowSize, 0, Array.empty, tail, windowSize) :> RRBVector<'T>
            }
        elif windowSize <= Literals.blockSize * 2 then
            // Sequence of root+tail vectors (always-full root)
            seq {
                let items = Array.zeroCreate windowSize
                let mutable count = 0
                let itemEnumerator = (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()
                while count < windowSize && itemEnumerator.MoveNext() do
                    items.[count] <- itemEnumerator.Current
                    count <- count + 1
                let mutable root, tail = items |> Array.splitAt Literals.blockSize
                yield RRBSapling<'T>(windowSize, 0, root, tail, Literals.blockSize) :> RRBVector<'T>
                while itemEnumerator.MoveNext() do
                    root <- root |> Array.popFirstAndPush tail.[0]
                    tail <- tail |> Array.popFirstAndPush itemEnumerator.Current
                    yield RRBSapling<'T>(windowSize, 0, root, tail, Literals.blockSize) :> RRBVector<'T>
            }
        else
            // Sequence of vectors that share as much structure as possible with the original vector and with each other
            seq {
                let mutable slidingVec = vec.Take windowSize
                let rest = vec.Skip windowSize
                yield slidingVec
                for item in rest do
                    slidingVec <- (slidingVec :?> RRBTree<'T> :> IRRBInternal<'T>).RemoveWithoutRebalance 0
                    slidingVec <- slidingVec.Push item
                    yield slidingVec
            }
*)
    let inline zip (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        let minLength = Operators.min vec1.Length vec2.Length
        (vec1, vec2) ||> Seq.zip |> RRBHelpers.buildTreeOfSeqWithKnownSize minLength
    let inline zip3 (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) (vec3 : RRBVector<'T3>) =
        let minLength = Operators.min (Operators.min vec1.Length vec2.Length) vec3.Length
        (vec1, vec2, vec3) |||> Seq.zip3 |> RRBHelpers.buildTreeOfSeqWithKnownSize minLength
