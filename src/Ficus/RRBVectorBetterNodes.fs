module Ficus.RRBVectorBetterNodes

open RRBArrayExtensions

module Literals =
    let [<Literal>] internal blockSizeShift = 5
    let [<Literal>] internal radixSearchErrorMax = 2  // Number of extra search steps to allow; 2 is a good balance
    let [<Literal>] internal eMaxPlusOne = 3  // radixSearchErrorMax + 1
    let [<Literal>] internal blockSize = 32  // 2 ** blockSizeShift
    let [<Literal>] internal blockIndexMask = 31  // 2 ** blockSizeShift - 1. Use with &&& to get the current level's index
    let [<Literal>] internal blockSizeMin = 31  // blockSize - (radixSearchErrorMax / 2).

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

module RRBMath =
    let inline radixIndex (shift : int) (treeIdx : int) : int =
        (treeIdx >>> shift) &&& Literals.blockIndexMask

    // Syntactic sugar for operations we'll use *all the time*: moving up and down the tree levels
    let inline down shift = shift - Literals.blockSizeShift
    let inline up shift = shift + Literals.blockSizeShift

    // This takes a `len` parameter that should be the size of the size table, so that it can handle expanded nodes
    let isSizeTableFullAtShift shift (sizeTbl : int[]) len =
        if len <= 1 then true else
        let checkIdx = len - 2
        sizeTbl.[checkIdx] = ((checkIdx + 1) <<< shift)

    // Used in replacing leaf nodes
    let copyAndAddNToSizeTable incIdx n oldST =
        let newST = Array.copy oldST
        for i = incIdx to oldST.Length - 1 do
            newST.[i] <- newST.[i] + n
        newST

    // TODO: Is this actually used? If not, get rid of it
    let inline copyAndSubtractNFromSizeTable decIdx n oldST =
        copyAndAddNToSizeTable decIdx (-n) oldST

open RRBMath
open Ficus

// Used in insertion logic. TODO: Once I nail down the method name, add it here
type SlideResult<'a> =
    | SimpleInsertion of newCurrent : 'a
    | SlidItemsLeft of newLeft : 'a * newCurrent : 'a
    | SlidItemsRight of newCurrent : 'a * newRight : 'a
    | SplitNode of newCurrent : 'a * newRight : 'a

// RRBVector uses an opaque "owner token", implemented internally as a string ref, for determining whether a node
// can be safely updated in-place or not. A node created by a "normal" vector has no owner and thus cannot ever be
// updated in-place. Since each use of `ref null` would create a separate object on the heap, we'll avoid that by
// having a singleton that all non-transient nodes will use as their owner token. When a new owner token is needed,
// we create a new ref, which will be allocated on the heap. Its value doesn't matter, so we have it reference the
// empty string so that the minimum possible number of objects will live on the heap.

type OwnerToken = string ref

let nullOwner : OwnerToken = ref null

let mkOwnerToken() = ref ""

// Summary of type hierarchy of nodes
//
// type RRBNode<'T> =
//     | RRBFullNode of children : Node<'T>[]
//         with subclass RRBExpandedFullNode of Node<'T>[] * realSize : int
//     | RRBRelaxedNode of children : Node<'T>[] * sizeTable : int[]
//         with subclass RRBExpandedRelaxedNode of Node<'T>[] * sizeTable : int[] * realSize : int
//     | RRBLeafNode of items : 'T[]
//         with subclass RRBExpandedLeafNode of items : 'T[] * realSize : int
//
// The file is organized with the "compact" nodes together, and the "expanded" nodes together in a lower section.

[<AbstractClass; AllowNullLiteral>]
type RRBNode<'T>(ownerToken : OwnerToken) =
    member val Owner = ownerToken with get, set

    abstract member Shrink : OwnerToken -> RRBNode<'T>
    abstract member Expand : OwnerToken -> RRBNode<'T>

    abstract member NodeSize : int          // How many children does this single node have?
    abstract member TreeSize : int -> int   // How many total items are found in this node's entire descendant tree?
    abstract member SlotCount : int         // Used in rebalancing; the "slot count" is the total of the node sizes of this node's children
    abstract member TwigSlotCount : int     // Like SlotCount, but used when we *know* this node is a twig and its children are leaves, which allows some optimizations

    abstract member GetEditableNode : OwnerToken -> RRBNode<'T>
    abstract member GetEditableEmptyNodeOfLengthN : OwnerToken -> int -> RRBNode<'T>

    member this.IsEditableBy (owner : OwnerToken) =
        LanguagePrimitives.PhysicalEquality owner ownerToken && not (isNull !ownerToken)
        // Note that this test is NOT "if owner = owner".

    static member CreateSizeTable (shift : int) (array:RRBNode<'T>[]) : int[] =
        let sizeTable = Array.zeroCreate array.Length
        let mutable total = 0
        for i = 0 to array.Length - 1 do
            total <- total + (array.[i]).TreeSize (down shift)
            sizeTable.[i] <- total
        sizeTable

    static member MkLeaf (owner : OwnerToken) (items : 'T[]) = RRBLeafNode<'T>(owner, items)
    static member MkNode (owner : OwnerToken) (shift : int) (children : RRBNode<'T>[]) =
        RRBRelaxedNode<'T>.Create(owner, shift, children)
    static member MkNodeKnownSize (owner : OwnerToken) (shift : int) (children : RRBNode<'T>[]) (sizeTable : int[]) =
        RRBRelaxedNode<'T>.CreateWithSizeTable(owner, shift, children, sizeTable)
    static member MkFullNode (owner : OwnerToken) (children : RRBNode<'T>[]) =
        // if children.Length = 1 then SingletonNode<'T>(owner, entries) :> Node<'T> else  // TODO: Do we want an RRBSingletonNode class as well?
        RRBFullNode<'T>.Create(owner, children)

    abstract member UpdatedTree : OwnerToken -> int -> int -> 'T -> RRBNode<'T>  // Params: owner shift treeIdx newItem
    abstract member InsertedTree : OwnerToken -> int -> int -> 'T -> RRBFullNode<'T> option -> int -> SlideResult<RRBNode<'T>>  // Params: owner shift treeIdx (item : 'T) (parentOpt : Node option) idxOfNodeInParent



and RRBFullNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[]) =
    inherit RRBNode<'T>(ownerToken)

    static member Create (ownerToken : OwnerToken, children : RRBNode<'T>[]) = RRBFullNode<'T>(ownerToken, children)

    member this.Children = children

    member this.FirstChild = this.Children.[0]
    member this.LastChild  = this.Children.[this.NodeSize - 1]

    override this.NodeSize = children.Length
    override this.TreeSize shift =
        // A full node is allowed to have an incomplete rightmost entry, but all but its rightmost entry must be complete.
        // Therefore, we can shortcut this calculation for most of the nodes, but we do need to calculate the rightmost node.
        // TODO: Remove the "deliberate failure" check once we go to production
        if this.NodeSize = 0 then
            failwith "TreeSize called on an empty node; shouldn't happen"
            0
        else
            ((this.NodeSize - 1) <<< shift) + this.LastChild.TreeSize (down shift)
    override this.SlotCount = children |> Array.sumBy (fun child -> child.NodeSize)
    override this.TwigSlotCount =
        // Just as with TreeSize, we can skip calculating all but the rightmost node
        if this.NodeSize = 0 then 0 else ((this.NodeSize - 1) <<< Literals.blockSizeShift) + this.LastChild.NodeSize

    member this.FullNodeIsTrulyFull shift =
        this.NodeSize = 0 || this.LastChild.TreeSize (down shift) >= (1 <<< shift)

    // TODO: Should GetEditableNode return the base class? Or should it return the derived class, and therefore be non-inherited?
    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBFullNode<'T>(owner, Array.copy children) :> RRBNode<'T>

    override this.GetEditableEmptyNodeOfLengthN owner len =
        if this.IsEditableBy owner && this.Children.Length = len
        then this :> RRBNode<'T>
        else RRBFullNode<'T>(owner, Array.zeroCreate len) :> RRBNode<'T>

    override this.Shrink _ = this :> RRBNode<'T>
    override this.Expand _ = this :> RRBNode<'T>

    member this.ToRRBIfNeeded shift =
        if shift <= 0 then this :> RRBNode<'T> else
            let sizeTable = RRBNode<'T>.CreateSizeTable shift children
            if RRBMath.isSizeTableFullAtShift shift sizeTable sizeTable.Length
            then this :> RRBNode<'T>
            else RRBRelaxedNode<'T>(ownerToken, children, sizeTable) :> RRBNode<'T>
            // TODO: Check if expanded nodes need to override this

    member this.IndexesAndChild shift treeIdx =
        let localIdx = radixIndex shift treeIdx
        let child = children.[localIdx]
        let antimask = ~~~(Literals.blockIndexMask <<< shift)
        let nextTreeIdx = treeIdx &&& antimask
        localIdx, child, nextTreeIdx

    member this.UpdatedChildSameSize owner localIdx newChild =
        // TODO: If we need to override this in any child class, make it virtual (e.g. "abstract Foo" and then "default this.Foo")
        let node = this.GetEditableNode owner :?> RRBFullNode<'T>
        node.Children.[localIdx] <- newChild
        node

    abstract member UpdatedChildDifferentSize : OwnerToken -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.UpdatedChildDifferentSize owner localIdx newChild sizeDiff =
        this.UpdatedChildSameSize owner localIdx newChild
        // TODO: Actually, need to convert this to RRBRelaxedNode because child size has changed

    abstract member UpdatedChildSpecificChildSize : OwnerToken -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.UpdatedChildSpecificChildSize owner localIdx newChild childSize =
        this.UpdatedChildSameSize owner localIdx newChild
        // TODO: Actually, need to convert this to RRBRelaxedNode because child size has changed

    // TODO: Write a variant of UpdatedChildDifferentSize that can handle "sliding" items left or right, because RRBRelaxedNode will want to also "slide" the size table entries appropriately
    // And it will be useful in implementing the rebalance feature as well, where we'll be combining multiple nodes, often by "sliding" items around.
    // Perhaps we'll implement it by keeping track of the individual sizes of each item (by subtraction from the previous size table entry), and then adding each individual size as we go

    member this.BuildSizeTable shift count lastIdx =
        let fullSize = 1 <<< shift
        Array.init count (fun idx -> if idx = lastIdx then fullSize * idx + this.Children.[idx].TreeSize (down shift) else fullSize * (idx + 1))

    abstract member SplitNodeIntoArrays : OwnerToken -> int -> int -> (RRBNode<'T> [] * int []) * (RRBNode<'T> [] * int [])  // Params: owner shift splitIdx, and return: (left items, left sizes), (right items, right sizes)
    default this.SplitNodeIntoArrays owner shift splitIdx =
        // TODO: Expanded nodes *must* rewrite this so that the right children will be handled correctly (not filled with nulls, since we won't always *want* the right child to end up expanded)
        let fullSize = 1 <<< shift
        let rightLen = this.NodeSize - splitIdx
        let leftChildren, rightChildren = this.Children |> Array.splitAt splitIdx
        let leftSizes = Array.init splitIdx (fun idx -> fullSize * (idx+1))
        if rightChildren |> Array.isEmpty && splitIdx > 0 then
            // Usually the last child's size goes into rightSizes, but in this one case it needs to go into leftSizes instead
            leftSizes.[splitIdx - 1] <- (splitIdx - 1) * fullSize + (Array.last leftChildren).TreeSize (down shift)
        let rightSizes = Array.init rightLen (fun idx -> if idx = rightLen - 1 then fullSize * idx + this.Children.[idx].TreeSize (down shift) else fullSize * (idx+1))
        (leftChildren, leftSizes), (rightChildren, rightSizes)

    abstract member RemoveLeftmostChildren : OwnerToken -> int -> int -> (RRBNode<'T> * int) seq * RRBNode<'T>  // Params: owner shift itemCount, and return: (removed items, sizes of removed items, new node)
    default this.RemoveLeftmostChildren owner shift itemCount =
        // TODO: Document that itemCount of 0 should never happen, and/or make sure it does. This should be an internal function anyway. OR... decide that it's okay, and verify that it's okay.
#if DEBUG
        if itemCount <= 0 then failwith <| sprintf "Item count of %d in RRBFullNode<'T>.RemoveLeftmostChildren -- should never be zero or negative" itemCount
#else
        if itemCount <= 0 then Seq.empty, this :> RRBNode<'T> else
#endif
        let fullSize = 1 <<< shift
        let lastIdx = this.NodeSize - 1
        let newLen = this.NodeSize - itemCount
        let newNode = this.GetEditableEmptyNodeOfLengthN owner newLen :?> RRBFullNode<'T>
        if newLen > 0 then
            Array.blit this.Children itemCount newNode.Children 0 newLen
        this.Children |> Seq.truncate itemCount |> Seq.mapi (fun idx child ->
            let cumulativeSize = if idx = lastIdx then (fullSize * idx) + child.TreeSize (down shift) else fullSize * (idx + 1)
            child, cumulativeSize), newNode :> RRBNode<'T>

    // TODO: Test the RemoveLeftmostChildren variants with a wide variety of node types

    abstract member InsertAndSlideChildrenLeft : OwnerToken -> int -> int -> RRBNode<'T> -> RRBFullNode<'T> -> RRBFullNode<'T> * RRBFullNode<'T>
    default this.InsertAndSlideChildrenLeft owner shift localIdx newChild leftSibling =
        // if localIdx <= itemCount then
        //     printfn "DEBUG: Inserted item will end up in left sibling (left-hand side of slide left)"
        // else
        //     printfn "DEBUG: Inserted item will end up in this node (right-hand side of slide left)"
        let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly (localIdx + leftSibling.NodeSize) newChild leftSibling.Children this.Children
        // TODO: Might be able to be clever with an "append new items" thing in the left sibling, but this will do since it's simple
        // TODO: Optimize LATER, once we're sure that this works. Don't prematurely optimize.
        let newLeft  = RRBNode<'T>.MkNode owner shift newLeftItems
        let newRight = RRBNode<'T>.MkNode owner shift newRightItems
        newLeft, newRight

    abstract member InsertAndSlideChildrenRight : OwnerToken -> int -> int -> RRBNode<'T> -> RRBFullNode<'T> -> RRBFullNode<'T> * RRBFullNode<'T>
    default this.InsertAndSlideChildrenRight owner shift localIdx newChild rightSibling =
        let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly localIdx newChild this.Children rightSibling.Children
        // TODO: Might be able to be clever with an "insert new items at left side" thing in the right sibling, but this will do since it's simple
        // TODO: Optimize LATER, once we're sure that this works. Don't prematurely optimize.
        let newLeft  = RRBNode<'T>.MkNode owner shift newLeftItems
        let newRight = RRBNode<'T>.MkNode owner shift newRightItems
        newLeft, newRight

    abstract member InsertAndSplitNode : OwnerToken -> int -> int -> RRBNode<'T> -> RRBFullNode<'T> * RRBFullNode<'T>
    default this.InsertAndSplitNode owner shift localIdx newChild =
        let newLeftItems, newRightItems = Array.insertAndSplitEvenly (localIdx + 1) newChild this.Children
        let newLeft  = RRBNode<'T>.MkNode owner shift newLeftItems
        let newRight = RRBNode<'T>.MkNode owner shift newRightItems
        newLeft, newRight

    override this.UpdatedTree owner shift treeIdx newItem =
        let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
        let newNode = child.UpdatedTree owner (down shift) nextIdx newItem
        this.UpdatedChildSameSize owner localIdx newNode :> RRBNode<'T>

    override this.InsertedTree owner shift treeIdx item parentOpt idxOfNodeInParent =
        let localIdx, child, nextLvlIdx = this.IndexesAndChild shift treeIdx
        let insertResult = child.InsertedTree owner (down shift) nextLvlIdx item (Some this) localIdx
        match insertResult with
        | SimpleInsertion newChild ->
            SimpleInsertion (this.UpdatedChildDifferentSize owner localIdx newChild 1 :> RRBNode<'T>)
        | SlidItemsLeft (newLeft, newChild) ->
            // Always update the *right* child first, then the left: that way the size table adjustments will be correct
            let newNode = this.UpdatedChildSpecificChildSize owner localIdx newChild (newChild.TreeSize (down shift))
            SimpleInsertion (newNode.UpdatedChildSpecificChildSize owner (localIdx - 1) newLeft (newLeft.TreeSize (down shift)) :> RRBNode<'T>)
            // TODO: Do I need an "Update two child items at once" function? What about the size table? We should be able to manage the size table more cleverly in RelaxedNodes.
        | SlidItemsRight (newChild, newRight) ->
            let newNode = this.UpdatedChildSpecificChildSize owner localIdx newChild (newChild.TreeSize (down shift))
            SimpleInsertion (newNode.UpdatedChildSpecificChildSize owner (localIdx + 1) newRight (newRight.TreeSize (down shift)) :> RRBNode<'T>)
            // TODO: Comments from SlidItemsLeft re size table apply here too.
        | SplitNode (newChild, newRight) ->
            if this.NodeSize < Literals.blockSize then
                let newNode = this.InsertedChild owner shift (localIdx + 1) newRight (newRight.TreeSize (down shift))
                SimpleInsertion (newNode.UpdatedChildSpecificChildSize owner localIdx newChild (newChild.TreeSize (down shift)) :> RRBNode<'T>)
            else
                let localIdx, _, _ = this.IndexesAndChild shift treeIdx
                match (parentOpt, idxOfNodeInParent) with
                | Some parent, idx when idx > 0 && parent.Children.[idx - 1].NodeSize < Literals.blockSize ->
                    // Room in the left sibling
                    let leftSib = parent.Children.[idx - 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdatedChildSpecificChildSize owner localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSlideChildrenLeft owner shift (localIdx + 1) newRight leftSib
                    SlidItemsLeft (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)
                | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                    // Room in the right sibling
                    let rightSib = parent.Children.[idx + 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdatedChildSpecificChildSize owner localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSlideChildrenRight owner shift (localIdx + 1) newRight rightSib
                    SlidItemsRight (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)
                | _ ->
                    // No room left or right, so split
                    let newNode = this.UpdatedChildSpecificChildSize owner localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSplitNode owner shift localIdx newRight
                    SplitNode (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)

    abstract member AppendChild : OwnerToken -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.AppendChild owner shift newChild childSize =
        let newChildren = this.Children |> Array.copyAndAppend newChild
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if this.FullNodeIsTrulyFull shift then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // Last item wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member InsertedChild : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.InsertedChild owner shift localIdx newChild childSize =
        if localIdx = this.NodeSize then
            this.AppendChild owner shift newChild childSize
        else
            let fullSize = 1 <<< shift
            let newChildren = this.Children |> Array.copyAndInsertAt localIdx newChild
            if childSize = fullSize then
                // Only way this could be wrong is if we were inserting at the end, i.e. appending, and that's taken care of by "if localIdx = this.NodeSize" above
                RRBNode<'T>.MkFullNode owner newChildren
            else
                // Full node means that all but last child were max size, so we only have to call TreeSize once
                let sizeTable = Array.zeroCreate (this.NodeSize + 1)
                let mutable cumulativeSize = 0
                for i = 0 to this.NodeSize - 1 do
                    cumulativeSize <- cumulativeSize + (if i = localIdx then childSize else fullSize)
                    sizeTable.[i] <- cumulativeSize
                sizeTable.[this.NodeSize] <- cumulativeSize + this.LastChild.TreeSize (down shift)
                RRBNode<'T>.MkNodeKnownSize owner shift newChildren sizeTable

    abstract member RemoveLastChild : OwnerToken -> int -> RRBFullNode<'T>
    default this.RemoveLastChild owner shift =
        // Expanded nodes can do this in a transient way, but "normal" nodes can't
        this.Children |> Array.copyAndPop |> RRBNode<'T>.MkFullNode owner

    member this.RemoveLastLeaf owner shift =
        // EXPAND: This needs an implementation in expanded nodes, where we expand the new last child after shrinking the child we return
        if shift <= Literals.blockSizeShift then
            // Children are leaves
            if this.NodeSize = 0 then
#if DEBUG
                failwith <| sprintf "RemoveLastLeaf was called on an empty node at shift %d" shift
#else
                RRBLeafNode(owner, Array.empty), this  // TODO: Should be able to eliminate this branch, I hope
#endif
            else
                let leaf = this.LastChild :?> RRBLeafNode<'T>
                let newNode = this.RemoveLastChild owner shift // Popping the last entry from a FullNode can't ever turn it into an RRBNode.
                leaf, newNode
        else
            // Children are nodes
            let leaf, newLastChild = (this.LastChild :?> RRBFullNode<'T>).RemoveLastLeaf owner (down shift)
            let newNode =
                if newLastChild.NodeSize = 0
                then this.RemoveLastChild owner shift  // Child had just one child of its own and is now empty, so remove it
                else this.UpdatedChildSpecificChildSize owner (this.NodeSize - 1) newLastChild (newLastChild.TreeSize (down shift))
            leaf, newNode



and RRBRelaxedNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[], sizeTable : int[]) =
    inherit RRBFullNode<'T>(ownerToken, children)

    static member Create (owner : OwnerToken, shift : int, children : RRBNode<'T>[]) =
        let sizeTbl = RRBNode<'T>.CreateSizeTable shift children
        RRBRelaxedNode<'T>.CreateWithSizeTable(owner, shift, children, sizeTbl)

    static member CreateWithSizeTable (owner : OwnerToken, shift : int, children : RRBNode<'T>[], sizeTbl : int[]) =
        if isSizeTableFullAtShift shift sizeTbl sizeTbl.Length then
            RRBFullNode<'T>(owner, children)
        else
            RRBRelaxedNode<'T>(owner, children, sizeTbl) :> RRBFullNode<'T>

    member this.SizeTable = sizeTable

    override this.NodeSize = children.Length
    override this.TreeSize _ =
        if this.NodeSize = 0 then
            failwith "TreeSize called on an empty node; shouldn't happen"
            0
        else
            sizeTable.[this.NodeSize - 1]
    override this.TwigSlotCount =
        // In a relaxed twig node, the last entry in the size table is all we need to look up
        if this.NodeSize = 0 then 0 else this.SizeTable.[this.NodeSize - 1]

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBRelaxedNode<'T>(owner, Array.copy children, Array.copy sizeTable) :> RRBNode<'T>

    override this.UpdatedChildDifferentSize owner localIdx newChild sizeDiff =
        let newNode = this.GetEditableNode owner :?> RRBRelaxedNode<'T>
        newNode.Children.[localIdx] <- newChild
        for i = localIdx to this.NodeSize - 1 do
            newNode.SizeTable.[i] <- this.SizeTable.[i] + sizeDiff
        newNode :> RRBFullNode<'T>

    override this.UpdatedChildSpecificChildSize owner localIdx newChild childSize =
        let oldChildSize = this.SizeTable.[localIdx]
        this.UpdatedChildDifferentSize owner localIdx newChild (childSize - oldChildSize)

    override this.SplitNodeIntoArrays owner shift splitIdx =
        // TODO: Expanded nodes *must* rewrite this so that the right children will be handled correctly (not filled with nulls, since we won't always *want* the right child to end up expanded)
        let leftChildren, rightChildren = this.Children |> Array.splitAt splitIdx
        let leftSizes, rightSizes = this.SizeTable |> Array.splitAt splitIdx
        if splitIdx > 0 then
            let lastLeftSize = Array.last leftSizes
            let lastRightIdx = rightSizes.Length - 1
            for i = 0 to lastRightIdx do
                rightSizes.[i] <- rightSizes.[i] - lastLeftSize
        (leftChildren, leftSizes), (rightChildren, rightSizes)

    override this.RemoveLeftmostChildren owner shift itemCount =
        // TODO: Document that itemCount of 0 should never happen, and/or make sure it does. This should be an internal function anyway. OR... decide that it's okay, and verify that it's okay.
#if DEBUG
        if itemCount <= 0 then failwith <| sprintf "Item count of %d in RRBFullNode<'T>.RemoveLeftmostChildren -- should never be zero or negative" itemCount
#else
        if itemCount <= 0 then Seq.empty, this :> RRBNode<'T> else
#endif
        // TODO: Triple-check this one because I wrote it in haste. Check that the cumulative size table is correct, for example: make sure it doesn't start with 0
        let newLen = this.NodeSize - itemCount
        let newNode = this.GetEditableEmptyNodeOfLengthN owner newLen :?> RRBRelaxedNode<'T>
        // TODO: Consider the scenario where GetEditableEmptyNodeOfLengthN returns `this`. We'd end up returning the wrong set of children in the result below unless we created an intermediate array.
        // And if that's the case, why are we trying so hard to *avoid* creating an intermediate array here? Just split the arrays and be done with it, and call this.CreateSomethingSizeTable (new function)
        // to create the new nodes from items and size tables. Why a new function? Because our current CreateWithSizeTable is a static method, so RRBExpandedRelaxedNode won't override it, and we want
        // to override it in the specific case of an expanded node (which will create an array of length 32 and then use Array.CopyTo to populate it from the source arrays).
        if newLen > 0 then
            let sizeDiff = this.SizeTable.[itemCount - 1]
            for i = 0 to newLen - 1 do
                let j = i + itemCount
                newNode.Children.[i] <- this.Children.[j]
                newNode.SizeTable.[i] <- this.SizeTable.[j] - sizeDiff
        ((this.Children |> Seq.truncate itemCount), (this.SizeTable |> Seq.truncate itemCount)) ||> Seq.map2 (fun child size -> child, size), newNode :> RRBNode<'T>

    // TODO: If there's a good way to implement this, do so. Might be more efficient, might not.
    // override this.InsertAndSlideChildrenLeft owner shift localIdx newChild leftSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSlideChildrenRight owner shift localIdx newChild rightSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSplitNode owner shift localIdx newChild =
    //     failwith "Not implemented"

    override this.AppendChild owner shift newChild childSize =
        let newChildren = this.Children |> Array.copyAndAppend newChild
        let lastSizeTableEntry = if this.SizeTable.Length = 0 then 0 else Array.last this.SizeTable
        let newSizeTable = this.SizeTable |> Array.copyAndAppend (lastSizeTableEntry + childSize)
        RRBNode<'T>.MkNodeKnownSize owner shift newChildren newSizeTable

    override this.RemoveLastChild owner shift =
        let children' = this.Children |> Array.copyAndPop
        let sizeTable' = this.SizeTable |> Array.copyAndPop
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'



and RRBLeafNode<'T>(ownerToken : OwnerToken, items : 'T[]) =
    inherit RRBNode<'T>(ownerToken)

    member this.Items = items

    override this.NodeSize = items.Length
    override this.TreeSize _ = items.Length
    override this.SlotCount =
        failwith "Slot count called on a leaf node"  // TODO: Remove this before going into production, and make it return this.NodeSize
        this.NodeSize
    override this.TwigSlotCount =
        failwith "Twig slot count called on a leaf node"  // TODO: Remove this before going into production, and make it return this.NodeSize
        this.NodeSize

    override this.Shrink _ = this :> RRBNode<'T>
    override this.Expand owner =
        // If anyone else might possibly have a reference to this node, we need to copy the item array
        // But if the item array isn't Literals.blockSize in length, then the RRBExpandedLeafNode constructor will take care of that detail for us
        let safeItems = if not (this.IsEditableBy owner) && this.NodeSize = Literals.blockSize then Array.copy items else items
        RRBExpandedLeafNode<'T>(owner, safeItems) :> RRBNode<'T>

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(owner, Array.copy items) :> RRBNode<'T>

    override this.GetEditableEmptyNodeOfLengthN owner len =
        if this.IsEditableBy owner && this.Items.Length = len
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(owner, Array.zeroCreate len) :> RRBNode<'T>

    abstract member LeafNodeWithItems : OwnerToken -> 'T [] -> RRBLeafNode<'T>
    default this.LeafNodeWithItems owner newItems =
        if this.NodeSize = newItems.Length then  // NOT this.NodeSize here
            let newNode = this.GetEditableNode owner :?> RRBLeafNode<'T>
            newItems.CopyTo(newNode.Items, 0)
            newNode
        else
            RRBNode<'T>.MkLeaf owner newItems

    member this.UpdatedItem owner localIdx newItem =
        let node = this.GetEditableNode owner :?> RRBLeafNode<'T>
        node.Items.[localIdx] <- newItem
        node :> RRBNode<'T>

    override this.UpdatedTree owner shift treeIdx newItem =
        this.UpdatedItem owner treeIdx newItem

    abstract member InsertedItem : OwnerToken -> int -> 'T -> RRBLeafNode<'T>
    default this.InsertedItem owner localIdx item =
        // ExpandedLeafNodes will override this to do an insert in place if they can,
        // but shrunken nodes cannot insert in place so there's no point in checking the owner in *this* class.
        let newItems = this.Items |> Array.copyAndInsertAt localIdx item
        RRBLeafNode<'T>(owner, newItems)

    abstract member AppendedItem : OwnerToken -> 'T -> RRBLeafNode<'T>
    default this.AppendedItem owner item =
        // ExpandedLeafNodes will override this to do an insert in place if they can,
        // but shrunken nodes cannot insert in place so there's no point in checking the owner in *this* class.
        let newItems = this.Items |> Array.copyAndAppend item
        RRBLeafNode<'T>(owner, newItems)

    override this.InsertedTree owner shift treeIdx item parentOpt idxOfNodeInParent =
        if this.NodeSize < Literals.blockSize then
            SimpleInsertion (this.InsertedItem owner treeIdx item :> RRBNode<'T>)
        else
            let localIdx = treeIdx
            match (parentOpt, idxOfNodeInParent) with
            | Some parent, idx when idx > 0 && parent.Children.[idx - 1].NodeSize < Literals.blockSize ->
                // Room in the left sibling
                let leftSib = parent.Children.[idx - 1] :?> RRBLeafNode<'T>
                let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly (localIdx + leftSib.NodeSize) item leftSib.Items this.Items
                let newLeft  = RRBNode<'T>.MkLeaf owner newLeftItems :> RRBNode<'T>
                let newRight = this.LeafNodeWithItems owner newRightItems :> RRBNode<'T>
                SlidItemsLeft (newLeft, newRight)
            | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                // Room in the right sibling
                let rightSib = parent.Children.[idx + 1] :?> RRBLeafNode<'T>
                let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly localIdx item this.Items rightSib.Items
                // Note that we DON'T use LeafNodeWithItems here: expanded leaves may only be the rightmost leaf in the tree,
                // so if this is an expanded note that would benefit from using LeafNodeWithItems, we should never reach this branch
                let newLeft  = RRBNode<'T>.MkLeaf owner newLeftItems :> RRBNode<'T>
                let newRight = RRBNode<'T>.MkLeaf owner newRightItems :> RRBNode<'T>
                SlidItemsRight (newLeft, newRight)
            | _ ->
                // Don't need to get fancy with SplitNode here, though expanded leaf nodes will want to do something slightly more clever (ensuring that the new right-hand node is still expanded)
                let newLeftItems, newRightItems = Array.insertAndSplitEvenly localIdx item this.Items
                let newLeft  = RRBNode<'T>.MkLeaf owner newLeftItems :> RRBNode<'T>
                let newRight = this.LeafNodeWithItems owner newRightItems :> RRBNode<'T>
                SplitNode (newLeft, newRight)

    abstract member RemoveLastItem : OwnerToken -> RRBLeafNode<'T>
    override this.RemoveLastItem owner =
        this.Items |> Array.copyAndPop |> RRBNode<'T>.MkLeaf owner



// === EXPANDED NODES ===

// Expanded nodes are used in transient trees. Their arrays are always Literals.blockSize in size, so an int field
// is used to keep track of the node's actual size. The rest of the node is filled with nulls (or the default value
// of 'T in the case of leaves). This allows appends to be *very* fast.

and RRBExpandedFullNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[], ?realSize : int) =
    inherit RRBFullNode<'T>(ownerToken, Array.expandToBlockSize children)

    member val CurrentLength : int = defaultArg realSize (Array.length children) with get, set
    override this.NodeSize = this.CurrentLength

    override this.Shrink owner =
        let len = this.NodeSize
        let children' =
            if len = Literals.blockSize
            then Array.copy this.Children
            else this.Children |> Array.truncate len
        RRBFullNode<'T>(owner, children') :> RRBNode<'T>

    override this.Expand owner =
        if this.IsEditableBy owner then this :> RRBNode<'T> else RRBExpandedFullNode<'T>(owner, Array.copy this.Children) :> RRBNode<'T>
        // TODO: That's the same logic as GetEditableNode. Does it make sense to combine them? I.e., have `override this.Expand owner = this.GetEditableNode owner`?

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedFullNode<'T>(owner, Array.copy this.Children) :> RRBNode<'T>

    // override this.InsertAndSlideChildrenLeft owner shift localIdx newChild leftSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSlideChildrenRight owner shift localIdx newChild rightSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSplitNode owner shift localIdx newChild =
    //     failwith "Not implemented"

    override this.RemoveLastChild owner shift =
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        let newNode = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let idx = this.NodeSize - 1
        newNode.Children.[idx] <- null
        newNode.CurrentLength <- idx
        newNode :> RRBFullNode<'T>



and RRBExpandedRelaxedNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[], sizeTable : int[], ?realSize : int) =
    inherit RRBRelaxedNode<'T>(ownerToken, Array.expandToBlockSize children, Array.expandToBlockSize sizeTable)

    member val CurrentLength : int = defaultArg realSize (Array.length children) with get, set
    override this.NodeSize = this.CurrentLength

    override this.Shrink owner =
        let len = this.NodeSize
        let children' =
            if len = Literals.blockSize
            then Array.copy this.Children
            else this.Children |> Array.truncate len
        let sizeTable' =
            if len = Literals.blockSize
            then Array.copy this.SizeTable
            else this.SizeTable |> Array.truncate len
        RRBRelaxedNode<'T>(owner, children', sizeTable') :> RRBNode<'T>

    override this.Expand owner =
        if this.IsEditableBy owner then this :> RRBNode<'T> else RRBExpandedFullNode<'T>(owner, Array.copy this.Children) :> RRBNode<'T>
        // TODO: That's the same logic as GetEditableNode. Does it make sense to combine them? I.e., have `override this.Expand owner = this.GetEditableNode owner`?

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedFullNode<'T>(owner, Array.copy this.Children) :> RRBNode<'T>

    override this.RemoveLastChild owner shift =
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        let newNode = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let idx = this.NodeSize - 1
        newNode.Children.[idx] <- null
        newNode.SizeTable.[idx] <- 0
        newNode.CurrentLength <- idx
        if isSizeTableFullAtShift shift newNode.SizeTable idx then
            // Can convert back to full node, so let's do it for future efficiency's sake
            RRBExpandedFullNode(owner, newNode.Children) :> RRBFullNode<'T>
        else
            newNode :> RRBFullNode<'T>



and RRBExpandedLeafNode<'T>(ownerToken : OwnerToken, items : 'T[], ?realSize : int) =
    inherit RRBLeafNode<'T>(ownerToken, Array.expandToBlockSize items)

    member val CurrentLength : int = defaultArg realSize (Array.length items) with get, set
    override this.NodeSize = this.CurrentLength

    override this.Shrink owner =
        let len = this.NodeSize
        let items' =
            if len = Literals.blockSize
            then Array.copy items
            else items |> Array.truncate len
        RRBLeafNode<'T>(owner, items') :> RRBNode<'T>

    override this.Expand owner =
        if this.IsEditableBy owner then this :> RRBNode<'T> else RRBExpandedLeafNode<'T>(owner, Array.copy this.Items) :> RRBNode<'T>

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedLeafNode<'T>(owner, Array.copy this.Items) :> RRBNode<'T>

    override this.InsertedItem owner localIdx item =
        // No bounds-checking: that's the job of the caller
        let newLeaf = this.GetEditableNode owner :?> RRBExpandedLeafNode<'T>
        Array.blit newLeaf.Items localIdx newLeaf.Items (localIdx+1) (newLeaf.NodeSize - localIdx)
        newLeaf.Items.[localIdx] <- item
        newLeaf.CurrentLength <- newLeaf.NodeSize + 1
        newLeaf :> RRBLeafNode<'T>

    override this.AppendedItem owner item =
        // No bounds-checking: that's the job of the caller
        let newLeaf = this.GetEditableNode owner :?> RRBExpandedLeafNode<'T>
        let len = newLeaf.NodeSize
        newLeaf.Items.[len] <- item
        newLeaf.CurrentLength <- len + 1
        newLeaf :> RRBLeafNode<'T>

    override this.LeafNodeWithItems owner newItems =
        let oldNodeLength = this.NodeSize
        let newNode = this.GetEditableNode owner :?> RRBExpandedLeafNode<'T>
        let newNodeLength = newItems.Length
        newItems.CopyTo(newNode.Items, 0)
        for i = newNodeLength to oldNodeLength - 1 do
            newNode.Items.[i] <- Unchecked.defaultof<'T>
        newNode.CurrentLength <- newNodeLength
        newNode :> RRBLeafNode<'T>

    override this.RemoveLastItem owner =
        let newLeaf = this.GetEditableNode owner :?> RRBExpandedLeafNode<'T>
        let idx = this.NodeSize - 1
        newLeaf.Items.[idx] <- Unchecked.defaultof<'T>
        newLeaf.CurrentLength <- idx
        newLeaf :> RRBLeafNode<'T>

(*
Idea for a rebalance algorithm:

First, look through the node in order from i=0 to i=31 for the first
child with child.NodeSize < Literals.blockSizeMin (that is, < 31).
Now mark that as the left point, and sweep forward (keeping track of
a running length) looking at child.NodeSize for subsequent children.
Once you've found a series of adjacent children whose NodeSize properties
add up to at least 32 less than (length * 32), that's a point where you can
reduce the current node's size by 1 with a rebalance. E.g.,

16 17 32 32 ...   <- 16+17 = 33, which is >  (length-1)*32, so rebalance not useful
15 17 32 32 ...   <- 15+17 = 32, which is <= (length-1)*32, so [15; 17] is a rebalance candidate of length 2. Start index 0, length 2.
16 17 31 32 ...   <- 16+17 = 33, > 32, not a candidate. But 16+17+31 = 64, <= 64, so that's a length-3 run that reduces down to 2.
16 17 32 31 ...

... and the key insight that I want to record is this: once you find a run of length N that reduces by 1, look at at least N more items to see if there's a better candidate.
To do so:

1. While sweeping the first run, keep track of the minimum number found during that sweep, and its index. Use that index (and length N at most) as another possible run candidate; if a shorter-than-N run is found, great.
2. Take the final item of the first run, and the *next* item after that one. The smaller of the two becomes the next candidate, and we sweep at most N items to find another run candidate that's shorter than N.

So there are two places where a length of N will be swept after finding the first candidate:
  a) From the smallest child (if it's not the first), and
  b) From the smaller of the last or post-last item of the first run. (That is, indices i+N-1 and i+N will be compared).

It's possible that a) will fail because the first was the smallest, and that b) will fail because i+N+N goes past the end of the current node's NodeSize. In which case we go only to the end of the current node and no farther.
Either way, if a) or b) fails, then it's ignored. The other one (if it's valid) will be checked, and overall, the one with the SMALLEST run length wins. (I.e., it must be LESS than N).

... And one more possibility: once we've found a run of length N, we continue looking at one more child as long as there's still room in the combined node we've built so far.
E.g., in 4 5 6 7 8 9, running total is 4; 9; 15; 22; 30; 39. 39 goes over the length of the combined node, so we'd grab [4; 5; 6; 7; 8] and turn then into a node of 30, resulting in [30; 9] at the end.
And in 8 7 6 5 4 3 2 1 2 3 4 5 6 7 8, we'd find [8; 7; 6; 5; 4] but then we'd look at [4; 3; 2; 1; 2] and [3; 2; 1; 2; 3] and each of those could be extended by N to [4; 3; 2; 1; 2; 3; 4; 5; 6; 7] = [30; 7], or [3; 2; 1; 2; 3; 4; 5; 6; 7] = [26; 7]
So there, the [4 3 2 1 2 3 4 5 6] would be selected as the final rebalance to be run.


Another way to think about it. Consider an ideally-filled subtree. The children are length [M; M; M], etc.
Now consider that any M-1 children have one "gap", any M-2 children have 2 "gaps", and so on.
As we look at sub-arrays, we have a length and we keep a running sum of the items in the sub-array.
Formulas:
  idealSlotCount = length * M (or length <<< blockSizeShift)
  gaps = idealSlotCount - sum
  reductions = gaps / M (or gaps >>> blockSizeShift)

The score is the # of reductions, and between two items with the same reductions, the smaller length wins.
So the score is (reductions, -length) and then you compare those tuples with normal comparison; the greater score wins.
*)

// Basic algorithm found at https://stackoverflow.com/questions/13023188/smallest-subset-of-array-whose-sum-is-no-less-than-key
let smallestRunGreaterThan n arr =
    let mutable acc = 0uy
    let mutable p = 0
    let mutable q = 0
    let arrLen = Array.length arr
    let mutable bestIdx = 0
    let mutable bestLen = arrLen
    while q < arrLen do
        // Expand candidate run until its total is at least N
        while acc < n && q < arrLen do
            acc <- acc + arr.[q]
            q <- q + 1
        while acc - arr.[p] >= n && p < arrLen do
            acc <- acc - arr.[p]
            p <- p + 1
        if acc >= n then
            let candidateLen = q - p
            if candidateLen < bestLen then
                bestLen <- candidateLen
                bestIdx <- p
        acc <- acc - arr.[p]
        p <- p + 1
    bestIdx, bestLen
// TODO: Write a unit test or two for this implementation, and make sure that [0..9] is among the tests we use.
// Also, random tests that compare this implementation to a brute-force O(N^2) search and make sure that it finds something of minimal length. Bonus if it finds the leftmost possible solution.

let findMergeCandidates (sizeSeq : #seq<int>) len =
    use e = sizeSeq.GetEnumerator()
    let sizes = Array.init len (fun _ -> byte (if e.MoveNext() then Literals.blockSize - e.Current else 0))
    sizes |> smallestRunGreaterThan (byte Literals.blockSize)

// TODO: At some point, uncomment this version and test whether it is more efficient
// let findMergeCandidatesTwoPasses (sizeSeq : #seq<int>) len =
//     use e = sizeSeq.GetEnumerator()
//     let sizes = Array.init len (fun _ -> byte (if e.MoveNext() then Literals.blockSize - e.Current else 0))
//     let idx1, len1 = sizes |> smallestRunGreaterThan (byte Literals.blockSize)
//     let idx2, len2 = sizes |> smallestRunGreaterThan (byte (Literals.blockSize <<< 1))
//     // Drop two slots if we can do so in less than twice the work needed to drop a single slot
//     if len2 < (len1 * 2) then
//         idx2, len2
//     else
//         idx1, len1
