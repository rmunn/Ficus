module Ficus.RRBVectorBetterNodes

open System.Threading
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

    let isSizeTableFullAtShift shift (sizeTbl : int[]) =
        let len = Array.length sizeTbl
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

// Used in insertion logic. TODO: Once I nail down the method name, add it here
// TODO: Might want to remove the slidItemCount part of SlidItems(Left|Right), since I don't seem to be actually using it.
// TODO: But first, wait until I've written the RelaxedNode implementation. Then decide about the slidItemCount.
type SlideResult<'a> =
    | SimpleInsertion of newCurrent : 'a
    | SlidItemsLeft of slidItemCount : int * newLeft : 'a * newCurrent : 'a
    | SlidItemsRight of slidItemCount : int * newCurrent : 'a * newRight : 'a
    | SplitNode of newCurrent : 'a * newRight : 'a

[<AbstractClass>]
type RRBNode<'T>(thread : Thread ref) =
    let thread = thread

    member this.Thread : Thread ref = thread
    member this.SetThread (t : Thread) : unit = thread := t

    abstract member Shrink : unit -> RRBNode<'T>
    abstract member Expand : Thread ref -> RRBNode<'T>

    abstract member NodeSize : int          // How many children does this single node have?
    abstract member TreeSize : int -> int   // How many total items are found in this node's entire descendant tree?
    abstract member SlotCount : int         // Used in rebalancing; the "slot count" is the total of the node sizes of this node's children
    abstract member TwigSlotCount : int     // Like SlotCount, but used when we *know* this node is a twig and its children are leaves, which allows some optimizations

    abstract member GetEditableNode : Thread ref -> RRBNode<'T>
    abstract member GetEditableEmptyNodeOfLengthN : Thread ref -> int -> RRBNode<'T>

    member this.IsEditableBy (mutator : Thread ref) =
        LanguagePrimitives.PhysicalEquality mutator thread && not (isNull !thread)
        // Note that this test is NOT "if mutator = thread".

    static member CreateSizeTable (shift : int) (array:RRBNode<'T>[]) : int[] =
        let sizeTable = Array.zeroCreate array.Length
        let mutable total = 0
        for i = 0 to array.Length - 1 do
            total <- total + (array.[i]).TreeSize (down shift)
            sizeTable.[i] <- total
        sizeTable

    static member MkLeaf (thread : Thread ref) (items : 'T[]) = RRBLeafNode<'T>(thread, items)
    static member MkNode (thread : Thread ref) (shift : int) (children : RRBNode<'T>[]) =
        RRBRelaxedNode<'T>.Create(thread, shift, children)
    static member MkNodeKnownSize (thread : Thread ref) (shift : int) (children : RRBNode<'T>[]) (sizeTable : int[]) =
        RRBRelaxedNode<'T>.CreateWithSizeTable(thread, shift, children, sizeTable)
    static member MkFullNode (thread : Thread ref) (children : RRBNode<'T>[]) =
        // if children.Length = 1 then SingletonNode<'T>(thread, entries) :> Node<'T> else  // TODO: Do we want an RRBSingletonNode class as well?
        RRBFullNode<'T>.Create(thread, children)

    abstract member UpdatedTree : Thread ref -> int -> int -> 'T -> RRBNode<'T>  // Params: mutator shift treeIdx newItem
    abstract member InsertedTree : Thread ref -> int -> int -> 'T -> RRBFullNode<'T> option -> int -> SlideResult<RRBNode<'T>>  // Params: mutator shift treeIdx (item : 'T) (parentOpt : Node option) idxOfNodeInParent

and RRBFullNode<'T>(thread : Thread ref, children : RRBNode<'T>[]) =
    inherit RRBNode<'T>(thread)

    static member Create (thread : Thread ref, children : RRBNode<'T>[]) = RRBFullNode<'T>(thread, children)

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
    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBFullNode<'T>(mutator, Array.copy children) :> RRBNode<'T>

    override this.GetEditableEmptyNodeOfLengthN mutator len =
        if this.IsEditableBy mutator && this.Children.Length = len
        then this :> RRBNode<'T>
        else RRBFullNode<'T>(mutator, Array.zeroCreate len) :> RRBNode<'T>

    override this.Shrink() = this :> RRBNode<'T>
    override this.Expand _ = this :> RRBNode<'T>

    member this.ToRRBIfNeeded shift =
        if shift <= 0 then this :> RRBNode<'T> else
            let sizeTable = RRBNode<'T>.CreateSizeTable shift children
            if RRBMath.isSizeTableFullAtShift shift sizeTable
            then this :> RRBNode<'T>
            else RRBRelaxedNode<'T>(thread, children, sizeTable) :> RRBNode<'T>

    member this.IndexesAndChild shift treeIdx =
        let localIdx = radixIndex shift treeIdx
        let child = children.[localIdx]
        let antimask = ~~~(Literals.blockIndexMask <<< shift)
        let nextTreeIdx = treeIdx &&& antimask
        localIdx, child, nextTreeIdx

    member this.UpdatedChildSameSize mutator localIdx newChild =
        // TODO: If we need to override this in any child class, make it virtual (e.g. "abstract Foo" and then "default this.Foo")
        let node = this.GetEditableNode mutator :?> RRBFullNode<'T>
        node.Children.[localIdx] <- newChild
        node

    abstract member UpdatedChildDifferentSize : Thread ref -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.UpdatedChildDifferentSize mutator localIdx newChild sizeDiff =
        this.UpdatedChildSameSize mutator localIdx newChild
        // TODO: Actually, need to convert this to RRBRelaxedNode because child size has changed

    abstract member UpdatedChildSpecificChildSize : Thread ref -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.UpdatedChildSpecificChildSize mutator localIdx newChild childSize =
        this.UpdatedChildSameSize mutator localIdx newChild
        // TODO: Actually, need to convert this to RRBRelaxedNode because child size has changed

    // TODO: Write a variant of UpdatedChildDifferentSize that can handle "sliding" items left or right, because RRBRelaxedNode will want to also "slide" the size table entries appropriately
    // And it will be useful in implementing the rebalance feature as well, where we'll be combining multiple nodes, often by "sliding" items around.
    // Perhaps we'll implement it by keeping track of the individual sizes of each item (by subtraction from the previous size table entry), and then adding each individual size as we go

    member this.BuildSizeTable shift count lastIdx =
        let fullSize = 1 <<< shift
        Array.init count (fun idx -> if idx = lastIdx then fullSize * idx + this.Children.[idx].TreeSize (down shift) else fullSize * (idx + 1))

    abstract member SplitNodeIntoArrays : Thread ref -> int -> int -> (RRBNode<'T> [] * int []) * (RRBNode<'T> [] * int [])  // Params: mutator shift splitIdx, and return: (left items, left sizes), (right items, right sizes)
    default this.SplitNodeIntoArrays mutator shift splitIdx =
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

    abstract member RemoveLeftmostChildren : Thread ref -> int -> int -> (RRBNode<'T> * int) seq * RRBNode<'T>  // Params: mutator shift itemCount, and return: (removed items, sizes of removed items, new node)
    default this.RemoveLeftmostChildren mutator shift itemCount =
        // TODO: Document that itemCount of 0 should never happen, and/or make sure it does. This should be an internal function anyway. OR... decide that it's okay, and verify that it's okay.
#if DEBUG
        if itemCount <= 0 then failwith <| sprintf "Item count of %d in RRBFullNode<'T>.RemoveLeftmostChildren -- should never be zero or negative" itemCount
#else
        if itemCount <= 0 then Seq.empty, this :> RRBNode<'T> else
#endif
        let fullSize = 1 <<< shift
        let lastIdx = this.NodeSize - 1
        let newLen = this.NodeSize - itemCount
        let newNode = this.GetEditableEmptyNodeOfLengthN mutator newLen :?> RRBFullNode<'T>
        if newLen > 0 then
            Array.blit this.Children itemCount newNode.Children 0 newLen
        this.Children |> Seq.truncate itemCount |> Seq.mapi (fun idx child ->
            let cumulativeSize = if idx = lastIdx then (fullSize * idx) + child.TreeSize (down shift) else fullSize * (idx + 1)
            child, cumulativeSize), newNode :> RRBNode<'T>

    // TODO: Test the RemoveLeftmostChildren variants with a wide variety of node types

    abstract member InsertAndSlideChildrenLeft : Thread ref -> int -> int -> RRBNode<'T> -> RRBFullNode<'T> -> RRBFullNode<'T> * RRBFullNode<'T>
    default this.InsertAndSlideChildrenLeft mutator shift localIdx newChild leftSibling =
        // if localIdx <= itemCount then
        //     printfn "DEBUG: Inserted item will end up in left sibling (left-hand side of slide left)"
        // else
        //     printfn "DEBUG: Inserted item will end up in this node (right-hand side of slide left)"
        let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly (localIdx + leftSibling.NodeSize) newChild leftSibling.Children this.Children
        // TODO: Might be able to be clever with an "append new items" thing in the left sibling, but this will do since it's simple
        // TODO: Optimize LATER, once we're sure that this works. Don't prematurely optimize.
        let newLeft  = RRBNode<'T>.MkNode mutator shift newLeftItems
        let newRight = RRBNode<'T>.MkNode mutator shift newRightItems
        newLeft, newRight

    abstract member InsertAndSlideChildrenRight : Thread ref -> int -> int -> RRBNode<'T> -> RRBFullNode<'T> -> RRBFullNode<'T> * RRBFullNode<'T>
    default this.InsertAndSlideChildrenRight mutator shift localIdx newChild rightSibling =
        let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly localIdx newChild this.Children rightSibling.Children
        // TODO: Might be able to be clever with an "insert new items at left side" thing in the right sibling, but this will do since it's simple
        // TODO: Optimize LATER, once we're sure that this works. Don't prematurely optimize.
        let newLeft  = RRBNode<'T>.MkNode mutator shift newLeftItems
        let newRight = RRBNode<'T>.MkNode mutator shift newRightItems
        newLeft, newRight

    abstract member InsertAndSplitNode : Thread ref -> int -> int -> RRBNode<'T> -> RRBFullNode<'T> * RRBFullNode<'T>
    default this.InsertAndSplitNode mutator shift localIdx newChild =
        let newLeftItems, newRightItems = Array.insertAndSplitEvenly (localIdx + 1) newChild this.Children
        let newLeft  = RRBNode<'T>.MkNode mutator shift newLeftItems
        let newRight = RRBNode<'T>.MkNode mutator shift newRightItems
        newLeft, newRight

    override this.UpdatedTree mutator shift treeIdx newItem =
        let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
        let newNode = child.UpdatedTree mutator (down shift) nextIdx newItem
        this.UpdatedChildSameSize mutator localIdx newNode :> RRBNode<'T>

    override this.InsertedTree mutator shift treeIdx item parentOpt idxOfNodeInParent =
        let localIdx, child, nextLvlIdx = this.IndexesAndChild shift treeIdx
        let insertResult = child.InsertedTree mutator (down shift) nextLvlIdx item (Some this) localIdx
        match insertResult with
        | SimpleInsertion newChild ->
            SimpleInsertion (this.UpdatedChildDifferentSize mutator localIdx newChild 1 :> RRBNode<'T>)
        | SlidItemsLeft (slidItemCount, newLeft, newChild) ->
            // Always update the *right* child first, then the left: that way the size table adjustments will be correct
            let newNode = this.UpdatedChildSpecificChildSize mutator localIdx newChild (newChild.TreeSize (down shift))
            SimpleInsertion (newNode.UpdatedChildSpecificChildSize mutator (localIdx - 1) newLeft (newLeft.TreeSize (down shift)) :> RRBNode<'T>)
            // TODO: Do I need an "Update two child items at once" function? What about the size table? We should be able to manage the size table
        | SlidItemsRight (slidItemCount, newChild, newRight) ->
            let newNode = this.UpdatedChildSpecificChildSize mutator (localIdx - 1) newChild (newChild.TreeSize (down shift))
            SimpleInsertion (newNode.UpdatedChildSpecificChildSize mutator (localIdx + 1) newRight (newChild.TreeSize (down shift)) :> RRBNode<'T>)
            // TODO: Do I need an "Update two child items at once" function? What about the size table? We should be able to manage the size table
        | SplitNode (newChild, newRight) ->
            if this.NodeSize < Literals.blockSize then
                let newNode = this.InsertedChild mutator shift (localIdx + 1) newRight (newRight.TreeSize (down shift))
                SimpleInsertion (newNode.UpdatedChildSpecificChildSize mutator localIdx newChild (newChild.TreeSize (down shift)) :> RRBNode<'T>)
            else
                let localIdx, _, _ = this.IndexesAndChild shift treeIdx
                match (parentOpt, idxOfNodeInParent) with
                | Some parent, idx when idx > 0 && parent.Children.[idx - 1].NodeSize < Literals.blockSize ->
                    // Room in the left sibling
                    let leftSib = parent.Children.[idx - 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdatedChildSpecificChildSize mutator localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSlideChildrenLeft mutator shift (localIdx + 1) newRight leftSib
                    SlidItemsLeft (0, newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)
                    // TODO: Calculate the actual number of items rather than just using 0
                    // ... Or decide that the slid item count isn't interesting at all and get rid of it from the DU
                | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                    // Room in the right sibling
                    let rightSib = parent.Children.[idx + 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdatedChildSpecificChildSize mutator localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSlideChildrenRight mutator shift (localIdx + 1) newRight rightSib
                    SlidItemsRight (0, newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)
                    // TODO: Calculate the actual number of items rather than just using 0
                    // ... Or decide that the slid item count isn't interesting at all and get rid of it from the DU
                | _ ->
                    // No room left or right, so split
                    let newNode = this.UpdatedChildSpecificChildSize mutator localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSplitNode mutator shift localIdx newRight
                    SplitNode (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)

    abstract member AppendChild : Thread ref -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.AppendChild mutator shift newChild childSize =
        let newChildren = this.Children |> Array.copyAndAppend newChild
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if this.FullNodeIsTrulyFull shift then
            RRBNode<'T>.MkFullNode mutator newChildren
        else
            // Last item wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode mutator shift newChildren

    abstract member InsertedChild : Thread ref -> int -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.InsertedChild mutator shift localIdx newChild childSize =
        if localIdx = this.NodeSize then
            this.AppendChild mutator shift newChild childSize
        else
            let fullSize = 1 <<< shift
            let newChildren = this.Children |> Array.copyAndInsertAt localIdx newChild
            if childSize = fullSize then
                // Only way this could be wrong is if we were inserting at the end, i.e. appending, and that's taken care of by "if localIdx = this.NodeSize" above
                RRBNode<'T>.MkFullNode mutator newChildren
            else
                // Full node means that all but last child were max size, so we only have to call TreeSize once
                let sizeTable = Array.zeroCreate (this.NodeSize + 1)
                let mutable cumulativeSize = 0
                for i = 0 to this.NodeSize - 1 do
                    cumulativeSize <- cumulativeSize + (if i = localIdx then childSize else fullSize)
                    sizeTable.[i] <- cumulativeSize
                sizeTable.[this.NodeSize] <- cumulativeSize + this.LastChild.TreeSize (down shift)
                RRBNode<'T>.MkNodeKnownSize mutator shift newChildren sizeTable

    abstract member RemoveLastChild<'T> : Thread ref -> int -> RRBFullNode<'T>
    default this.RemoveLastChild<'T> mutator shift =
        // Expanded nodes can do this in a transient way, but "normal" nodes can't
        // TODO: When I implement expanded nodes, copy the TODO below into the expanded node implementation, then remove it here
        // TODO: First make sure that "mutator" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our mutator is now null -- but we failed to check that.
        this.Children |> Array.copyAndPop |> RRBNode<'T>.MkFullNode mutator

and RRBRelaxedNode<'T>(thread : Thread ref, children : RRBNode<'T>[], sizeTable : int[]) =
    inherit RRBFullNode<'T>(thread, children)

    static member Create (thread : Thread ref, shift : int, children : RRBNode<'T>[]) =
        let sizeTbl = RRBNode<'T>.CreateSizeTable shift children
        RRBRelaxedNode<'T>.CreateWithSizeTable(thread, shift, children, sizeTbl)

    static member CreateWithSizeTable (thread : Thread ref, shift : int, children : RRBNode<'T>[], sizeTbl : int[]) =
        if isSizeTableFullAtShift shift sizeTbl then
            RRBFullNode<'T>(thread, children)
        else
            RRBRelaxedNode<'T>(thread, children, sizeTbl) :> RRBFullNode<'T>

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

    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBRelaxedNode<'T>(mutator, Array.copy children, Array.copy sizeTable) :> RRBNode<'T>

    override this.UpdatedChildDifferentSize mutator localIdx newChild sizeDiff =
        let newNode = this.GetEditableNode mutator :?> RRBRelaxedNode<'T>
        newNode.Children.[localIdx] <- newChild
        for i = localIdx to this.NodeSize - 1 do
            newNode.SizeTable.[i] <- this.SizeTable.[i] + sizeDiff
        newNode :> RRBFullNode<'T>

    override this.UpdatedChildSpecificChildSize mutator localIdx newChild childSize =
        let oldChildSize = this.SizeTable.[localIdx]
        this.UpdatedChildDifferentSize mutator localIdx newChild (childSize - oldChildSize)

    override this.SplitNodeIntoArrays mutator shift splitIdx =
        // TODO: Expanded nodes *must* rewrite this so that the right children will be handled correctly (not filled with nulls, since we won't always *want* the right child to end up expanded)
        let leftChildren, rightChildren = this.Children |> Array.splitAt splitIdx
        let leftSizes, rightSizes = this.SizeTable |> Array.splitAt splitIdx
        if splitIdx > 0 then
            let lastLeftSize = Array.last leftSizes
            let lastRightIdx = rightSizes.Length - 1
            for i = 0 to lastRightIdx do
                rightSizes.[i] <- rightSizes.[i] - lastLeftSize
        (leftChildren, leftSizes), (rightChildren, rightSizes)

    override this.RemoveLeftmostChildren mutator shift itemCount =
        // TODO: Document that itemCount of 0 should never happen, and/or make sure it does. This should be an internal function anyway. OR... decide that it's okay, and verify that it's okay.
#if DEBUG
        if itemCount <= 0 then failwith <| sprintf "Item count of %d in RRBFullNode<'T>.RemoveLeftmostChildren -- should never be zero or negative" itemCount
#else
        if itemCount <= 0 then Seq.empty, this :> RRBNode<'T> else
#endif
        // TODO: Triple-check this one because I wrote it in haste. Check that the cumulative size table is correct, for example: make sure it doesn't start with 0
        let newLen = this.NodeSize - itemCount
        let newNode = this.GetEditableEmptyNodeOfLengthN mutator newLen :?> RRBRelaxedNode<'T>
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
    // override this.InsertAndSlideChildrenLeft mutator shift localIdx newChild leftSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSlideChildrenRight mutator shift localIdx newChild rightSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSplitNode mutator shift localIdx newChild =
    //     failwith "Not implemented"

    override this.AppendChild mutator shift newChild childSize =
        let newChildren = this.Children |> Array.copyAndAppend newChild
        let lastSizeTableEntry = if this.SizeTable.Length = 0 then 0 else Array.last this.SizeTable
        let newSizeTable = this.SizeTable |> Array.copyAndAppend (lastSizeTableEntry + childSize)
        RRBNode<'T>.MkNodeKnownSize mutator shift newChildren newSizeTable

    override this.RemoveLastChild mutator shift =
        let children' = this.Children |> Array.copyAndPop
        let sizeTable' = this.SizeTable |> Array.copyAndPop
        RRBNode<'T>.MkNodeKnownSize mutator shift children' sizeTable'

and RRBLeafNode<'T>(thread : Thread ref, items : 'T[]) =
    inherit RRBNode<'T>(thread)

    member this.Items = items

    override this.NodeSize = items.Length
    override this.TreeSize _ = items.Length
    override this.SlotCount =
        failwith "Slot count called on a leaf node"  // TODO: Remove this before going into production, and make it return this.NodeSize
        this.NodeSize
    override this.TwigSlotCount =
        failwith "Twig slot count called on a leaf node"  // TODO: Remove this before going into production, and make it return this.NodeSize
        this.NodeSize

    override this.Shrink() = this :> RRBNode<'T>
    override this.Expand _ = this :> RRBNode<'T>

    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(mutator, Array.copy items) :> RRBNode<'T>

    override this.GetEditableEmptyNodeOfLengthN mutator len =
        if this.IsEditableBy mutator && this.Items.Length = len
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(mutator, Array.zeroCreate len) :> RRBNode<'T>

    abstract member LeafNodeWithItems : Thread ref -> 'T [] -> RRBLeafNode<'T>
    default this.LeafNodeWithItems mutator newItems =
        if this.NodeSize = newItems.Length then  // NOT this.NodeSize here
            let newNode = this.GetEditableNode mutator :?> RRBLeafNode<'T>
            newItems.CopyTo(newNode.Items, 0)
            newNode
        else
            RRBNode<'T>.MkLeaf mutator newItems

    member this.UpdatedItem mutator localIdx newItem =
        let node = this.GetEditableNode mutator :?> RRBLeafNode<'T>
        node.Items.[localIdx] <- newItem
        node :> RRBNode<'T>

    override this.UpdatedTree mutator shift treeIdx newItem =
        this.UpdatedItem mutator treeIdx newItem

    abstract member InsertedItem : Thread ref -> int -> 'T -> RRBLeafNode<'T>
    default this.InsertedItem mutator localIdx item =
        // ExpandedLeafNodes will override this to do an insert in place if they can,
        // but shrunken nodes cannot insert in place so there's no point in checking the mutator in *this* class.
        let newItems = this.Items |> Array.copyAndInsertAt localIdx item
        RRBLeafNode<'T>(mutator, newItems)

    override this.InsertedTree mutator shift treeIdx item parentOpt idxOfNodeInParent =
        if this.NodeSize < Literals.blockSize then
            SimpleInsertion (this.InsertedItem mutator treeIdx item :> RRBNode<'T>)
        else
            let localIdx = treeIdx
            match (parentOpt, idxOfNodeInParent) with
            | Some parent, idx when idx > 0 && parent.Children.[idx - 1].NodeSize < Literals.blockSize ->
                // Room in the left sibling
                let leftSib = parent.Children.[idx - 1] :?> RRBLeafNode<'T>
                let oldLeftLength = leftSib.NodeSize
                let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly (localIdx + oldLeftLength) item leftSib.Items this.Items
                let slidItemCount = newLeftItems.Length - oldLeftLength
                let newLeft  = RRBNode<'T>.MkLeaf mutator newLeftItems :> RRBNode<'T>
                let newRight = this.LeafNodeWithItems mutator newRightItems :> RRBNode<'T>
                SlidItemsLeft (slidItemCount, newLeft, newRight)
            | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                // Room in the right sibling
                let rightSib = parent.Children.[idx + 1] :?> RRBLeafNode<'T>
                let oldRightLength = rightSib.NodeSize
                let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly localIdx item this.Items rightSib.Items
                let slidItemCount = newRightItems.Length - oldRightLength
                // Note that we DON'T use LeafNodeWithItems here: expanded leaves may only be the rightmost leaf in the tree,
                // so if this is an expanded note that would benefit from using LeafNodeWithItems, we should never reach this branch
                let newLeft  = RRBNode<'T>.MkLeaf mutator newLeftItems :> RRBNode<'T>
                let newRight = RRBNode<'T>.MkLeaf mutator newRightItems :> RRBNode<'T>
                SlidItemsRight (slidItemCount, newLeft, newRight)
            | _ ->
                // Don't need to get fancy with SplitNode here, though expanded leaf nodes will want to do something slightly more clever (ensuring that the new right-hand node is still expanded)
                let newLeftItems, newRightItems = Array.insertAndSplitEvenly localIdx item this.Items
                let newLeft  = RRBNode<'T>.MkLeaf mutator newLeftItems :> RRBNode<'T>
                let newRight = this.LeafNodeWithItems mutator newRightItems :> RRBNode<'T>
                SplitNode (newLeft, newRight)

and RRBExpandedLeafNode<'T>(thread : Thread ref, items : 'T[]) =
    inherit RRBLeafNode<'T>(thread, Array.expandToBlockSize items)

    member val CurrentLength : int = Array.length items with get, set
    override this.NodeSize = this.CurrentLength

    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBExpandedLeafNode<'T>(mutator, Array.copy this.Items) :> RRBNode<'T>

    override this.InsertedItem mutator localIdx item =
        // No bounds-checking: that's the job of the caller
        let newNode = this.GetEditableNode mutator :?> RRBExpandedLeafNode<'T>
        Array.blit newNode.Items localIdx newNode.Items (localIdx+1) (newNode.NodeSize - localIdx)
        newNode.Items.[localIdx] <- item
        newNode.CurrentLength <- newNode.NodeSize + 1
        newNode :> RRBLeafNode<'T>

    override this.LeafNodeWithItems mutator newItems =
        let oldNodeLength = this.NodeSize
        let newNode = this.GetEditableNode mutator :?> RRBExpandedLeafNode<'T>
        let newNodeLength = newItems.Length
        newItems.CopyTo(newNode.Items, 0)
        for i = newNodeLength to oldNodeLength - 1 do
            newNode.Items.[i] <- Unchecked.defaultof<'T>
        newNode.CurrentLength <- newNodeLength
        newNode :> RRBLeafNode<'T>
