module rec Ficus.RRBVectorNodes

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
    let inline radixIndex shift treeIdx =
        (treeIdx >>> shift) &&& Literals.blockIndexMask

    // Syntactic sugar for operations we'll use *all the time*: moving up and down the tree levels
    let inline down shift = shift - Literals.blockSizeShift
    let inline up shift = shift + Literals.blockSizeShift

    let isSizeTableFullAtShift shift (sizeTbl : int[]) =
        let len = Array.length sizeTbl
        if len <= 1 then true else
        let checkIdx = len - 2
        sizeTbl.[checkIdx] = ((checkIdx + 1) <<< shift)

    /// Used in replacing leaf nodes
    let copyAndAddNToSizeTable incIdx n oldST =
        let newST = Array.copy oldST
        for i = incIdx to oldST.Length - 1 do
            newST.[i] <- newST.[i] + n
        newST

    let inline copyAndSubtractNFromSizeTable decIdx n oldST =
        copyAndAddNToSizeTable decIdx (-n) oldST

module NodeCreation =
    open RRBMath

    // Unfortunately, we can't move these functions into the Node and RRBNode classes, apparently due to https://stackoverflow.com/q/41746333
    let mkNode (thread : Thread ref) entries = if Array.length entries = 1 then SingletonNode(thread, entries) :> Node else Node(thread, entries)

    let mkRRBNodeWithSizeTable thread shift entries sizeTable =
        if Array.length entries = 1 then SingletonNode(thread, entries) :> Node
        elif isSizeTableFullAtShift shift sizeTable then Node(thread, entries)
        else RRBNode(thread, entries, sizeTable) :> Node

    let treeSize<'T> shift (node : obj) : int =
        if shift <= 0 then
            (node :?> 'T[]).Length
        else
            (node :?> Node).TreeSize<'T> shift

    let createSizeTable<'T> shift (array:obj[]) =
        let sizeTable = Array.zeroCreate array.Length
        let mutable total = 0
        for i = 0 to array.Length - 1 do
            total <- total + treeSize<'T> (down shift) (array.[i])
            sizeTable.[i] <- total
        sizeTable

    let mkRRBNode<'T> thread shift entries = createSizeTable<'T> shift entries |> mkRRBNodeWithSizeTable thread shift entries

    let expandArray (arr : 'T[]) =
        if Array.isEmpty arr || Array.length arr >= Literals.blockSize then arr
        else
            let arr' = Array.zeroCreate Literals.blockSize
            arr.CopyTo(arr', 0)
            arr'

module ConcurrentCounter =
    let internal counter = ref 0

    let getNextID() =
        let result = Interlocked.Increment counter
        // If it ever wraps around to 0 again, we want to skip 0
        if result <> 0 then result else Interlocked.Increment counter

[<StructuredFormatDisplay("Node({StringRepr})")>]
type Node(thread, array : obj[]) =
    let thread = thread
    new() = Node(ref null,Array.create Literals.blockSize null)
    static member InCurrentThread() = Node(ref Thread.CurrentThread, Array.zeroCreate Literals.blockSize)
    member this.Array = array
    member this.Thread = thread
    member this.SetThread t = thread := t
    member this.StringRepr = sprintf "%A" array

    abstract member Shrink : unit -> Node
    default this.Shrink() = this

    abstract member Expand : Thread ref -> Node
    default this.Expand mutator =
        let array' = Array.zeroCreate Literals.blockSize
        this.Array.CopyTo(array', 0)
        ExpandedNode(mutator, this.NodeSize, array') :> Node

    abstract member EnsureEditable : Thread ref -> Node
    default this.EnsureEditable mutator =
        if LanguagePrimitives.PhysicalEquality mutator thread && not (isNull !thread)  // Note that this is NOT "if mutator = thread"
        then this
        else Node(mutator, Array.copy array)

    abstract member ToRRBIfNeeded<'T> : int -> Node
    default this.ToRRBIfNeeded<'T> shift =
        let sizeTable = NodeCreation.createSizeTable<'T> shift array
        if RRBMath.isSizeTableFullAtShift shift sizeTable then this else RRBNode(thread, array, sizeTable) :> Node

    abstract member IndexesAndChild : int -> int -> int * obj * int
    default this.IndexesAndChild shift treeIdx =
        let localIdx = RRBMath.radixIndex shift treeIdx
        let child = array.[localIdx]
        let antimask = ~~~(Literals.blockIndexMask <<< shift)
        let nextTreeIdx = treeIdx &&& antimask
        localIdx, child, nextTreeIdx

    abstract member UpdatedSameSize : Thread ref -> int -> obj -> Node
    default this.UpdatedSameSize mutator localIdx newChild =
        let newNode = this.EnsureEditable mutator
        newNode.Array.[localIdx] <- newChild
        newNode

    member this.UpdatedTwig mutator shift treeIdx (newItem : 'T) =
        let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
        let newNode = this.EnsureEditable mutator
        if LanguagePrimitives.PhysicalEquality newNode this then
            (child :?> 'T[]).[nextIdx] <- newItem
        else
            let newLeaf = (child :?> 'T[]) |> Array.copyAndSet nextIdx newItem
            newNode.Array.[localIdx] <- box newLeaf
        newNode

    member this.UpdatedTree mutator shift treeIdx (newItem : 'T) =
        // TODO: Perhaps this belongs on the tree class instead of the node class?
        if shift <= Literals.blockSizeShift then
            this.UpdatedTwig mutator shift treeIdx newItem
        else
            let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
            let newNode = (child :?> Node).UpdatedTree mutator (RRBMath.down shift) nextIdx newItem
            this.UpdatedSameSize mutator localIdx newNode

    abstract member UpdatedNewSize<'T> : Thread ref -> int -> int -> obj -> int -> Node
    default this.UpdatedNewSize<'T> mutator shift localIdx newChild childSize =
        // Note: childSize should be *tree* size, not *node* size. In other words, something appropriate for the size table at this level.
        if childSize = (1 <<< shift) then
            // This is still a full node
            this.UpdatedSameSize mutator localIdx newChild
        else
            // This has probably become an RRB node (unless the updated child was the last child)
            let newNode = this.EnsureEditable mutator
            newNode.Array.[localIdx] <- newChild
            newNode.ToRRBIfNeeded<'T> shift
        // TODO: Can this be simplified into two lines as follows?
        // let newNode = this.UpdatedSameSize mutator localIdx newChild
        // if childSize = (1 <<< shift) then newNode else newNode.ToRRBIfNeeded<'T> shift

    abstract member NodeSize : int
    default this.NodeSize = array.Length

    // Get an array containing only the *valid* children of this node (subclasses like ExpandedNode might need to override).
    // Use IterChildren instead if possible to avoid array creation.
    abstract member Children : obj[]
    default this.Children = array

    abstract member IterChildren : unit -> seq<obj>
    default this.IterChildren() = array |> Seq.ofArray

    abstract member RevIterChildren : unit -> seq<obj>
    default this.RevIterChildren() = seq { for i = this.NodeSize - 1 downto 0 do yield this.Array.[i] }

    abstract member TreeSize<'T> : int -> int
    default this.TreeSize<'T> shift =
        // A full node is allowed to have an incomplete rightmost entry, but all but its rightmost entry must be complete.
        // Therefore, we can shortcut this calculation for most of the nodes, but we do need to calculate the rightmost node.
        if shift <= Literals.blockSizeShift then
            ((this.NodeSize - 1) <<< shift) + ((array.[this.NodeSize - 1]) :?> 'T[]).Length
        else
            ((this.NodeSize - 1) <<< shift) + ((array.[this.NodeSize - 1]) :?> Node).TreeSize<'T> (RRBMath.down shift)

    abstract member TwigSlotCount<'T> : unit -> int
    default this.TwigSlotCount<'T>() =
        if this.NodeSize = 0 then
            // failwith "Deliberate failure: TwigSlotCount should never be called on an empty Node instance"
            // Nope, it can happen
            0
        else
            ((this.NodeSize - 1) <<< Literals.blockSizeShift) + ((array.[this.NodeSize - 1]) :?> 'T[]).Length

    // Uses "this.Array" instead of "array" because of error FS1113. TODO: Is there a real speed benefit to making this an inline function? If not, just make it a normal method.
    member this.FullNodeIsTrulyFull<'T> shift =
        shift < Literals.blockSizeShift || this.NodeSize = 0 || NodeCreation.treeSize<'T> (RRBMath.down shift) (this.Array.[this.NodeSize - 1]) >= (1 <<< shift)

    // Assumes that the node does *not* yet have blockSize children; verifying that is the job of the caller function
    abstract member AppendChild<'T> : Thread ref -> int -> obj -> int -> Node
    default this.AppendChild<'T> mutator shift newChild childSize =
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if this.FullNodeIsTrulyFull<'T> shift then
            array |> Array.copyAndAppend newChild |> NodeCreation.mkNode mutator
        else
            array |> Array.copyAndAppend newChild |> NodeCreation.mkRRBNode<'T> mutator shift

    abstract member RemoveLastChild<'T> : Thread ref -> int -> Node
    default this.RemoveLastChild<'T> mutator shift =
        // Expanded nodes can do this in a transient way, but "normal" nodes can't
        // TODO: First make sure that "mutator" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our mutator is now null -- but we failed to check that.
        array |> Array.copyAndPop |> NodeCreation.mkNode mutator

    abstract member ConcatOtherNode<'T> : Thread ref -> int -> Node -> Node
    default this.ConcatOtherNode<'T> mutator shift otherNode =
        // Concatenate the other node to the right-hand side of this one
        // It's the calling function's responsibility to ensure array size is small enough
        // Expanded nodes can do this in a transient way, but "normal" nodes can't
        Array.append array otherNode.Array |> NodeCreation.mkRRBNode<'T> mutator shift  // FIXME: We currently don't consider whether otherNode might be an expanded node
        // TODO: Put rebalancing logic in here as well; this will become the node equivalent of mergeArrays<'T>

    abstract member RemoveLastLeaf<'T> : Thread ref -> int -> 'T[] * Node
    default this.RemoveLastLeaf<'T> mutator shift =
        if shift <= 0 then
            failwith "Deliberate failure at shift 0 or less"  // This proves that we're not actually using this code branch
            // TODO: Keep that deliberate failure line in here until we're *completely* finished with refactoring, in case we end up needing
            // to use removeLastLeaf for anything else. Then once we're *completely* done refactoring, delete this unnecessary if branch.
        elif shift <= Literals.blockSizeShift then
            // Children of this node are leaves
            if this.NodeSize = 0 then Array.empty, this
            else
                let leaf = (array.[this.NodeSize - 1]) :?> 'T []
                let newNode = this.RemoveLastChild<'T> mutator shift // Popping the last entry from a FullNode can't ever turn it into an RRBNode.
                leaf, newNode
        else
            // Children are nodes, not leaves -- so we're going to take the rightmost and dig down into it.
            // And if the recursive call returns an empty node, we'll strip the entry off our node.
            let leaf, child' = (array.[this.NodeSize - 1] :?> Node).RemoveLastLeaf<'T> mutator (RRBMath.down shift)
            let newNode =
                if child'.NodeSize = 0
                then this.RemoveLastChild<'T> mutator shift
                else
                    let childSize' = child'.TreeSize<'T> (RRBMath.down shift)  // Or, wait, should this be treesize (down shift)? TODO: Find out
                    this.UpdatedNewSize<'T> mutator shift (this.NodeSize - 1) child' childSize' // array |> Array.copyAndSetLast (box child') |> NodeCreation.mkNode mutator
            leaf, newNode

    // Used in appendLeafWithGrowth, which creates the new path and then calls us to create a new "root" node above us, with us as the left
    abstract member PushRootUp : int -> int -> Node -> Node
    default this.PushRootUp shift leafLen (newRight : Node) =
        // Don't need shift or leafLen for ordinary nodesm only for RRBNodes
        NodeCreation.mkNode thread [|box this; box newRight|]

    abstract member TakeChildren : Thread ref -> int -> int -> Node
    default this.TakeChildren mutator shift n =
        array |> Array.truncate n |> NodeCreation.mkNode mutator // TODO: Implement this in descendant types, because we want to be able to Take and Skip on transients

    abstract member SkipChildren : Thread ref -> int -> int -> Node
    default this.SkipChildren mutator shift n =
        array |> Array.skip n |> NodeCreation.mkNode mutator // TODO: Implement this in descendant types, because we want to be able to Take and Skip on transients


[<StructuredFormatDisplay("ExpandedNode({StringRepr})")>]
type ExpandedNode(thread, realLength : int, array : obj[]) =
    inherit Node(thread, array)
    let thread = thread
    member val CurrentLength = realLength with get, set
    new() = ExpandedNode(ref null, 0, Array.create Literals.blockSize null)
    static member InCurrentThread() = ExpandedNode(ref Thread.CurrentThread, 0, Array.zeroCreate Literals.blockSize)
    member this.Array = array
    member this.Thread = thread
    member this.SetThread t = thread := t
    member this.StringRepr = sprintf "%A" array

    override this.Shrink() = Node(thread, this.Array |> Array.truncate this.NodeSize)
    override this.Expand mutator = this.EnsureEditable mutator

    override this.NodeSize = this.CurrentLength

    override this.Children = if this.NodeSize = this.Array.Length then this.Array else this.Array |> Array.truncate this.NodeSize

    override this.IterChildren() = this.Array |> Seq.ofArray |> Seq.truncate this.NodeSize

    override this.EnsureEditable mutator =
        if LanguagePrimitives.PhysicalEquality mutator thread && not (isNull !thread)  // Note that this is NOT "if mutator = thread"
        then this :> Node
        else ExpandedNode(mutator, this.CurrentLength, Array.copy array) :> Node

    member this.UpdatedTree mutator shift treeIdx (newItem : 'T) =
        // TODO: Perhaps this belongs on the tree class instead of the node class?
        if shift <= Literals.blockSizeShift then
            let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
            let newLeaf = (child :?> 'T[]) |> Array.copyAndSet nextIdx newItem |> box
            this.UpdatedSameSize mutator localIdx newLeaf
        else
            let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
            let newNode = (child :?> Node).UpdatedTree mutator (RRBMath.down shift) nextIdx newItem
            this.UpdatedSameSize mutator localIdx newNode

    override this.UpdatedNewSize<'T> mutator shift localIdx newChild childSize =
        // Note: childSize should be *tree* size, not *node* size. In other words, something appropriate for the size table at this level.
        if childSize = (1 <<< shift) then
            // This is still a full node
            NodeCreation.mkNode thread (array |> Array.copyAndSet localIdx newChild)
        else
            // This has become an RRB node, so recalculate the size table via mkRRBNode
            array |> Array.copyAndSet localIdx newChild |> NodeCreation.mkRRBNode<'T> thread shift

    override this.TwigSlotCount<'T>() =
        if this.CurrentLength = 0 then
            // failwith "Deliberate failure: TwigSlotCount should never be called on an empty Node instance"
            // Nope, it can happen
            0
        else
            ((this.CurrentLength - 1) <<< Literals.blockSizeShift) + ((array.[this.CurrentLength - 1]) :?> 'T[]).Length

    // Uses "this.Array" instead of "array" because of error FS1113. TODO: Is there a real speed benefit to making this an inline function? If not, just make it a normal method.
    member inline this.FullNodeIsTrulyFull<'T> shift =
        // TODO: Is this really needed? Can't we just inherit from parent class?
        shift < Literals.blockSizeShift || this.NodeSize = 0 || NodeCreation.treeSize<'T> (RRBMath.down shift) (this.Array.[this.NodeSize - 1]) >= (1 <<< shift)

    // Assumes that the node does *not* yet have blockSize children; verifying that is the job of the caller function
    override this.AppendChild<'T> mutator shift newChild childSize =
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        let newNode = this.EnsureEditable mutator :?> ExpandedNode
        newNode.Array.[newNode.NodeSize] <- newChild
        newNode.CurrentLength <- newNode.CurrentLength + 1
        if newNode.FullNodeIsTrulyFull<'T> shift then newNode :> Node else newNode.ToRRBIfNeeded<'T> shift

    override this.RemoveLastChild<'T> mutator shift =
        let newNode = this.EnsureEditable mutator :?> ExpandedNode
        newNode.Array.[newNode.CurrentLength - 1] <- null
        newNode.CurrentLength <- newNode.CurrentLength - 1
        newNode :> Node

    // TODO: Implement this (if it turns out to be useful)
    // override this.ConcatOtherNode<'T> mutator shift otherNode =
    //     // Concatenate the other node to the right-hand side of this one
    //     // It's the calling function's responsibility to ensure array size is small enough
    //     // Only expanded nodes will actually do this in a transient way
    //     Array.append array otherNode.Array |> NodeCreation.mkRRBNode<'T> mutator shift
    //     // TODO: Put rebalancing logic in here as well; this will become the node equivalent of mergeArrays<'T>

    // Used in appendLeafWithGrowth, which creates the new path and then calls us to create a new "root" node above us, with us as the left
    override this.PushRootUp shift leafLen (newRight : Node) =
        // Don't need shift or leafLen for ordinary nodes, only for RRBNodes
        NodeCreation.mkNode thread [|box this; box newRight|]

    override this.TakeChildren mutator shift n =
#if DEBUG
        // Calling code is responsible for ensuring that n <= this.NodeSize, but we'll check this in a debug build
        if n > this.NodeSize then failwithf "TakeChildren called with n (%d) > this.NodeSize (%d) on node %A" n this.NodeSize this.Array
#endif
        let newNode = this.EnsureEditable mutator
        for i = n to newNode.NodeSize - 1 do
            newNode.Array.[i] <- null
        (newNode :?> ExpandedNode).CurrentLength <- n
        newNode

    override this.SkipChildren mutator shift n =
#if DEBUG
        // Calling code is responsible for ensuring that n <= this.NodeSize, but we'll check this in a debug build
        if n > this.NodeSize then failwithf "SkipChildren called with n (%d) > this.NodeSize (%d) on node %A" n this.NodeSize this.Array
#endif
        let newSize = this.NodeSize - n
        let arr' = Array.zeroCreate Literals.blockSize
        Array.blit array n arr' 0 newSize
        ExpandedNode(mutator, newSize, arr') :> Node

[<StructuredFormatDisplay("SingletonNode({StringRepr})")>]
type SingletonNode(thread, array) =
    inherit Node(thread, array)
    static member InCurrentThread() = SingletonNode(ref Thread.CurrentThread, Array.zeroCreate 1)

    override this.NodeSize = 1

    override this.AppendChild<'T> mutator shift newChild childSize =
        let canStayFull =
            if shift <= Literals.blockSizeShift
            then (array.[0] :?> 'T[]).Length = Literals.blockSize
            else (array.[0] :?> Node).NodeSize = Literals.blockSize && not (array.[0] :? RRBNode)
        if canStayFull then
            Node(thread, [|array.[0]; newChild|])
        else
            let firstChildTreeSize = (array.[0] :?> Node).TreeSize<'T> shift
            RRBNode(thread, [|array.[0]; newChild|], [|firstChildTreeSize; firstChildTreeSize + childSize|]) :> Node

    override this.IndexesAndChild _ treeIdx =  // No need for radix math when we know that there's just one child
        0, array.[0], treeIdx

    override this.UpdatedSameSize mutator localIdx newChild =
        if LanguagePrimitives.PhysicalEquality mutator thread && not (isNull !thread)
        then array.[0] <- newChild; this :> Node
        else SingletonNode(mutator, Array.singleton newChild) :> Node

    override this.UpdatedNewSize<'T> mutator shift localIdx newChild childSize =
        this.UpdatedSameSize mutator localIdx newChild  // SingletonNodes are guaranteed not to become RRB so we can save time

    override this.TakeChildren mutator shift n =
        if n = 0 then Node(mutator, Array.empty) else this :> Node

    override this.SkipChildren mutator shift n =
        if n = 0 then this :> Node else Node(mutator, Array.empty)

[<StructuredFormatDisplay("RRBNode({StringRepr})")>]
type RRBNode(thread, array, sizeTable : int[]) =
    inherit Node(thread, array)
    static member InCurrentThread() = RRBNode(ref Thread.CurrentThread, Array.zeroCreate Literals.blockSize, Array.zeroCreate Literals.blockSize)
    member this.SizeTable = sizeTable
    member this.StringRepr = sprintf "sizeTable=%A,children=%A" sizeTable array
    override this.IndexesAndChild shift treeIdx =
        let mutable localIdx = RRBMath.radixIndex shift treeIdx
        while sizeTable.[localIdx] <= treeIdx do
            localIdx <- localIdx + 1
        let child = array.[localIdx]
        let nextTreeIdx = if localIdx = 0 then treeIdx else treeIdx - sizeTable.[localIdx - 1]
        localIdx, child, nextTreeIdx

    override this.Expand mutator =
        // TODO: Optimize for the case where: a) the mutator is the same as our thread ref, and b) we're already at size Literals.blockSize; in which case we don't have to copy.
        // TODO: But first, consider: can that ever happen? Or is this.Expand() only called in the process of creating a transient, in which case all old thread refs would be invalidated anyway?
        let array' = Array.zeroCreate Literals.blockSize
        let sizeTable' = Array.zeroCreate Literals.blockSize
        this.Array.CopyTo(array', 0)
        this.SizeTable.CopyTo(sizeTable', 0)
        ExpandedRRBNode(mutator, this.NodeSize, array', sizeTable') :> Node

    override this.EnsureEditable mutator =
        if LanguagePrimitives.PhysicalEquality mutator thread && not (isNull !thread)  // Note that this is NOT "if mutator = thread"
        then this :> Node
        else RRBNode(mutator, Array.copy array, Array.copy sizeTable) :> Node

    override this.UpdatedSameSize mutator localIdx newChild =
        let newNode = this.EnsureEditable mutator
        newNode.Array.[localIdx] <- newChild
        RRBNode(thread, array |> Array.copyAndSet localIdx newChild, sizeTable) :> Node

    override this.TreeSize shift = sizeTable.[this.NodeSize - 1]

    override this.TwigSlotCount<'T>() =
        if array.Length = 0 then
            failwith "Deliberate failure: TwigSlotCount should never be called on an empty RRBNode instance"
        else
            Array.last sizeTable

    override this.UpdatedNewSize mutator shift localIdx newChild childSize =
        let oldSize = if localIdx = 0 then sizeTable.[0] else sizeTable.[localIdx] - sizeTable.[localIdx - 1]
        let sizeDiff = childSize - oldSize
        let array' = array |> Array.copyAndSet localIdx newChild
        let sizeTable' = if sizeDiff = 0 then sizeTable else sizeTable |> RRBMath.copyAndAddNToSizeTable localIdx sizeDiff
        NodeCreation.mkRRBNodeWithSizeTable thread shift array' sizeTable'

    override this.AppendChild<'T> mutator shift newChild childSize =
        let array' = array |> Array.copyAndAppend newChild
        let lastSizeTableEntry = if sizeTable.Length = 0 then 0 else sizeTable |> Array.last
        let sizeTable' = sizeTable |> Array.copyAndAppend (childSize + lastSizeTableEntry)
        NodeCreation.mkRRBNodeWithSizeTable thread shift array' sizeTable'

    override this.RemoveLastChild mutator shift =
        let array' = array |> Array.copyAndPop
        let sizeTable' = sizeTable |> Array.copyAndPop
        NodeCreation.mkRRBNodeWithSizeTable mutator shift array' sizeTable'

    // COMMENTED OUT 2018-03-28 by RM because I don't think we need this override any more, now that we've done RemoveLastChild correctly.
    // If that's truly the case (after implementing the expanded-node versions), then make this function no longer be abstract/virtual
    // override this.RemoveLastLeaf<'T> mutator shift =
    //     if shift <= 0 then
    //         failwith "Deliberate failure at shift 0 or less"  // This proves that we're not actually using this code branch
    //         // TODO: Keep that deliberate failure line in here until we're *completely* finished with refactoring, in case we end up needing
    //         // to use removeLastLeaf for anything else. Then once we're *completely* done refactoring, delete this unnecessary if branch.
    //     elif shift <= Literals.blockSizeShift then
    //         // Children of this node are leaves
    //         // TODO: Is there some way we could not have this code block duplicated?
    //         if array.Length = 0 then Array.empty, this :> Node else
    //         let leaf = (array.[this.NodeSize - 1]) :?> 'T []
    //         let newNode = this.RemoveLastChild<'T> mutator shift
    //         leaf, newNode
    //     else
    //         // Children are nodes, not leaves -- so we're going to take the rightmost and dig down into it.
    //         // And if the recursive call returns an empty node, we'll strip the entry off our node.
    //         let leaf, child' = (array.[this.NodeSize - 1] :?> Node).RemoveLastLeaf<'T> mutator (RRBMath.down shift)
    //         let newNode =
    //             if child'.Array.Length = 0
    //             then this.RemoveLastChild<'T> mutator shift
    //             else array |> Array.copyAndSetLast (box child') |> NodeCreation.mkRRBNode<'T> thread shift
    //         leaf, newNode

    override this.PushRootUp shift leafLen (newRight : Node) =
        let oldSize = Array.last sizeTable
        NodeCreation.mkRRBNodeWithSizeTable thread (RRBMath.up shift) [|box this; box newRight|] [|oldSize; oldSize + leafLen|]

    override this.TakeChildren mutator shift n =
#if DEBUG
        // Calling code is responsible for ensuring that n <= this.NodeSize, but we'll check this in a debug build
        if n > this.NodeSize then failwithf "TakeChildren called with n (%d) > this.NodeSize (%d) on node %A" n this.NodeSize this.Array
#endif
        let array' = array |> Array.truncate n
        let sizeTable' = sizeTable |> Array.truncate n
        NodeCreation.mkRRBNodeWithSizeTable mutator shift array' sizeTable'

    override this.SkipChildren mutator shift n =
#if DEBUG
        // Calling code is responsible for ensuring that n <= this.NodeSize, but we'll check this in a debug build
        if n > this.NodeSize then failwithf "SkipChildren called with n (%d) > this.NodeSize (%d) on node %A" n this.NodeSize this.Array
#endif
        let array' = array |> Array.skip n
        let sizeTable' =
            if n >= this.NodeSize then Array.empty
            else
                let diff = sizeTable.[n]
                sizeTable |> Array.skip n |> Array.map (fun size -> size - diff)
        NodeCreation.mkRRBNodeWithSizeTable mutator shift array' sizeTable'


[<StructuredFormatDisplay("ExpandedNode({StringRepr})")>]
type ExpandedRRBNode(thread : Thread ref, realLength : int, array : obj[], sizeTable : int[]) =
    inherit RRBNode(thread, array, sizeTable)
    let thread = thread
    member val CurrentLength = realLength with get, set
    new() = ExpandedRRBNode(ref null, 0, Array.zeroCreate Literals.blockSize, Array.zeroCreate Literals.blockSize)
    static member InCurrentThread() = Node(ref Thread.CurrentThread, Array.zeroCreate Literals.blockSize)
    member this.Array = array
    member this.Thread = thread
    member this.SetThread t = thread := t
    member this.StringRepr = sprintf "%A" array

    override this.NodeSize = this.CurrentLength

    override this.Children = if this.NodeSize = this.Array.Length then this.Array else this.Array |> Array.truncate this.NodeSize

    override this.IterChildren() = this.Array |> Seq.ofArray |> Seq.truncate this.NodeSize

    override this.Shrink() = RRBNode(thread, this.Array |> Array.truncate this.NodeSize, this.SizeTable |> Array.truncate this.NodeSize) :> Node
    override this.Expand mutator = this.EnsureEditable mutator

    override this.EnsureEditable mutator =
        if LanguagePrimitives.PhysicalEquality mutator thread && not (isNull !thread)  // Note that this is NOT "if mutator = thread"
        then this :> Node
        else ExpandedRRBNode(mutator, this.CurrentLength, Array.copy array, Array.copy sizeTable) :> Node

    override this.UpdatedSameSize mutator localIdx newChild = Node(thread, array |> Array.copyAndSet localIdx newChild)

    member this.UpdatedTree mutator shift treeIdx (newItem : 'T) =
        // // TODO: Perhaps this belongs on the tree class instead of the node class?
        if shift <= Literals.blockSizeShift then
            let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
            let newLeaf = (child :?> 'T[]) |> Array.copyAndSet nextIdx newItem |> box
            this.UpdatedSameSize mutator localIdx newLeaf
        else
            let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
            let newNode = (child :?> Node).UpdatedTree mutator (RRBMath.down shift) nextIdx newItem
            this.UpdatedSameSize mutator localIdx newNode

    override this.UpdatedNewSize<'T> mutator shift localIdx newChild childSize =
        // // Note: childSize should be *tree* size, not *node* size. In other words, something appropriate for the size table at this level.
        // if childSize = (1 <<< shift) then
        //     // This is still a full node
        //     NodeCreation.mkNode thread (array |> Array.copyAndSet localIdx newChild)
        // else
        //     // This has become an RRB node, so recalculate the size table via mkRRBNode
        //     array |> Array.copyAndSet localIdx newChild |> NodeCreation.mkRRBNode<'T> thread shift
        let oldSize = if localIdx = 0 then sizeTable.[0] else sizeTable.[localIdx] - sizeTable.[localIdx - 1]
        let sizeDiff = childSize - oldSize
        let newNode = this.EnsureEditable mutator :?> RRBNode
        newNode.Array.[localIdx] <- newChild
        if sizeDiff <> 0 then
            for i = localIdx to this.NodeSize - 1 do
                newNode.SizeTable.[i] <- newNode.SizeTable.[i] + sizeDiff
        newNode :> Node

    override this.TwigSlotCount<'T>() =
        if this.CurrentLength = 0 then 0 else sizeTable.[this.CurrentLength - 1]

    // Assumes that the node does *not* yet have blockSize children; verifying that is the job of the caller function
    override this.AppendChild<'T> mutator shift newChild childSize =
        let lastSizeTableEntry = if this.NodeSize = 0 then 0 else sizeTable.[this.NodeSize - 1]
        let newNode = this.EnsureEditable mutator :?> ExpandedRRBNode
        newNode.Array.[newNode.CurrentLength] <- newChild
        newNode.SizeTable.[newNode.CurrentLength] <- lastSizeTableEntry + childSize
        newNode.CurrentLength <- newNode.CurrentLength + 1
        newNode :> Node

    override this.RemoveLastChild<'T> mutator shift =
        let newNode = this.EnsureEditable mutator :?> ExpandedRRBNode
        newNode.CurrentLength <- newNode.CurrentLength - 1
        newNode.Array.[newNode.CurrentLength] <- null
        newNode.SizeTable.[newNode.CurrentLength] <- 0
        newNode :> Node

    // Used in appendLeafWithGrowth, which creates the new path and then calls us to create a new "root" node above us, with us as the left
    override this.PushRootUp shift leafLen (newRight : Node) =
        // // Don't need shift or leafLen for ordinary nodes, only for RRBNodes
        // NodeCreation.mkNode thread [|box this; box newRight|]
        let oldSize = sizeTable.[this.NodeSize - 1]
        // This is an actual use case for InCurrentThread()... so write it correctly, then use it here. TODO
        let newRoot = ExpandedRRBNode(thread, 2, Array.zeroCreate Literals.blockSize, Array.zeroCreate Literals.blockSize)
        newRoot.Array.[0] <- box this
        newRoot.Array.[1] <- box newRight
        newRoot.SizeTable.[0] <- oldSize
        newRoot.SizeTable.[1] <- oldSize + leafLen
        newRoot :> Node

    override this.TakeChildren mutator shift n =
#if DEBUG
        // Calling code is responsible for ensuring that n <= this.NodeSize, but we'll check this in a debug build
        if n > this.NodeSize then failwithf "TakeChildren called with n (%d) > this.NodeSize (%d) on node %A" n this.NodeSize this.Array
#endif
        let newNode = this.EnsureEditable mutator :?> ExpandedRRBNode
        for i = n to newNode.NodeSize - 1 do
            newNode.Array.[i] <- null
            newNode.SizeTable.[i] <- 0
        newNode.CurrentLength <- n
        newNode :> Node

    override this.SkipChildren mutator shift n =
#if DEBUG
        // Calling code is responsible for ensuring that n <= this.NodeSize, but we'll check this in a debug build
        if n > this.NodeSize then failwithf "SkipChildren called with n (%d) > this.NodeSize (%d) on node %A" n this.NodeSize this.Array
#endif
        let newSize = this.NodeSize - n
        let array' = Array.zeroCreate Literals.blockSize
        let sizeTable' = Array.zeroCreate Literals.blockSize
        Array.blit array n array' 0 newSize
        Array.blit sizeTable n sizeTable' 0 newSize
        let diff = sizeTable'.[0]
        for i = 0 to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] - diff
        NodeCreation.mkRRBNodeWithSizeTable mutator shift array' sizeTable'



