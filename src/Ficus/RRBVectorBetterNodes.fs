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

    let findMergeCandidates (sizeSeq : #seq<int>) len =
        use e = sizeSeq.GetEnumerator()
        let sizes = Array.init len (fun _ -> byte (if e.MoveNext() then Literals.blockSize - e.Current else 0))
        sizes |> Array.smallestRunGreaterThan (byte Literals.blockSize)

    // TODO: At some point, uncomment this version and test whether it is more efficient
    // let findMergeCandidatesTwoPasses (sizeSeq : #seq<int>) len =
    //     use e = sizeSeq.GetEnumerator()
    //     let sizes = Array.init len (fun _ -> byte (if e.MoveNext() then Literals.blockSize - e.Current else 0))
    //     let idx1, len1 = sizes |> smallestRunGreaterThan (byte Literals.blockSize)
    //     let idx2, len2 = sizes |> smallestRunGreaterThan (byte (Literals.blockSize <<< 1))
    //     // Drop two slots if we can do so in less than twice the work needed to drop a single slot
    //     if len2 < (len1 * 2) then
    //         idx2, len2, 2
    //     else
    //         idx1, len1, 1


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

    abstract member SetNodeSize : int -> unit

    abstract member GetEditableNode : OwnerToken -> RRBNode<'T>
    abstract member GetEditableEmptyNodeOfLengthN : OwnerToken -> int -> RRBNode<'T>

    member this.IsEditableBy (owner : OwnerToken) =
        LanguagePrimitives.PhysicalEquality owner ownerToken && not (isNull !ownerToken)
        // Note that this test is NOT "if owner = owner".

    static member CreateSizeTableS (shift : int) (array:RRBNode<'T>[]) (len : int) : int[] =
        let sizeTable = Array.zeroCreate len
        let mutable total = 0
        for i = 0 to len - 1 do
            total <- total + (array.[i]).TreeSize (down shift)
            sizeTable.[i] <- total
        sizeTable

    static member CreateSizeTable (shift : int) (array:RRBNode<'T>[]) : int[] =
        RRBNode<'T>.CreateSizeTableS shift array array.Length

    static member MkLeaf (owner : OwnerToken) (items : 'T[]) = RRBLeafNode<'T>(owner, items)
    static member MkNode (owner : OwnerToken) (shift : int) (children : RRBNode<'T>[]) =
        RRBRelaxedNode<'T>.Create(owner, shift, children)
    static member MkNodeKnownSize (owner : OwnerToken) (shift : int) (children : RRBNode<'T>[]) (sizeTable : int[]) =
        RRBRelaxedNode<'T>.CreateWithSizeTable(owner, shift, children, sizeTable)
    static member MkFullNode (owner : OwnerToken) (children : RRBNode<'T>[]) =
        // if children.Length = 1 then SingletonNode<'T>(owner, entries) :> Node<'T> else  // TODO: Do we want an RRBSingletonNode class as well? ANSWER: No we don't; remove this TODO
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

    override this.SetNodeSize _ = ()  // No-op; only used in expanded nodes

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
    override this.Expand owner =
        let node' = this.GetEditableNode owner :?> RRBFullNode<'T>
        RRBExpandedFullNode<'T>(owner, node'.Children) :> RRBNode<'T>

    abstract member ToRelaxedNodeIfNeeded : int -> RRBNode<'T>
    default this.ToRelaxedNodeIfNeeded shift =
        // TODO: Use this.BuildSizeTable instead???
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

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    abstract member AppendChild : OwnerToken -> int -> RRBNode<'T> -> RRBFullNode<'T>
    default this.AppendChild owner shift newChild =
        let newChildren = this.Children |> Array.copyAndAppend newChild
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if this.FullNodeIsTrulyFull shift then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // Last item wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member AppendChildS : OwnerToken -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.AppendChildS owner shift newChild _newChildSize =
        this.AppendChild owner shift newChild

    abstract member InsertChild : OwnerToken -> int -> int -> RRBNode<'T> -> RRBFullNode<'T>
    default this.InsertChild owner shift localIdx newChild =
        if localIdx = this.NodeSize then this.AppendChild owner shift newChild else
        let newChildren = this.Children |> Array.copyAndInsertAt localIdx newChild
        let newChildSize = newChild.TreeSize (down shift)
        if newChildSize >= (1 <<< shift) then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // New child wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member InsertChildS : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let newChildren = this.Children |> Array.copyAndInsertAt localIdx newChild
        if newChildSize >= (1 <<< shift) then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // New child wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member RemoveChild : OwnerToken -> int -> int -> RRBFullNode<'T>
    default this.RemoveChild owner shift localIdx =
        let newChildren = this.Children |> Array.copyAndRemoveAt localIdx
        // Removing a child from a full node can never make it become not-full
        RRBNode<'T>.MkFullNode owner newChildren

    abstract member RemoveLastChild : OwnerToken -> int -> RRBFullNode<'T>
    default this.RemoveLastChild owner shift =
        // Expanded nodes can do this in a transient way, but "normal" nodes can't
        this.Children |> Array.copyAndPop |> RRBNode<'T>.MkFullNode owner

    // TODO: Would RemoveChildS ever be useful?
    // abstract member RemoveChildS : OwnerToken -> int -> int -> int -> RRBFullNode<'T>
    // override this.RemoveChildS owner shift localIdx oldChildSize =
    //     this

    member this.UpdateChild owner shift localIdx newChild =
        // TODO: If we need to override this in any child class, make it virtual (e.g. "abstract Foo" and then "default this.Foo")
        let node = this.GetEditableNode owner :?> RRBFullNode<'T>
        node.Children.[localIdx] <- newChild
        node

    abstract member UpdateChildSRel : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.UpdateChildSRel owner shift localIdx newChild sizeDiff =
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let node' = RRBRelaxedNode<'T>(this.Owner, this.Children, sizeTable)
        node'.UpdateChildSRel owner shift localIdx newChild sizeDiff

    abstract member UpdateChildSAbs : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.UpdateChildSAbs owner shift localIdx newChild childSize =
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let node' = RRBRelaxedNode<'T>(this.Owner, this.Children, sizeTable)
        node'.UpdateChildSAbs owner shift localIdx newChild childSize

    abstract member KeepNLeft : OwnerToken -> int -> int -> RRBFullNode<'T>
    default this.KeepNLeft owner shift n =
        let arr' = this.Children |> Array.truncate n
        RRBFullNode<'T>(owner, arr')
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    abstract member KeepNRight : OwnerToken -> int -> int -> RRBFullNode<'T>
    default this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
        let arr' = this.Children |> Array.skip skip
        RRBFullNode<'T>(owner, arr')
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    abstract member SplitAndKeepNLeft : OwnerToken -> int -> int -> (RRBFullNode<'T> * RRBNode<'T> [])
    default this.SplitAndKeepNLeft owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeft, n must be at least one, got %d. This node: %A" n this
#endif
        let l, r = this.Children |> Array.splitAt n
        let node' = RRBFullNode<'T>(owner, l)
        (node', r)

    abstract member SplitAndKeepNLeftS : OwnerToken -> int -> int -> (RRBFullNode<'T> * (RRBNode<'T> [] * int []))
    default this.SplitAndKeepNLeftS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeftS, n must be at least one, got %d. This node: %A" n this
#endif
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let l, r = this.Children |> Array.splitAt n
        let lS, rS = sizeTable |> Array.splitAt n
        let lastSize = lS |> Array.last
        for i = 0 to rS.Length - 1 do
            rS.[i] <- rS.[i] - lastSize
        let node' = RRBFullNode<'T>(owner, l)
        (node', (r, rS))

    abstract member SplitAndKeepNRight : OwnerToken -> int -> int -> (RRBNode<'T> [] * RRBFullNode<'T>)
    default this.SplitAndKeepNRight owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRight, n must be at least one, got %d. This node: %A" n this
#endif
        let skip = this.NodeSize - n
        let l, r = this.Children |> Array.splitAt skip
        let node' = RRBFullNode<'T>(owner, r)
        (l, node')

    abstract member SplitAndKeepNRightS : OwnerToken -> int -> int -> ((RRBNode<'T> [] * int []) * RRBFullNode<'T>)
    default this.SplitAndKeepNRightS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRightS, n must be at least one, got %d. This node: %A" n this
#endif
        let size = this.NodeSize
        let skip = size - n
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let l, r = this.Children |> Array.splitAt skip
        let lS, rS = sizeTable |> Array.splitAt skip
        // No need to adjust rS here since we don't use it
        let node' = RRBFullNode<'T>(owner, r)
        ((l, lS), node')

    abstract member AppendNChildren : OwnerToken -> int -> int -> RRBNode<'T> seq -> RRBFullNode<'T>
    default this.AppendNChildren owner shift n newChildren =
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', 0)
        let sizeTable = this.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate newSize
        sizeTable.CopyTo(sizeTable', 0)
        let mutable prevSizeTableEntry = sizeTable'.[size - 1]
        use eC = newChildren.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() then
                children'.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                sizeTable'.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    abstract member AppendNChildrenS : OwnerToken -> int -> int -> RRBNode<'T> seq -> int seq -> RRBFullNode<'T>
    default this.AppendNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', 0)
        let sizeTable = this.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate newSize
        sizeTable.CopyTo(sizeTable', 0)
        let lastSizeTableEntry = sizeTable'.[size - 1]
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                children'.[i] <- eC.Current
                sizeTable'.[i] <- lastSizeTableEntry + eS.Current
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    abstract member PrependNChildren : OwnerToken -> int -> int -> RRBNode<'T> seq -> RRBFullNode<'T>
    default this.PrependNChildren owner shift n newChildren =
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', n)
        let sizeTable = this.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate newSize
        sizeTable.CopyTo(sizeTable', n)
        let mutable prevSizeTableEntry = 0
        use eC = newChildren.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() then
                children'.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                sizeTable'.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    abstract member PrependNChildrenS : OwnerToken -> int -> int -> RRBNode<'T> seq -> int seq -> RRBFullNode<'T>
    default this.PrependNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', n)
        let sizeTable = this.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate newSize
        sizeTable.CopyTo(sizeTable', n)
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                children'.[i] <- eC.Current
                sizeTable'.[i] <- eS.Current
        let lastSizeTableEntry = sizeTable'.[n - 1]
        for i = n to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] + lastSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    // ===== END of NODE MANIPULATION functions =====

    // TODO: Write a variant of UpdateChildDifferentSize that can handle "sliding" items left or right, because RRBRelaxedNode will want to also "slide" the size table entries appropriately
    // And it will be useful in implementing the rebalance feature as well, where we'll be combining multiple nodes, often by "sliding" items around.
    // Perhaps we'll implement it by keeping track of the individual sizes of each item (by subtraction from the previous size table entry), and then adding each individual size as we go

    // TODO: Determine whether the above comment is complete now, then remove it.

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
        this.UpdateChild owner shift localIdx newNode :> RRBNode<'T>

    override this.InsertedTree owner shift treeIdx item parentOpt idxOfNodeInParent =
        let localIdx, child, nextLvlIdx = this.IndexesAndChild shift treeIdx
        let insertResult = child.InsertedTree owner (down shift) nextLvlIdx item (Some this) localIdx
        match insertResult with
        | SimpleInsertion newChild ->
            SimpleInsertion (this.UpdateChildSRel owner shift localIdx newChild 1 :> RRBNode<'T>)
        | SlidItemsLeft (newLeft, newChild) ->
            // Always update the *right* child first, then the left: that way the size table adjustments will be correct
            let newNode = this.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift))
            SimpleInsertion (newNode.UpdateChildSAbs owner shift (localIdx - 1) newLeft (newLeft.TreeSize (down shift)) :> RRBNode<'T>)
            // TODO: Do I need an "Update two child items at once" function? What about the size table? We should be able to manage the size table more cleverly in RelaxedNodes.
        | SlidItemsRight (newChild, newRight) ->
            let newNode = this.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift))
            SimpleInsertion (newNode.UpdateChildSAbs owner shift (localIdx + 1) newRight (newRight.TreeSize (down shift)) :> RRBNode<'T>)
            // TODO: Comments from SlidItemsLeft re size table apply here too.
        | SplitNode (newChild, newRight) ->
            if this.NodeSize < Literals.blockSize then
                let newNode = this.InsertedChild owner shift (localIdx + 1) newRight (newRight.TreeSize (down shift))
                SimpleInsertion (newNode.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift)) :> RRBNode<'T>)
            else
                let localIdx, _, _ = this.IndexesAndChild shift treeIdx
                match (parentOpt, idxOfNodeInParent) with
                | Some parent, idx when idx > 0 && parent.Children.[idx - 1].NodeSize < Literals.blockSize ->
                    // Room in the left sibling
                    let leftSib = parent.Children.[idx - 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSlideChildrenLeft owner shift (localIdx + 1) newRight leftSib
                    SlidItemsLeft (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)
                | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                    // Room in the right sibling
                    let rightSib = parent.Children.[idx + 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSlideChildrenRight owner shift (localIdx + 1) newRight rightSib
                    SlidItemsRight (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)
                | _ ->
                    // No room left or right, so split
                    let newNode = this.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift))
                    let newLeft, newRight = newNode.InsertAndSplitNode owner shift localIdx newRight
                    SplitNode (newLeft :> RRBNode<'T>, newRight :> RRBNode<'T>)

    // abstract member AppendChild : OwnerToken -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    // default this.AppendChild owner shift newChild childSize =
    //     let newChildren = this.Children |> Array.copyAndAppend newChild
    //     // Full nodes are allowed to have their last item be non-full, so we have to check that
    //     if this.FullNodeIsTrulyFull shift then
    //         RRBNode<'T>.MkFullNode owner newChildren
    //     else
    //         // Last item wasn't full, so this is going to become a relaxed node
    //         RRBNode<'T>.MkNode owner shift newChildren

    abstract member InsertedChild : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBFullNode<'T>
    default this.InsertedChild owner shift localIdx newChild childSize =
        if localIdx = this.NodeSize then
            this.AppendChildS owner shift newChild childSize
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
                else this.UpdateChildSAbs owner shift (this.NodeSize - 1) newLastChild (newLastChild.TreeSize (down shift))
            leaf, newNode

    // --- REBALANCING ---

    member this.Rebalance (owner : OwnerToken) (shift : int) =
        let items = this.Children |> Seq.ofArray
        let sizes = items |> Seq.map (fun node -> node.NodeSize)
        let len = this.NodeSize
        let idx, mergeLen = findMergeCandidates sizes len
        let sizeReduction = 1  // When we start using findMergeCandidatesTwoPasses, this will be part of the above line as a 3-tuple, not a 2-tuple
        let newLen = len - sizeReduction
        let newNode = this.GetEditableEmptyNodeOfLengthN owner newLen :?> RRBFullNode<'T>
        // Prefix
        if not (LanguagePrimitives.PhysicalEquality newNode this) then
            Array.blit this.Children 0 newNode.Children 0 idx
        // Consolidated section
        this.ConsolidateChildren owner shift idx mergeLen newNode.Children
        // Remnant
        Array.blit this.Children (idx + mergeLen) newNode.Children (idx + mergeLen - sizeReduction) (len - idx)
        this.SetNodeSize newLen  // This allows expanded nodes to zero out the appropriate parts, so that garbage collection can happen on anything that was shifted
        newNode

    member this.Rebalance2 (owner : OwnerToken) (shift : int) (right : RRBFullNode<'T>) =
        let thisLen = this.NodeSize
        let rightLen = right.NodeSize
        let len = thisLen + rightLen
        let thisItems = this.Children |> Seq.ofArray |> Seq.take thisLen
        let rightItems = right.Children |> Seq.ofArray |> Seq.take rightLen
        let items = Seq.append thisItems rightItems
        let sizes = items |> Seq.map (fun node -> node.NodeSize)
        let idx, mergeLen = findMergeCandidates sizes len
        let sizeReduction = 1  // When we start using findMergeCandidatesTwoPasses, this will be part of the above line as a 3-tuple, not a 2-tuple
        let newLen = len - sizeReduction
        if newLen <= Literals.blockSize then
            // TODO: This segment could be pulled out into a function called "applyRebalancePlan"
            let newNode = this.GetEditableEmptyNodeOfLengthN owner newLen :?> RRBFullNode<'T>
            // Prefix
            if not (LanguagePrimitives.PhysicalEquality newNode this) then
                Array.blit this.Children 0 newNode.Children 0 idx
            // Consolidated section
            this.ConsolidateChildren owner shift idx mergeLen newNode.Children
            // Remnant
            Array.blit this.Children (idx + mergeLen) newNode.Children (idx + mergeLen - sizeReduction) (len - idx)
            this.SetNodeSize newLen  // This allows expanded nodes to zero out the appropriate parts, so that garbage collection can happen on anything that was shifted
            newNode, None
        else
            let newItems = Array.zeroCreate newLen
            Array.blit this.Children 0 newItems 0 idx
            // TODO: Write the rest of this -- must figure out where the split is
            this, None  // TODO: Change this line to be "left, right" once I write the rest of the if branch correctly

    member this.ConsolidateChildren (owner : OwnerToken) (shift : int) (idx : int) (mergeLen : int) (destChildren : RRBNode<'T>[]) =
        if shift <= Literals.blockSizeShift then
            // Children are leaves
            let children = this.Children |> Seq.cast<RRBLeafNode<'T>>
            let arraysToMerge = children |> Seq.skip idx |> Seq.truncate mergeLen |> Seq.map (fun leaf -> leaf.Items)
            let combined = Array.concat arraysToMerge
            let split = combined |> Array.chunkBySize Literals.blockSize
            let leaves = split |> Array.map (fun items -> RRBNode<'T>.MkLeaf owner items :> RRBNode<'T>)
#if DEBUG
            if Array.length leaves <> mergeLen then
                failwith <| sprintf "Expected a length of %d in the rebalanced section of leaves, but found a length of %d instead. Whole array was %A" mergeLen (Array.length leaves) this.Children
#endif
            leaves.CopyTo(destChildren, idx)
        else
            // Children are nodes
            let children = this.Children |> Seq.cast<RRBFullNode<'T>>
            let arraysToMerge = children |> Seq.skip idx |> Seq.truncate mergeLen |> Seq.map (fun leaf -> leaf.Children)
            let combined = Array.concat arraysToMerge
            let split = combined |> Array.chunkBySize Literals.blockSize
            let downshift = down shift
            let newChildren = split |> Array.map (fun items -> RRBNode<'T>.MkNode owner downshift items :> RRBNode<'T>)
#if DEBUG
            if Array.length newChildren <> mergeLen then
                failwith <| sprintf "Expected a length of %d in the rebalanced section of new children, but found a length of %d instead. Whole array was %A" mergeLen (Array.length newChildren) this.Children
#endif
            newChildren.CopyTo(destChildren, idx)


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

    override this.Shrink _ = this :> RRBNode<'T>
    override this.Expand owner =
        let node' = this.GetEditableNode owner :?> RRBRelaxedNode<'T>
        RRBExpandedRelaxedNode<'T>(owner, node'.Children, node'.SizeTable) :> RRBNode<'T>

    abstract member ToFullNodeIfNeeded : int -> RRBFullNode<'T>
    default this.ToFullNodeIfNeeded shift =
        if RRBMath.isSizeTableFullAtShift shift sizeTable sizeTable.Length
        then RRBFullNode<'T>(this.Owner, this.Children)
        else this :> RRBFullNode<'T>

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    override this.AppendChild owner shift newChild =
        this.AppendChildS owner shift newChild (newChild.TreeSize (down shift))

    override this.AppendChildS owner shift newChild newChildSize =
        let children' = this.Children |> Array.copyAndAppend newChild
        let lastSizeTableEntry = if this.SizeTable.Length = 0 then 0 else Array.last this.SizeTable
        let sizeTable' = this.SizeTable |> Array.copyAndAppend (lastSizeTableEntry + newChildSize)
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.InsertChild owner shift localIdx newChild =
        this.InsertChildS owner shift localIdx newChild (newChild.TreeSize (down shift))

    override this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let children' = this.Children |> Array.copyAndInsertAt localIdx newChild
        let sizeTable' = this.SizeTable |> Array.copyAndInsertAt localIdx newChildSize
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.RemoveChild owner shift localIdx =
        let children' = this.Children |> Array.copyAndRemoveAt localIdx
        let sizeTable' = this.SizeTable |> Array.copyAndRemoveAt localIdx
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.RemoveLastChild owner shift =
        let children' = this.Children |> Array.copyAndPop
        let sizeTable' = this.SizeTable |> Array.copyAndPop
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.UpdateChildSRel owner shift localIdx newChild sizeDiff =
        let node' = this.GetEditableNode owner :?> RRBRelaxedNode<'T>
        node'.Children.[localIdx] <- newChild
        for i = localIdx to this.NodeSize - 1 do
            node'.SizeTable.[i] <- this.SizeTable.[i] + sizeDiff
        node'.ToFullNodeIfNeeded shift

    override this.UpdateChildSAbs owner shift localIdx newChild childSize =
        let leftSiblingSize = if localIdx <= 0 then 0 else this.SizeTable.[localIdx - 1]
        let oldChildSize = this.SizeTable.[localIdx] - leftSiblingSize
        this.UpdateChildSRel owner shift localIdx newChild (childSize - oldChildSize)

    override this.KeepNLeft owner shift n =
        let children' = this.Children |> Array.truncate n
        let sizeTable' = this.SizeTable |> Array.truncate n
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
#if DEBUG
        if n <= 0 then
            failwithf "In KeepNRight, n must be at least one, got %d. This node: %A" n this
        if n >= this.NodeSize then
            failwithf "In KeepNRight, n must be less than node size, got %d (and node size was %d). This node: %A" n this.NodeSize this
        if skip <= 0 then
            failwithf "In KeepNRight, n must be less than node size, got %d (and node size was %d), resulting in %d skipped children. This node: %A" n this.NodeSize skip this
#endif
        let children' = this.Children |> Array.skip skip
        let sizeTable' = this.SizeTable |> Array.skip skip
        let lastSizeTableEntry = this.SizeTable.[skip - 1]
        for i = skip to this.NodeSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] - lastSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.SplitAndKeepNLeft owner shift n =
        let l, r = this.Children |> Array.splitAt n
        let lS = this.SizeTable |> Array.truncate n
        let node' = RRBNode<'T>.MkNodeKnownSize owner shift l lS
        (node', r)

    override this.SplitAndKeepNLeftS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeftS, n must be at least one, got %d. This node: %A" n this
#endif
        let l, r = this.Children |> Array.splitAt n
        let lS, rS = this.SizeTable |> Array.splitAt n
        let lastSize = lS |> Array.last
        for i = 0 to rS.Length - 1 do
            rS.[i] <- rS.[i] - lastSize
        let node' = RRBNode<'T>.MkNodeKnownSize owner shift l lS
        (node', (r, rS))

    override this.SplitAndKeepNRight owner shift n =
        let ((l, _), node') = this.SplitAndKeepNRightS owner shift n
        (l, node')

    override this.SplitAndKeepNRightS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRightS, n must be at least one, got %d. This node: %A" n this
#endif
        let l, r = this.Children |> Array.splitAt n
        let lS, rS = this.SizeTable |> Array.splitAt n
        let lastSize = lS |> Array.last
        for i = 0 to rS.Length - 1 do
            rS.[i] <- rS.[i] - lastSize
        let node' = RRBNode<'T>.MkNodeKnownSize owner shift r rS
        ((l, lS), node')

    override this.AppendNChildren owner shift n newChildren =
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', 0)
        let sizeTable' = Array.zeroCreate newSize
        this.SizeTable.CopyTo(sizeTable', 0)
        let mutable prevSizeTableEntry = sizeTable'.[size - 1]
        use eC = newChildren.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() then
                children'.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                sizeTable'.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'
        // TODO: That's *almost* identical to the version in RRBFullNode<'T> - separate out the common code and combine it

    override this.AppendNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', 0)
        let sizeTable' = Array.zeroCreate newSize
        this.SizeTable.CopyTo(sizeTable', 0)
        let lastSizeTableEntry = sizeTable'.[size - 1]
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                children'.[i] <- eC.Current
                sizeTable'.[i] <- lastSizeTableEntry + eS.Current
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'
        // TODO: That's *almost* identical to the version in RRBFullNode<'T> - separate out the common code and combine it

    override this.PrependNChildren owner shift n newChildren =
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', n)
        let sizeTable' = Array.zeroCreate newSize
        this.SizeTable.CopyTo(sizeTable', 0)
        let mutable prevSizeTableEntry = 0
        use eC = newChildren.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() then
                children'.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                sizeTable'.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'
        // TODO: That's *almost* identical to the version in RRBFullNode<'T> - separate out the common code and combine it

    override this.PrependNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', n)
        let sizeTable' = Array.zeroCreate newSize
        this.SizeTable.CopyTo(sizeTable', 0)
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                children'.[i] <- eC.Current
                sizeTable'.[i] <- eS.Current
        let lastSizeTableEntry = sizeTable'.[n - 1]
        for i = n to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] + lastSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'
        // TODO: That's *almost* identical to the version in RRBFullNode<'T> - separate out the common code and combine it

    // ===== END of NODE MANIPULATION functions =====

    override this.SplitNodeIntoArrays owner shift splitIdx =
        // TODO: Expanded nodes *must* rewrite this so that the right children will be handled correctly (not filled with nulls, since we won't always *want* the right child to end up expanded)
        // TODO: Find and replace all occurrences of SplitNodeIntoArrays with the SplitAndKeep versions above... unless there's a darn good reason for using the arrays version here
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

    override this.SetNodeSize _ = ()

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

    override this.Expand owner = this.GetEditableNode owner

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedFullNode<'T>(owner, Array.copy this.Children, this.NodeSize) :> RRBNode<'T>

    override this.SetNodeSize newSize =
        // This should only be called when the node is already editable
        let curSize = this.NodeSize
        if curSize = newSize then
            ()
        elif curSize > newSize then
            // Node expanded, so no need to zero anything out
            this.CurrentLength <- newSize
        else
            // Node shrank, so zero out the children between newSize and oldSize
            for i = newSize to curSize - 1 do
                this.Children.[i] <- null

    override this.ToRelaxedNodeIfNeeded shift =
        if shift <= 0 then this :> RRBNode<'T> else
            let size = this.NodeSize
            let sizeTable = RRBNode<'T>.CreateSizeTableS shift this.Children size
            if RRBMath.isSizeTableFullAtShift shift sizeTable size
            then this :> RRBNode<'T>
            else RRBExpandedRelaxedNode<'T>(ownerToken, this.Children, sizeTable, this.NodeSize) :> RRBNode<'T>

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    override this.AppendChild owner shift newChild =
        let trulyFull = this.FullNodeIsTrulyFull shift
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let oldSize = node'.NodeSize
        node'.Children.[oldSize] <- newChild
        node'.SetNodeSize (oldSize + 1)
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if trulyFull then
            node' :> RRBFullNode<'T>
        else
            // Last item wasn't full, so result becomes a relaxed node
            let sizeTable = node'.BuildSizeTable shift oldSize (oldSize - 1)
#if DEBUG
            if sizeTable |> Array.isEmpty then
                failwithf "Got empty size table from calling this.BuildSizeTable %d %d %d in RRBExpandedFullNode<'T>.AppendChild. This node = %A" shift this.NodeSize (this.NodeSize - 1) this
#endif
            let lastEntry = sizeTable |> Array.last
            let sizeTable' = Array.expandToBlockSize sizeTable
            sizeTable'.[oldSize] <- lastEntry + newChild.TreeSize (down shift)
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', this.NodeSize) :> RRBFullNode<'T>
        // TODO: Might be able to optimize this by first checking if we're truly full, and then if we're not, convert to a relaxed node FIRST before calling RelaxedNode.AppendChildS

    override this.AppendChildS owner shift newChild _newChildSize =
        this.AppendChild owner shift newChild

    override this.InsertChild owner shift localIdx newChild =
        this.InsertChildS owner shift localIdx newChild (newChild.TreeSize (down shift))

    override this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let newSize = node'.NodeSize - 1
        for i = newSize downto localIdx do
            node'.Children.[i] <- node'.Children.[i-1]
        node'.SetNodeSize newSize
        if newChildSize = (1 <<< shift) then
            // Inserted a full child, so this is still a full node
            node' :> RRBFullNode<'T>
        else
            node'.ToRelaxedNodeIfNeeded shift :?> RRBFullNode<'T>

    override this.RemoveChild owner shift localIdx =
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let newSize = node'.NodeSize - 1
        for i = localIdx to newSize - 1 do
            node'.Children.[i] <- node'.Children.[i+1]
        node'.Children.[node'.NodeSize] <- null
        node'.SetNodeSize newSize
        // Removing a child from a full node can never make it non-full
        node' :> RRBFullNode<'T>

    override this.RemoveLastChild owner shift =
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let newSize = node'.NodeSize - 1
        node'.Children.[newSize] <- null
        node'.SizeTable.[newSize] <- 0
        node'.SetNodeSize newSize
        node'.ToFullNodeIfNeeded shift

    override this.UpdateChildSRel owner shift localIdx newChild sizeDiff =
        if localIdx = (this.NodeSize - 1) || sizeDiff = 0 then
            // No need to turn into a relaxed node
            this.UpdateChild owner shift localIdx newChild
        else
            let size = this.NodeSize
            let sizeTable = this.BuildSizeTable shift size (size-1)
            let node' = RRBExpandedRelaxedNode<'T>(this.Owner, this.Children, sizeTable, this.NodeSize)
            node'.UpdateChildSRel owner shift localIdx newChild sizeDiff

    override this.UpdateChildSAbs owner shift localIdx newChild childSize =
        if localIdx = (this.NodeSize - 1) || childSize = (1 <<< shift) then
            // No need to turn into a relaxed node
            this.UpdateChild owner shift localIdx newChild
        else
            let size = this.NodeSize
            let sizeTable = this.BuildSizeTable shift size (size-1)
            let node' = RRBExpandedRelaxedNode<'T>(this.Owner, this.Children, sizeTable, this.NodeSize)
            node'.UpdateChildSAbs owner shift localIdx newChild childSize

    override this.KeepNLeft owner shift n =
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
        node'.SetNodeSize n
        node' :> RRBFullNode<'T>
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    override this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        for i = 0 to n - 1 do
            node'.Children.[i] <- node'.Children.[i + skip]
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
        node'.SetNodeSize n
        node' :> RRBFullNode<'T>
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    override this.SplitAndKeepNLeft owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeft, n must be at least one, got %d. This node: %A" n this
#endif
        let r = Array.sub this.Children n (this.NodeSize - n)
        let node' = this.KeepNLeft owner shift n
        (node', r)

    override this.SplitAndKeepNLeftS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeftS, n must be at least one, got %d. This node: %A" n this
#endif
        let r = Array.sub this.Children n (this.NodeSize - n)
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let lS, rS = sizeTable |> Array.splitAt n
        let lastSize = lS |> Array.last
        for i = 0 to rS.Length - 1 do
            rS.[i] <- rS.[i] - lastSize
        let node' = this.KeepNLeft owner shift n
        (node', (r, rS))

    override this.SplitAndKeepNRight owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRight, n must be at least one, got %d. This node: %A" n this
#endif
        let l = Array.sub this.Children 0 n
        let node' = this.KeepNRight owner shift n
        (l, node')

    override this.SplitAndKeepNRightS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRightS, n must be at least one, got %d. This node: %A" n this
#endif
        let l = Array.sub this.Children 0 n
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let lS = Array.sub sizeTable 0 n
        let node' = this.KeepNRight owner shift n
        ((l, lS), node')

    override this.AppendNChildren owner shift n newChildren =
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        let fullChildSize = (1 <<< shift)
        let mutable stillFull = (node'.LastChild.TreeSize (down shift) >= fullChildSize)
        use eC = newChildren.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() then
                node'.Children.[i] <- eC.Current
                if stillFull && i < newSize - 1 then
                    // Last child can be non-full, but we need to check all the others
                    let childSize = eC.Current.TreeSize (down shift)
                    stillFull <- childSize >= fullChildSize
        node'.SetNodeSize newSize
        if stillFull then
            node' :> RRBFullNode<'T>
        else
            node'.ToRelaxedNodeIfNeeded shift :?> RRBFullNode<'T>

    override this.AppendNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        let sizeTable = this.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate Literals.blockSize
        sizeTable.CopyTo(sizeTable', 0)
        let lastSizeTableEntry = sizeTable'.[size - 1]
        let mutable stillFull = node'.FullNodeIsTrulyFull shift
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                node'.Children.[i] <- eC.Current
                sizeTable'.[i] <- lastSizeTableEntry + eS.Current
#if DEBUG
                if i <= 0 then
                    failwithf "AppendNChildrenS called on an empty node; this should never happen. Parameters: owner=%A shift=%d n=%d newChildren=%A sizes=%A and this node=%A" owner shift n newChildren sizes this
#endif
                let childSize = sizeTable'.[i] - sizeTable'.[i-1]
                if stillFull && i < newSize - 1 then
                    // Last child can be non-full, but we need to check all the others
                    stillFull <- childSize >= (1 <<< shift)
        node'.SetNodeSize newSize
        if stillFull then
            node' :> RRBFullNode<'T>
        else
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', newSize) :> RRBFullNode<'T>

    override this.PrependNChildren owner shift n newChildren =
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        let sizeTable = node'.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate Literals.blockSize
        sizeTable.CopyTo(sizeTable', n)
        for i = (size - 1) downto 0 do
            node'.Children.[i+n] <- node'.Children.[i]
        let mutable prevSizeTableEntry = 0
        let mutable stillFull = true
        let fullChildSize = (1 <<< shift)
        use eC = newChildren.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() then
                node'.Children.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                if childSize < fullChildSize then stillFull <- false
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                sizeTable'.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        for i = n to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] + prevSizeTableEntry
        node'.SetNodeSize newSize
        if stillFull then
            node' :> RRBFullNode<'T>
        else
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', newSize) :> RRBFullNode<'T>

    override this.PrependNChildrenS owner shift n newChildren sizes =
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        let sizeTable = node'.BuildSizeTable shift size (size - 1)
        let sizeTable' = Array.zeroCreate Literals.blockSize
        sizeTable.CopyTo(sizeTable', n)
        for i = (size - 1) downto 0 do
            node'.Children.[i+n] <- node'.Children.[i]
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                node'.Children.[i] <- eC.Current
                sizeTable'.[i] <- eS.Current
        let lastSizeTableEntry = sizeTable'.[n - 1]
        let stillFull = lastSizeTableEntry >= (n <<< shift)
        for i = n to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] + lastSizeTableEntry
        node'.SetNodeSize newSize
        if stillFull then
            node' :> RRBFullNode<'T>
        else
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', newSize) :> RRBFullNode<'T>

    // ===== END of NODE MANIPULATION functions =====

    // override this.InsertAndSlideChildrenLeft owner shift localIdx newChild leftSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSlideChildrenRight owner shift localIdx newChild rightSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSplitNode owner shift localIdx newChild =
    //     failwith "Not implemented"



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

    override this.Expand owner = this.GetEditableNode owner

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedRelaxedNode<'T>(owner, Array.copy this.Children, Array.copy this.SizeTable, this.NodeSize) :> RRBNode<'T>

    override this.SetNodeSize newSize =
        // This should only be called when the node is already editable
        let curSize = this.NodeSize
        if curSize = newSize then
            ()
        elif curSize > newSize then
            // Node expanded, so no need to zero anything out
            this.CurrentLength <- newSize
        else
            // Node shrank, so zero out the children between newSize and oldSize
            for i = newSize to curSize - 1 do
                this.Children.[i] <- null

    override this.ToFullNodeIfNeeded shift =
        if RRBMath.isSizeTableFullAtShift shift sizeTable this.NodeSize
        then RRBExpandedFullNode<'T>(this.Owner, this.Children, this.NodeSize) :> RRBFullNode<'T>
        else this :> RRBFullNode<'T>

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    override this.AppendChild owner shift newChild =
        let newChildSize = newChild.TreeSize (down shift)
        this.AppendChildS owner shift newChild newChildSize

    override this.AppendChildS owner shift newChild newChildSize =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let oldSize = node'.NodeSize
#if DEBUG
        if oldSize <= 0 then
            failwithf "AppendChildS called on an empty node; this should never happen. Parameters: owner=%A shift=%d newChild=%A and this node=%A" owner shift newChild this
#endif
        node'.Children.[oldSize] <- newChild
        node'.SizeTable.[oldSize] <- node'.SizeTable.[oldSize - 1] + newChildSize
        node'.SetNodeSize (oldSize + 1)
        node' :> RRBFullNode<'T>

    override this.InsertChild owner shift localIdx newChild =
        this.InsertChildS owner shift localIdx newChild (newChild.TreeSize (down shift))

    override this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let newSize = node'.NodeSize - 1
        for i = newSize downto localIdx do
            node'.Children.[i] <- node'.Children.[i-1]
            node'.SizeTable.[i] <- node'.SizeTable.[i-1] + newChildSize
        node'.SetNodeSize newSize
        node'.ToFullNodeIfNeeded shift

    override this.RemoveChild owner shift localIdx =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        let newSize = size - 1
        let oldChildSize = node'.SizeTable.[localIdx] - (if localIdx <= 0 then 0 else node'.SizeTable.[localIdx-1])
        for i = localIdx to newSize - 1 do
            node'.Children.[i] <- node'.Children.[i+1]
            node'.SizeTable.[i] <- node'.SizeTable.[i+1] - oldChildSize
        node'.Children.[newSize] <- null
        node'.SizeTable.[newSize] <- 0
        node'.SetNodeSize newSize
        node'.ToFullNodeIfNeeded shift

    override this.RemoveLastChild owner shift =
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let newSize = node'.NodeSize - 1
        node'.Children.[newSize] <- null
        node'.SizeTable.[newSize] <- 0
        node'.SetNodeSize newSize
        node'.ToFullNodeIfNeeded shift

    // No need to override UpdateChildSRel and UpdateChildSAbs; versions from compact RRBRelaxedNode will work just fine in expanded nodes

    override this.KeepNLeft owner shift n =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
            node'.SizeTable.[i] <- 0
        node'.SetNodeSize n
        node'.ToFullNodeIfNeeded shift

    override this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
#if DEBUG
        if n <= 0 then
            failwithf "In KeepNRight, n must be at least one, got %d. This node: %A" n this
        if n >= this.NodeSize then
            failwithf "In KeepNRight, n must be less than node size, got %d (and node size was %d). This node: %A" n this.NodeSize this
        if skip <= 0 then
            failwithf "In KeepNRight, n must be less than node size, got %d (and node size was %d), resulting in %d skipped children. This node: %A" n this.NodeSize skip this
#endif
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let lastSizeTableEntry = node'.SizeTable.[skip - 1]
        for i = 0 to n - 1 do
            node'.Children.[i] <- node'.Children.[i + skip]
            node'.SizeTable.[i] <- node'.SizeTable.[i + skip] - lastSizeTableEntry
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
            node'.SizeTable.[i] <- 0
        node'.SetNodeSize n
        node'.ToFullNodeIfNeeded shift

    override this.SplitAndKeepNLeft owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeft, n must be at least one, got %d. This node: %A" n this
#endif
        let r = Array.sub this.Children n (this.NodeSize - n)
        let node' = this.KeepNLeft owner shift n
        (node', r)

    override this.SplitAndKeepNLeftS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeftS, n must be at least one, got %d. This node: %A" n this
#endif
        let size = this.NodeSize
        let lastSizeL = this.SizeTable.[n-1]
        let rS = Array.sub this.SizeTable n (size - n)
        for i = 0 to rS.Length - 1 do
            rS.[i] <- rS.[i] - lastSizeL
        let r = Array.sub this.Children n (size - n)
        let node' = this.KeepNLeft owner shift n
        (node', (r, rS))

    override this.SplitAndKeepNRight owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRight, n must be at least one, got %d. This node: %A" n this
#endif
        let skip = this.NodeSize - n
        let l = Array.sub this.Children 0 skip
        let node' = this.KeepNRight owner shift n
        (l, node')

    override this.SplitAndKeepNRightS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRightS, n must be at least one, got %d. This node: %A" n this
#endif
        let skip = this.NodeSize - n
        let l = Array.sub this.Children 0 skip
        let lS = Array.sub this.SizeTable 0 skip
        let node' = this.KeepNRight owner shift n
        ((l, lS), node')

    override this.AppendNChildren owner shift n newChildren =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        let mutable prevSizeTableEntry = node'.SizeTable.[size - 1]
        use eC = newChildren.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() then
                node'.Children.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                node'.SizeTable.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        node'.SetNodeSize newSize
        node' :> RRBFullNode<'T>

    override this.AppendNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        let lastSizeTableEntry = node'.SizeTable.[size - 1]
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = size to newSize - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                node'.Children.[i] <- eC.Current
                node'.SizeTable.[i] <- lastSizeTableEntry + eS.Current
#if DEBUG
                if i <= 0 then
                    failwithf "AppendNChildrenS called on an empty node; this should never happen. Parameters: owner=%A shift=%d n=%d newChildren=%A sizes=%A and this node=%A" owner shift n newChildren sizes this
#endif
        node'.SetNodeSize newSize
        node' :> RRBFullNode<'T>

    override this.PrependNChildren owner shift n newChildren =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        for i = (size - 1) downto 0 do
            node'.Children.[i+n] <- node'.Children.[i]
            node'.SizeTable.[i+n] <- node'.SizeTable.[i]
        let mutable prevSizeTableEntry = 0
        use eC = newChildren.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() then
                node'.Children.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                node'.SizeTable.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        for i = n to newSize - 1 do
            node'.SizeTable.[i] <- node'.SizeTable.[i] + prevSizeTableEntry
        node'.SetNodeSize newSize
        node' :> RRBFullNode<'T>

    override this.PrependNChildrenS owner shift n newChildren sizes =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        let newSize = size + n
        for i = (size - 1) downto 0 do
            node'.Children.[i+n] <- node'.Children.[i]
            node'.SizeTable.[i+n] <- node'.SizeTable.[i]
        use eC = newChildren.GetEnumerator()
        use eS = sizes.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() && eS.MoveNext() then
                node'.Children.[i] <- eC.Current
                node'.SizeTable.[i] <- eS.Current
        let lastSizeTableEntry = node'.SizeTable.[n - 1]
        for i = n to newSize - 1 do
            node'.SizeTable.[i] <- node'.SizeTable.[i] + lastSizeTableEntry
        node'.SetNodeSize newSize
        node' :> RRBFullNode<'T>

    // ===== END of NODE MANIPULATION functions =====

// REDESIGN: Leaf nodes will never be expanded; only tree nodes will ever be expanded.


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

    override this.SetNodeSize newSize =
        // This should only be called when the node is already editable
        let curSize = this.NodeSize
        if curSize = newSize then
            ()
        elif curSize > newSize then
            // Node expanded, so no need to zero anything out
            this.CurrentLength <- newSize
        else
            // Node shrank, so zero out the children between newSize and oldSize
            for i = newSize to curSize - 1 do
                this.Items.[i] <- Unchecked.defaultof<'T>

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
