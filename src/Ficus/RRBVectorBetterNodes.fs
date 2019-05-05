module Ficus.RRBVectorBetterNodes

open RRBArrayExtensions

module Literals =
    let [<Literal>] internal blockSizeShift = 5  // TODO: Rename to "shiftSize"
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

let inline isSameObj a b = LanguagePrimitives.PhysicalEquality a b

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
    let findMergeCandidatesTwoPasses (sizeSeq : #seq<int>) len =
        use e = sizeSeq.GetEnumerator()
        let sizes = Array.init len (fun _ -> byte (if e.MoveNext() then Literals.blockSize - e.Current else 0))
        let idx1, len1 = sizes |> Array.smallestRunGreaterThan (byte Literals.blockSize)
        let idx2, len2 = sizes |> Array.smallestRunGreaterThan (byte (Literals.blockSize <<< 1))
        // Drop two slots if we can do so in less than twice the work needed to drop a single slot
        // if len2 < (len1 * 2) then  // TODO: Uncomment this one once we're done testing the rebalance algorithm
        if len2 < (len1 * 2) && len2 < 1000 && idx2 >= 0 then  // DEBUG: While testing the rebalance algorithm, we need this because smallestRunGreaterThan will return (-1, 999999) for "bad" runs
            idx2, len2, 2
        else
            idx1, len1, 1


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
        isSameObj owner ownerToken && not (isNull !ownerToken)
        // Note that this test is NOT "if owner = owner".

    static member PopulateSizeTableS (shift : int) (array:RRBNode<'T>[]) (len : int) (sizeTable : int[]) =
        let mutable total = 0
        for i = 0 to len - 1 do
            total <- total + (array.[i]).TreeSize (down shift)
            sizeTable.[i] <- total

    static member CreateSizeTableS (shift : int) (array:RRBNode<'T>[]) (len : int) : int[] =
        let sizeTable = Array.zeroCreate len
        RRBNode<'T>.PopulateSizeTableS shift array len sizeTable
        sizeTable

    static member CreateSizeTable (shift : int) (array:RRBNode<'T>[]) : int[] =
        RRBNode<'T>.CreateSizeTableS shift array array.Length

    static member MkLeaf (owner : OwnerToken) (items : 'T[]) = RRBLeafNode<'T>(owner, items) :> RRBNode<'T>
    static member MkNode (owner : OwnerToken) (shift : int) (children : RRBNode<'T>[]) =
        RRBRelaxedNode<'T>.Create(owner, shift, children) :> RRBNode<'T>
    static member MkNodeKnownSize (owner : OwnerToken) (shift : int) (children : RRBNode<'T>[]) (sizeTable : int[]) =
        RRBRelaxedNode<'T>.CreateWithSizeTable(owner, shift, children, sizeTable) :> RRBNode<'T>
    static member MkFullNode (owner : OwnerToken) (children : RRBNode<'T>[]) =
        // if children.Length = 1 then SingletonNode<'T>(owner, entries) :> Node<'T> else  // TODO: Do we want an RRBSingletonNode class as well? ANSWER: No we don't; remove this TODO
        RRBFullNode<'T>.Create(owner, children) :> RRBNode<'T>

    abstract member UpdatedTree : OwnerToken -> int -> int -> 'T -> RRBNode<'T>  // Params: owner shift treeIdx newItem
    abstract member InsertedTree : OwnerToken -> int -> int -> 'T -> RRBFullNode<'T> option -> int -> SlideResult<RRBNode<'T>>  // Params: owner shift treeIdx (item : 'T) (parentOpt : Node option) idxOfNodeInParent
    abstract member RemovedItem : OwnerToken -> int -> bool -> int -> 'T * RRBNode<'T>  // TODO: Rename to RemovedTree for consistency??
    abstract member GetTreeItem : int -> int -> 'T

    abstract member KeepNTreeItems : OwnerToken -> int -> int -> RRBNode<'T>
    abstract member SkipNTreeItems : OwnerToken -> int -> int -> RRBNode<'T>
    abstract member SplitTree : OwnerToken -> int -> int -> RRBNode<'T> * RRBNode<'T>

    member this.NeedsRebalance shift =
        let slots = if shift > Literals.blockSize then this.SlotCount else this.TwigSlotCount
        slots <= ((this.NodeSize - Literals.eMaxPlusOne) <<< Literals.blockSizeShift)

    member this.NeedsRebalance2 shift (right : RRBNode<'T>) =
        let slots = if shift > Literals.blockSize then this.SlotCount + right.SlotCount else this.TwigSlotCount + right.TwigSlotCount
        slots <= ((this.NodeSize + right.NodeSize - Literals.eMaxPlusOne) <<< Literals.blockSizeShift)

    member this.NeedsRebalance2PlusLeaf shift (leafLen : int) (right : RRBNode<'T>) =
#if DEBUG
        if shift <> Literals.blockSizeShift then
            failwith <| sprintf "NeedsRebalance2PlusLeaf may only be called at the twig level (shift %d), and it was instead called with shift %d" Literals.blockSizeShift shift
#endif
        let slots = this.TwigSlotCount + leafLen + right.TwigSlotCount
        slots <= ((this.NodeSize + 1 + right.NodeSize - Literals.eMaxPlusOne) <<< Literals.blockSizeShift)
        // TODO: Once unit testing is solid, remove the "shift" parameter since it should always be Literals.blockSizeShift


and [<StructuredFormatDisplay("FullNode({StringRepr})")>] RRBFullNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[]) =
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
            failwithf "TreeSize called on an empty node; shouldn't happen. Node is %A" this
            0
        else
            ((this.NodeSize - 1) <<< shift) + this.LastChild.TreeSize (down shift)
    override this.SlotCount = this.Children |> Array.sumBy (fun child -> child.NodeSize)
    override this.TwigSlotCount =
        // Just as with TreeSize, we can skip calculating all but the rightmost node
        if this.NodeSize = 0 then 0 else ((this.NodeSize - 1) <<< Literals.blockSizeShift) + this.LastChild.NodeSize

    override this.SetNodeSize _ = ()  // No-op; only used in expanded nodes

    member this.StringRepr : string = sprintf "length=%d, children=%A" this.NodeSize this.Children

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

    override this.Shrink owner = this.GetEditableNode owner
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

    abstract member IndexesAndChild : int -> int -> int * RRBNode<'T> * int
    default this.IndexesAndChild shift treeIdx =
        let localIdx = radixIndex shift treeIdx
        let child = this.Children.[localIdx]
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

    abstract member AppendChild : OwnerToken -> int -> RRBNode<'T> -> RRBNode<'T>
    default this.AppendChild owner shift newChild =
        let newChildren = this.Children |> Array.copyAndAppend newChild
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if this.FullNodeIsTrulyFull shift then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // Last item wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member AppendChildS : OwnerToken -> int -> RRBNode<'T> -> int -> RRBNode<'T>
    default this.AppendChildS owner shift newChild _newChildSize =
        this.AppendChild owner shift newChild

    abstract member InsertChild : OwnerToken -> int -> int -> RRBNode<'T> -> RRBNode<'T>
    default this.InsertChild owner shift localIdx newChild =
        if localIdx = this.NodeSize then this.AppendChild owner shift newChild else
        let newChildren = this.Children |> Array.copyAndInsertAt localIdx newChild
        let newChildSize = newChild.TreeSize (down shift)
        if newChildSize >= (1 <<< shift) then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // New child wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member InsertChildS : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBNode<'T>
    default this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let newChildren = this.Children |> Array.copyAndInsertAt localIdx newChild
        if newChildSize >= (1 <<< shift) then
            RRBNode<'T>.MkFullNode owner newChildren
        else
            // New child wasn't full, so this is going to become a relaxed node
            RRBNode<'T>.MkNode owner shift newChildren

    abstract member RemoveChild : OwnerToken -> int -> int -> RRBNode<'T>
    default this.RemoveChild owner shift localIdx =
        let newChildren = this.Children |> Array.copyAndRemoveAt localIdx
        // Removing a child from a full node can never make it become not-full
        RRBNode<'T>.MkFullNode owner newChildren

    abstract member RemoveLastChild : OwnerToken -> int -> RRBNode<'T>
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
        node :> RRBNode<'T>

    abstract member UpdateChildSRel : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBNode<'T>
    default this.UpdateChildSRel owner shift localIdx newChild sizeDiff =
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let node' = RRBRelaxedNode<'T>(this.Owner, this.Children, sizeTable)
        node'.UpdateChildSRel owner shift localIdx newChild sizeDiff

    abstract member UpdateChildSAbs : OwnerToken -> int -> int -> RRBNode<'T> -> int -> RRBNode<'T>
    default this.UpdateChildSAbs owner shift localIdx newChild childSize =
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let node' = RRBRelaxedNode<'T>(this.Owner, this.Children, sizeTable)
        node'.UpdateChildSAbs owner shift localIdx newChild childSize

    abstract member KeepNLeft : OwnerToken -> int -> int -> RRBNode<'T>
    default this.KeepNLeft owner shift n =
        if n = this.NodeSize then this :> RRBNode<'T> else
        let arr' = this.Children |> Array.truncate n
        RRBFullNode<'T>(owner, arr') :> RRBNode<'T>
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    abstract member KeepNRight : OwnerToken -> int -> int -> RRBNode<'T>
    default this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
        if skip = 0 then this :> RRBNode<'T> else
        let arr' = this.Children |> Array.skip skip
        RRBFullNode<'T>(owner, arr') :> RRBNode<'T>
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    abstract member SplitAndKeepNLeft : OwnerToken -> int -> int -> (RRBNode<'T> * RRBNode<'T> [])
    default this.SplitAndKeepNLeft owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeft, n must be at least one, got %d. This node: %A" n this
#endif
        let l, r = this.Children |> Array.splitAt n
        let node' = RRBFullNode<'T>(owner, l) :> RRBNode<'T>
        (node', r)

    abstract member SplitAndKeepNLeftS : OwnerToken -> int -> int -> (RRBNode<'T> * (RRBNode<'T> [] * int []))
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
        let node' = RRBFullNode<'T>(owner, l) :> RRBNode<'T>
        (node', (r, rS))

    abstract member SplitAndKeepNRight : OwnerToken -> int -> int -> (RRBNode<'T> [] * RRBNode<'T>)
    default this.SplitAndKeepNRight owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRight, n must be at least one, got %d. This node: %A" n this
#endif
        let skip = this.NodeSize - n
        let l, r = this.Children |> Array.splitAt skip
        let node' = RRBFullNode<'T>(owner, r) :> RRBNode<'T>
        (l, node')

    abstract member SplitAndKeepNRightS : OwnerToken -> int -> int -> ((RRBNode<'T> [] * int []) * RRBNode<'T>)
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
        let node' = RRBFullNode<'T>(owner, r) :> RRBNode<'T>
        ((l, lS), node')

    abstract member AppendNChildren : OwnerToken -> int -> int -> RRBNode<'T> seq -> bool -> RRBNode<'T>
    default this.AppendNChildren owner shift n newChildren _shouldExpandRightChildIfNeeded =
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

    abstract member AppendNChildrenS : OwnerToken -> int -> int -> RRBNode<'T> seq -> int seq -> bool -> RRBNode<'T>
    default this.AppendNChildrenS owner shift n newChildren sizes _shouldExpandRightChildIfNeeded =
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

    abstract member PrependNChildren : OwnerToken -> int -> int -> RRBNode<'T> seq -> RRBNode<'T>
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
        for i = n to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] + prevSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    abstract member PrependNChildrenS : OwnerToken -> int -> int -> RRBNode<'T> seq -> int seq -> RRBNode<'T>
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

    abstract member ExpandLastChildIfNeeded : OwnerToken -> RRBNode<'T>
    default this.ExpandLastChildIfNeeded owner =
        this :> RRBNode<'T>   // TODO: Decide whether this should be a side-effecting-only function and return unit

    // ===== END of NODE MANIPULATION functions =====

    // TODO: Write a variant of UpdateChildDifferentSize that can handle "sliding" items left or right, because RRBRelaxedNode will want to also "slide" the size table entries appropriately
    // And it will be useful in implementing the rebalance feature as well, where we'll be combining multiple nodes, often by "sliding" items around.
    // Perhaps we'll implement it by keeping track of the individual sizes of each item (by subtraction from the previous size table entry), and then adding each individual size as we go

    // TODO: Determine whether the above comment is complete now, then remove it.

    member this.BuildSizeTable shift count lastIdx =
        let fullSize = 1 <<< shift
        Array.init count (fun idx -> if idx = lastIdx then fullSize * idx + this.Children.[idx].TreeSize (down shift) else fullSize * (idx + 1))

    member this.NewPath<'T> owner endShift (leaf : RRBLeafNode<'T>) =
        // NOTE that this does *not* expand any nodes, because it might be called for a left path. It's the caller's responsibility to expand nodes as needed.
        let rec loop shift node =
            if shift >= endShift
            then node
            else let shift' = (up shift) in loop shift' (RRBNode<'T>.MkNode owner shift' [|node|])
        loop Literals.blockSizeShift (RRBNode<'T>.MkNode owner Literals.blockSizeShift [|leaf|])

    member this.TryAppendLeaf owner shift (newLeaf : RRBLeafNode<'T>) leafLen =
        if shift <= Literals.blockSizeShift then
            if this.NodeSize >= Literals.blockSize then None else this.AppendChildS owner shift newLeaf leafLen |> Some
        else
            match (this.LastChild :?> RRBFullNode<'T>).TryAppendLeaf owner (down shift) newLeaf leafLen with
            | Some newChild ->
                this.UpdateChildSRel owner shift (this.NodeSize - 1) newChild leafLen |> Some
            | None -> // Child's subtree had no room to append leaf
                if this.NodeSize >= Literals.blockSize
                then None
                else this.AppendChildS owner shift (this.NewPath owner (down shift) newLeaf) leafLen |> Some
                // Note that if we're an expanded node, AppendChildS will take care of shrinking old last child and expanding new one

    member this.AppendLeaf owner shift (newLeaf : RRBLeafNode<'T>) =
        match this.TryAppendLeaf owner shift newLeaf newLeaf.NodeSize with
        | Some node' -> node', shift
        | None ->
            let newRight = this.NewPath owner shift newLeaf
            this.NewParent owner shift [|this; newRight|], up shift

    member this.TryPrependLeaf owner shift (newLeaf : RRBLeafNode<'T>) leafLen =
        if shift <= Literals.blockSizeShift then
            if this.NodeSize >= Literals.blockSize then None else this.InsertChildS owner shift 0 newLeaf leafLen |> Some
        else
            match (this.FirstChild :?> RRBFullNode<'T>).TryPrependLeaf owner (down shift) newLeaf leafLen with
            | Some newChild ->
                this.UpdateChildSRel owner shift 0 newChild leafLen |> Some
            | None -> // Child's subtree had no room to prepend leaf
                if this.NodeSize >= Literals.blockSize
                then None
                else this.InsertChildS owner shift 0 (this.NewPath owner (down shift) newLeaf) leafLen |> Some
                // Note that if we're an expanded node, PrependChildS will take care of shrinking old last child and expanding new one

    member this.PrependLeaf owner shift (newLeaf : RRBLeafNode<'T>) =
        match this.TryPrependLeaf owner shift newLeaf newLeaf.NodeSize with
        | Some node' -> node', shift
        | None ->
            let newLeft = this.NewPath owner shift newLeaf :?> RRBFullNode<'T>
            this.NewParent owner shift [|newLeft; this|], up shift

    member this.SlideChildrenLeft owner shift n (leftSibling : RRBFullNode<'T>) =
        if n = 0 then leftSibling :> RRBNode<'T>, this :> RRBNode<'T> else
        let keepCnt = this.NodeSize - n
        let (l, lS), this' = this.SplitAndKeepNRightS owner shift keepCnt
        let leftSibling' = leftSibling.AppendNChildrenS owner shift n l lS true
        leftSibling', this'

    member this.SlideChildrenRight owner shift n (rightSibling : RRBFullNode<'T>) =
        if n = 0 then this :> RRBNode<'T>, rightSibling :> RRBNode<'T> else
        let keepCnt = this.NodeSize - n
        let this', (r, rS) = this.SplitAndKeepNLeftS owner shift keepCnt
        let rightSibling' = rightSibling.PrependNChildrenS owner shift n r rS
        this', rightSibling'

    member this.InsertAndSlideChildrenLeft owner shift localIdx newChild (leftSibling : RRBFullNode<'T>) =
        // TODO: Change this to calculate the amount to slide over, then use SlideChildrenLeft and SlideChildrenRight just up above
        // Preconditions: this.NodeSize = Literals.blockSize and leftSibling.NodeSize < Literals.blockSize

        // GOAL: we make left and right balanced, with any extra going on the left.
        // Method: calculate the average size, and make each one fit that average. Extra should go on the side that localIdx isn't on.
        // Special cases: 32 and 32 = we're not being called in that situation.
        // 31 and 32: if localIdx = 0, then just AppendChild on the left node and be done with it.
        // 30 and 32: turns to 31 and 31 and then we can insert on either side.
        // In the 31/32 case, totalSize will be 63 and that divided by 2 will be 31, so resultSizeL will end up as 31.

        let thisSize = this.NodeSize
        let sibSize = leftSibling.NodeSize
#if DEBUG
        if thisSize <> Literals.blockSize || sibSize >= Literals.blockSize then
            failwith <| sprintf "InsertAndSlideChildrenLeft should be called when this node is full (size %d) and left sibling is NOT full (size less than %d). Instead, this node was %A and left sibling was %A"
                                Literals.blockSize Literals.blockSize this leftSibling
#endif
        if localIdx = 0 && thisSize = Literals.blockSize && sibSize = Literals.blockSize - 1 then
            // Special case since the algorithm below would fail on this one scenario
            let leftSibling' = leftSibling.AppendChild owner shift newChild
            leftSibling', this :> RRBNode<'T>
        else
            let totalSize = thisSize + sibSize
            let resultSizeL = totalSize >>> 1
            let resultSizeR = totalSize - resultSizeL
            let n = thisSize - resultSizeR |> max 1
            let l, r = this.SlideChildrenLeft owner shift n leftSibling
            let idx = localIdx - n   // If we insert at 5 but we slid 3 items over, we're inserting at 2. If we insert at 2 but we slid 3 items over, we're inserting at -1, which is
            // 1 *before* the node size of the new left sibling. So left sibling was 24, now is 27, we're inserting at 26. Yep.
            if idx < 0 then
                // Inserting into left sibling
                let idx' = idx + l.NodeSize
                let l' = (l :?> RRBFullNode<'T>).InsertChild owner shift idx' newChild
                l', r
            else
                let r' = (r :?> RRBFullNode<'T>).InsertChild owner shift idx newChild
                l, r'
        // Special case of 31/32 must also be considered here. Custom test for that one.

    member this.InsertAndSlideChildrenRight owner shift localIdx newChild (rightSibling : RRBFullNode<'T>) =
        let thisSize = this.NodeSize
        let sibSize = rightSibling.NodeSize
#if DEBUG
        if thisSize <> Literals.blockSize || sibSize >= Literals.blockSize then
            failwith <| sprintf "InsertAndSlideChildrenLeft should be called when this node is full (size %d) and left sibling is NOT full (size less than %d). Instead, this node was %A and left sibling was %A"
                                Literals.blockSize Literals.blockSize this rightSibling
#endif
        let totalSize = thisSize + sibSize
        let resultSizeL = totalSize >>> 1
        let resultSizeR = totalSize - resultSizeL
        let n = thisSize - resultSizeL
        let l, r = this.SlideChildrenRight owner shift n rightSibling
        if localIdx < resultSizeL then
            let l' = (l :?> RRBFullNode<'T>).InsertChild owner shift localIdx newChild
            l', r
        else
            // Inserting into right sibling
            // TODO: Double-check this calculation with specific unit tests
            let r' = (r :?> RRBFullNode<'T>).InsertChild owner shift (localIdx - resultSizeL) newChild
            l, r'
        // Special case of 31/32 must also be considered here. Custom test for that one.

    member this.InsertAndSplitNode owner shift localIdx newChild =
        let thisSize = this.NodeSize
        let half = thisSize >>> 1
        let shouldInsertIntoLeft = localIdx < half
        let lArr, rNode = this.SplitAndKeepNRight owner shift (thisSize - half)
        if shouldInsertIntoLeft then
            let lArr' = lArr |> Array.copyAndInsertAt localIdx newChild
            let lNode = RRBNode<'T>.MkNode owner shift lArr'
            lNode, rNode
        else
            let lNode  = RRBNode<'T>.MkNode owner shift lArr
            let rNode' = (rNode :?> RRBFullNode<'T>).InsertChild owner shift (localIdx - half) newChild
            lNode, rNode'

    override this.UpdatedTree owner shift treeIdx newItem =
        let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
        let newNode = child.UpdatedTree owner (down shift) nextIdx newItem
        this.UpdateChild owner shift localIdx newNode

    override this.InsertedTree owner shift treeIdx item parentOpt idxOfNodeInParent =
        let localIdx, child, nextLvlIdx = this.IndexesAndChild shift treeIdx
        let insertResult = child.InsertedTree owner (down shift) nextLvlIdx item (Some this) localIdx
        match insertResult with
        | SimpleInsertion newChild ->
            SimpleInsertion (this.UpdateChildSRel owner shift localIdx newChild 1)
        | SlidItemsLeft (newLeftChild, newChild) ->
            let node' = this.UpdateChildSAbs owner shift (localIdx - 1) newLeftChild (newLeftChild.TreeSize (down shift)) :?> RRBFullNode<'T>
            SimpleInsertion (node'.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift)))
            // TODO: Do I need an "Update two child items at once" function? What about the size table? We should be able to manage the size table more cleverly in RelaxedNodes.
        | SlidItemsRight (newChild, newRightChild) ->
            let newNode = this.UpdateChildSAbs owner shift localIdx newChild (newChild.TreeSize (down shift)) :?> RRBFullNode<'T>
            SimpleInsertion (newNode.UpdateChildSAbs owner shift (localIdx + 1) newRightChild (newRightChild.TreeSize (down shift)))
            // TODO: Comments from SlidItemsLeft re size table apply here too.
        | SplitNode (newLeftChild, newRightChild) ->
            if this.NodeSize < Literals.blockSize then
                let newNode = this.UpdateChildSAbs owner shift localIdx newLeftChild (newLeftChild.TreeSize (down shift)) :?> RRBFullNode<'T>
                SimpleInsertion (newNode.InsertChild owner shift (localIdx + 1) newRightChild)
            else
                let localIdx, _, _ = this.IndexesAndChild shift treeIdx
                match (parentOpt, idxOfNodeInParent) with
                | Some parent, idx when idx > 0 && parent.Children.[idx - 1].NodeSize < Literals.blockSize ->
                    // Room in the left sibling
                    let leftSib = parent.Children.[idx - 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdateChildSAbs owner shift localIdx newLeftChild (newLeftChild.TreeSize (down shift)) :?> RRBFullNode<'T>
                    let newLeft, newRight = newNode.InsertAndSlideChildrenLeft owner shift (localIdx + 1) newRightChild leftSib
                    SlidItemsLeft (newLeft, newRight)
                | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                    // Room in the right sibling
                    let rightSib = parent.Children.[idx + 1] :?> RRBFullNode<'T>
                    let newNode = this.UpdateChildSAbs owner shift localIdx newLeftChild (newLeftChild.TreeSize (down shift)) :?> RRBFullNode<'T>
                    let newLeft, newRight = newNode.InsertAndSlideChildrenRight owner shift (localIdx + 1) newRightChild rightSib
                    SlidItemsRight (newLeft, newRight)
                | _ ->
                    // No room left or right, so split
                    let newNode = this.UpdateChildSAbs owner shift localIdx newLeftChild (newLeftChild.TreeSize (down shift)) :?> RRBFullNode<'T>
                    let newLeft, newRight = newNode.InsertAndSplitNode owner shift localIdx newRightChild
                    SplitNode (newLeft, newRight)

    override this.GetTreeItem shift treeIdx =
        let _, child, nextTreeIdx = this.IndexesAndChild shift treeIdx
        child.GetTreeItem (down shift) nextTreeIdx

    override this.RemovedItem owner shift shouldCheckForRebalancing treeIdx =
        let localIdx, child, nextTreeIdx = this.IndexesAndChild shift treeIdx
        let item, child' = child.RemovedItem owner (down shift) shouldCheckForRebalancing nextTreeIdx
        let node' =
            if child'.NodeSize <= 0 then
                this.RemoveChild owner shift localIdx
            else
                this.UpdateChildSRel owner shift localIdx child' -1
        if shouldCheckForRebalancing && false then  // TODO: Replace "false" with something that checks the need for a rebalance. TODO: Write that function
            item, (node' :?> RRBFullNode<'T>).Rebalance owner shift
            // TODO: Decide whether we need to deal with expanding the node here or not.
        else
            item, node'

    override this.KeepNTreeItems owner shift treeIdx =
        // Possibilities:
        // treeIdx = 0; we drop all of our nodes and keep nothing, i.e. we return an empty node (or do we return None? Or can this even happen? Gotta think about it)
        // treeIdx = this.TreeSize; we keep everything and don't need to descend any further (and should NOT call IndexesAndChild in that case, as it would go off the end!)
(*
Thinking.

In vector, we check for keep=0 and shortcut to return an empty tree. We check for keep=tailOffset and shortcut to drop current tail and promote a new tail.
So if we're in the tree, we're going to end up keeping at least one item. If we keep one item, then the parent keeps one child and its parent keeps one child, and so on.
So each node will keep at least one child.

At the root of the tree, treeIdx = number of items to keep, so it's also the index of the first item to drop.
So when we do IndexesAndChild, we should check nextLvlIdx before moving on; if nextLvlIdx = 0, then our treeIdx happened to *just* split between
one child and the next, and the child we found is actually the *next* child. So the child we actually want in this scenario is "this.Children[localIdx - 1]",
and the localIdx should actually be one to the left as well for the next paragraph's work, and we should do a #if DEBUG verification that localIdx will never
be 0 in this scenario. But if nextLvlIdx is not 0, then the child we found is indeed the child we want, and we're going to split that child in two at some point.

Now that we have a localIdx and a child, we should first get a node' from this node that does "KeepNLeft" with the tweaked localIdx.
Then we calculate the child' by doing (child.KeepNTreeItems (down shift) nextLvlIdx) and then we'll do an UpdateChildSAbs on ourselves (calculating child'.TreeSize (down shift)).
And now we have the result node that we're ready to return.

For the sake of expanded nodes, the SplitTree method (which we still need to write) won't just do (Keep, Skip) but will use SplitAndKeepNRight (NOT Left) so that
we won't shrink any right-hand nodes; instead, we'll have to create a MakeLeftNodeForSplit that expanded nodes will use to create an expanded node from the
left array. And SplitTree might have to decide whether to use an S variant or not; probably use an S variant everywhere for safety, but I need to think about this one.
*)
        let localIdx, child, nextTreeIdx = this.IndexesAndChild shift treeIdx
        if nextTreeIdx = 0 then
            // Splitting exactly between two subtrees, no need to recurse further down
#if DEBUG
            if localIdx = 0 then failwith "In KeepNTreeItems, localIdx should never be 0 when nextTreeIdx = 0, because that means we should have stopped at a level further up"
            if localIdx < 0 then failwith "In KeepNTreeItems, localIdx should never be < 0"
#endif
            this.KeepNLeft owner shift localIdx
        else
            // Splitting child in two, so recurse
            let node' = this.KeepNLeft owner shift (localIdx + 1)
            let child' = child.KeepNTreeItems owner (down shift) nextTreeIdx
            (node' :?> RRBFullNode<'T>).UpdateChildSAbs owner shift localIdx child' (child'.TreeSize (down shift))

(*
member this.SkipNTreeItems is going to be similar, but localIdx is the *start*, not the end, and we'll do KeepNRight and need to watch for fencepost errors calculating N
So if nextTreeIdx = 0, then we're still splitting between two subtrees, and we KeepNRight from localIdx to NodeSize and then don't recurse further down
What if nextTreeIdx = this.TreeSize shift? Can that happen? I think it can't, but TODO: Think this through, since as of this writing I'm too tired to think about it
*)
    override this.SkipNTreeItems owner shift treeIdx =
        let localIdx, child, nextTreeIdx = this.IndexesAndChild shift treeIdx
        let keep = this.NodeSize - localIdx
        if nextTreeIdx = 0 then
            // Splitting exactly between two subtrees, no need to recurse further down
#if DEBUG
            if localIdx = 0 then failwith "In SkipNTreeItems, localIdx should never be 0 when nextTreeIdx = 0, because that means we should have stopped at a level further up"
            if localIdx < 0 then failwith "In SkipNTreeItems, localIdx should never be < 0"
#endif
            this.KeepNRight owner shift keep
        else
            // Splitting child in two, so recurse
            let node' = this.KeepNRight owner shift keep
            let child' = child.SkipNTreeItems owner (down shift) nextTreeIdx
            (node' :?> RRBFullNode<'T>).UpdateChildSAbs owner shift 0 child' (child'.TreeSize (down shift))

    override this.SplitTree owner shift treeIdx =
        // treeIdx is first index of right-hand tree
        let localIdx, child, nextTreeIdx = this.IndexesAndChild shift treeIdx
        let keep = this.NodeSize - localIdx
        if nextTreeIdx = 0 then
            // Splitting exactly between two subtrees, no need to recurse further down
#if DEBUG
            if localIdx = 0 then failwith "In SplitTree, localIdx should never be 0 when nextTreeIdx = 0, because that means we should have stopped at a level further up"
            if localIdx < 0 then failwith "In SplitTree, localIdx should never be < 0"
#endif
            let (leftChildren, leftSizes), rightNode = this.SplitAndKeepNRightS owner shift keep
            let leftNode = this.MakeLeftNodeForSplit owner shift leftChildren leftSizes
            leftNode, rightNode
        else
            // Splitting child in two, so recurse
            let (leftChildren, leftSizes), rightNode = this.SplitAndKeepNRightS owner shift keep
            let leftNode = this.MakeLeftNodeForSplit owner shift leftChildren leftSizes
            let childL, childR = child.SplitTree owner (down shift) nextTreeIdx
            let leftNode' = (leftNode :?> RRBFullNode<'T>).AppendChild owner shift childL
            let rightNode' = (rightNode :?> RRBFullNode<'T>).UpdateChildSAbs owner shift 0 childR (childR.TreeSize (down shift))
            leftNode', rightNode'

    abstract member MakeLeftNodeForSplit : OwnerToken -> int -> RRBNode<'T>[] -> int[] -> RRBNode<'T>
    default this.MakeLeftNodeForSplit owner shift children sizes =
        RRBNode<'T>.MkNodeKnownSize owner shift children sizes

    member this.RemoveLastLeaf owner shift =
        // EXPAND: This needs an implementation in expanded nodes, where we expand the new last child after shrinking the child we return
        if shift <= Literals.blockSizeShift then
            // Children are leaves
            if this.NodeSize = 0 then
                failwith <| sprintf "RemoveLastLeaf was called on an empty node at shift %d" shift  // TODO: Remove this once unit testing is complete
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

    member this.PopLastLeaf owner shift = this.RemoveLastLeaf owner shift // TODO: Replace all occurrences of "RemoveLastLeaf" in RRBVector code with "PopLastLeaf"

    // TODO: Also need functions to add a new leaf at the end of the tree, and to prepend a leaf at the *beginning* of the tree.
    // Prepending a leaf is used in appending two vectors; if the left vector is leaf-only, or a root+leaf sapling, then we'll just prepend one or two leaves, then rebalance the leftmost twig.
    // Appending a leaf is used when the tail is already full while appending an item to the vector. And the reason for the invariant (the last leaf is full if its parent is full) is so that
    // when you append a full leaf, which happens all the time, you can count on not having to convert the full node above it to a relaxed node. (If the last leaf had NOT been full, then
    // you could have had M;M;M;M-2 and had a full node, but add a full leaf and you get M;M;M;M-2;M which must be a relaxed node).

    // --- REBALANCING ---

    // TODO: We might need one more low-level function to use in here: ExpandLastChildIfNeeded, which is a no-op in compact nodes but does something in expanded nodes.
    // DESIGN: Two approaches possible. One would use SlideChildrenLeft/Right to move things around... but that doesn't (currently) exist for leaves.
    // Another approach would be to always build arrays and then rebuild size tables. Which is better? We'll just have to try it.

    member this.Rebalance (owner : OwnerToken) (shift : int) =
        let childrenSeq = this.Children |> Seq.truncate this.NodeSize
        let len = this.NodeSize
        this.RebalanceImpl owner shift len childrenSeq |> fst

    member this.Rebalance2 (owner : OwnerToken) (shift : int) (right : RRBFullNode<'T>) =
        this.Rebalance2Plus1 owner shift None right

    member this.Rebalance2Plus1 (owner : OwnerToken) (shift : int) (middle : RRBLeafNode<'T> option) (right : RRBFullNode<'T>) =
#if DEBUG
        if middle.IsSome && shift <> Literals.blockSizeShift then
            failwith <| sprintf "Rebalancing two nodes plus a middle leaf is only allowed at twig level. Instead, parameters were shift=%d, middle=%A, right=%A" shift middle.Value right
#endif
        let totalLength, childrenSeq =
            match middle with
            | None -> this.NodeSize + right.NodeSize, Seq.append (this.Children |> Seq.truncate this.NodeSize) (right.Children |> Seq.truncate right.NodeSize)
            | Some leaf -> this.NodeSize + 1 + right.NodeSize, seq {
                yield! (this.Children  |> Seq.truncate this.NodeSize)
                yield leaf
                yield! (right.Children |> Seq.truncate right.NodeSize)
            }
        this.RebalanceImpl owner shift totalLength childrenSeq

    member this.RebalanceImpl (owner : OwnerToken) (shift : int) (totalLength : int) (childrenSeq : RRBNode<'T> seq) =
        let sizes = childrenSeq |> Seq.map (fun node -> node.NodeSize)
        // let idx, mergeLen = findMergeCandidates sizes len
        // let sizeReduction = 1
        let idx, mergeLen, sizeReduction = findMergeCandidatesTwoPasses sizes totalLength
        let newLen = totalLength - sizeReduction
        use items = childrenSeq.GetEnumerator()
        if newLen <= Literals.blockSize then
            let arr = this.MkArrayForRebalance owner shift newLen
            arr |> Array.fillFromEnumerator items 0 idx
            let mergedChildrenSeq = this.ApplyRebalancePlan owner shift sizes (idx, mergeLen, sizeReduction) items
            arr |> Array.fillFromSeq mergedChildrenSeq idx (mergeLen - sizeReduction)
            arr |> Array.fillFromEnumerator items (idx + mergeLen - sizeReduction) newLen
            this.MkNodeForRebalance owner shift arr newLen, None
        else
            let lenL = Literals.blockSize
            let lenR = newLen - lenL
            let arrL = this.MkArrayForRebalance owner shift lenL
            let arrR = Array.zeroCreate lenR
            Array.fill2FromEnumerator items 0 idx arrL arrR
            let mergedChildrenSeq = this.ApplyRebalancePlan owner shift sizes (idx, mergeLen, sizeReduction) items
            Array.fill2FromSeq mergedChildrenSeq idx (mergeLen - sizeReduction) arrL arrR
            Array.fill2FromEnumerator items (idx + mergeLen - sizeReduction) newLen arrL arrR
            this.MkNodeForRebalance owner shift arrL lenL, (this.MkNodeForRebalance owner shift arrR lenR |> Some)

    abstract member MkArrayForRebalance : OwnerToken -> int -> int -> RRBNode<'T> []
    default this.MkArrayForRebalance owner shift length =
        // In expanded nodes, this will return "this.Children" -- and MkNodeForRebalance will "zero out" its remaining children if its size shrank (which it should)
        Array.zeroCreate length

    abstract member MkNodeForRebalance : OwnerToken -> int -> RRBNode<'T> [] -> int -> RRBNode<'T>
    default this.MkNodeForRebalance owner shift arr len =
        // In expanded nodes, this will return "this" -- but first setting the length and zeroing out any remaining children since the length probably shrank
        RRBNode<'T>.MkNode owner shift arr

    member this.ApplyRebalancePlanImpl<'C> sizes (mergeStart, mergeLen, sizeReduction) (childrenEnum : System.Collections.Generic.IEnumerator<RRBNode<'T>>) (getGrandChildren : RRBNode<'T> -> 'C []) (mkNode : 'C [] -> RRBNode<'T>) =
        let totalSlots = sizes |> Seq.skip mergeStart |> Seq.truncate mergeLen |> Seq.sum
#if DEBUG
        let arrayCount = totalSlots / Literals.blockSize
        let remainingSize = totalSlots % Literals.blockSize
        if arrayCount + (if remainingSize = 0 then 0 else 1) + sizeReduction <> mergeLen
        then
            let childrenArr : RRBNode<'T> [] = Array.createFromEnumerator childrenEnum mergeLen
            failwith <| sprintf "Made a miscalculation somewhere; total slots %d yielded an array count of %d which, added to the size reduction %d, should have equaled merge length %d but didn't. Children to merge were %A" totalSlots arrayCount sizeReduction mergeLen childrenArr
#endif
        let grandChildrenSeq : 'C seq =
            seq {
                while childrenEnum.MoveNext() do
                    let child = childrenEnum.Current
                    yield! (getGrandChildren child)
            }
        let arraysSeq = Array.createManyFromSeq grandChildrenSeq totalSlots Literals.blockSize
        let nodesSeq = arraysSeq |> Seq.map mkNode
        nodesSeq

    member this.ApplyRebalancePlan owner shift sizes (mergeStart, mergeLen, sizeReduction) childrenEnum =
        if shift > Literals.blockSizeShift then
            this.ApplyRebalancePlanImpl<RRBNode<'T>> sizes (mergeStart, mergeLen, sizeReduction) childrenEnum (fun (node : RRBNode<'T>) -> (node :?> RRBFullNode<'T>).Children) (RRBNode<'T>.MkNode owner shift)
        else
            this.ApplyRebalancePlanImpl<'T> sizes (mergeStart, mergeLen, sizeReduction) childrenEnum (fun (leaf : RRBNode<'T>) -> (leaf :?> RRBLeafNode<'T>).Items) (RRBNode<'T>.MkLeaf owner)

    member this.ConcatNodes owner shift (right : RRBFullNode<'T>) =
        if this.NeedsRebalance2 shift right
        then this.Rebalance2 owner shift right
        elif this.NodeSize + right.NodeSize > Literals.blockSize then
            // Can't fit into a single node, so save time by not rewriting
            this :> RRBNode<'T>, Some (right :> RRBNode<'T>)
        else
            let left' =
                // Combine as efficiently as possible by re-using right node's size table if it exists
                if right :? RRBRelaxedNode<'T>
                then this.AppendNChildrenS owner shift right.NodeSize right.Children (right :?> RRBRelaxedNode<'T>).SizeTable false
                else this.AppendNChildren  owner shift right.NodeSize right.Children false
            left', None
    // That will form part of the MergeTrees logic, which will be used in concatenating vectors.

    member this.ConcatTwigsPlusLeaf owner shift (middle : RRBLeafNode<'T>) (right : RRBFullNode<'T>) =
        // Usually used when concatenating two vectors, where `middle` will be the tail of the left vector
        // Will ONLY be called when there's room to merge in the middle leaf, whether directly or by rebalancing
#if DEBUG
        if shift <> Literals.blockSizeShift then
            failwith <| sprintf "ConcatTwigsPlusLeaf called with shift <> Literals.blockSizeShift (%d); shift called with was %d instead. Left was %A, middle was %A, and right was %A" Literals.blockSizeShift shift this middle right
        if not (this.HasRoomToMergeTheTail shift middle right) then
            failwith <| sprintf "ConcatTwigsPlusLeaf should only be called when there is room to merge the tail, but there wasn't in this call. Left was %A, middle was %A, and right was %A" this middle right
#endif
        if this.NeedsRebalance2PlusLeaf shift middle.NodeSize right then
            this.Rebalance2Plus1 owner shift (Some middle) right
        elif this.NodeSize + 1 + right.NodeSize <= Literals.blockSize then
            // If we can compress into a single node, then the time spent rewriting the node is worth it
            let newLen = this.NodeSize + 1 + right.NodeSize
            let arr = this.MkArrayForRebalance owner shift newLen
            let items = seq {
                yield! this.Children |> Seq.truncate this.NodeSize
                yield middle :> RRBNode<'T>
                yield! right.Children |> Seq.truncate right.NodeSize
            }
            arr |> Array.fillFromSeq items 0 newLen
            this.MkNodeForRebalance owner shift arr newLen, None
        elif this.NodeSize < Literals.blockSize then
            // Save time by not rewriting right node
            let node' = this.AppendChild owner shift middle
            node', Some (right :> RRBNode<'T>)
        elif right.NodeSize < Literals.blockSize then
            // Save time by not rewriting left node
            let right' = right.InsertChild owner shift 0 middle
            this :> RRBNode<'T>, Some right'
        else
            failwith <| sprintf "ConcatTwigsPlusLeaf should only be called when there is room to merge the tail, and HasRoomToMergeTheTail reported true. Since left and right were both full, that should mean that the tail was mergeable because a rebalance was possible... but we didn't rebalance. This resulted in a tail merge that wasn't actually possible. Must find out why. Left was %A, middle was %A, and right was %A" this middle right

    member this.HasRoomToMergeTheTail shift (tail : RRBLeafNode<'T>) (right : RRBFullNode<'T>) =
        this.NodeSize  < Literals.blockSize
     || right.NodeSize < Literals.blockSize
     || this.NeedsRebalance2PlusLeaf shift tail.NodeSize right

    member this.MergeTree owner shift (tailOpt : RRBLeafNode<'T> option) rightShift (right : RRBFullNode<'T>) shouldKeepExpandedLeftNode =
        let shrinkLeftNode owner shouldKeepExpandedLeftNode (pair : RRBNode<'T> * RRBNode<'T> option) =
            match pair with
            | l, None when shouldKeepExpandedLeftNode -> l, None
            | l, None -> l.Shrink owner, None
            | l, Some r -> l.Shrink owner, Some r
        if shift = Literals.blockSizeShift && rightShift = Literals.blockSizeShift then
            match tailOpt with
            | None -> this.ConcatNodes owner shift right
            | Some tail -> this.ConcatTwigsPlusLeaf owner shift tail right
            |> shrinkLeftNode owner shouldKeepExpandedLeftNode
        elif shift = rightShift then
            let childL = this.LastChild :?> RRBFullNode<'T>
            let childR = right.FirstChild :?> RRBFullNode<'T>
            // TODO: Need to check if either node is expanded, and if so, figure out whether ConcatNodes should produce an expanded result
            // ... which might be trimmed up above. Perhaps for simplicity's sake, this function should acquire a boolean parameter "expandRightAsNeeded"?
            match childL.MergeTree owner (down shift) tailOpt (down rightShift) childR shouldKeepExpandedLeftNode with
            | child', None ->
                let parentL = this.UpdateChildSAbs owner shift (this.NodeSize - 1) child' (child'.TreeSize (down shift))
                if right.NodeSize > 1 then
                    let parentR = right.RemoveChild owner shift 0
                    (parentL :?> RRBFullNode<'T>).ConcatNodes owner shift (parentR :?> RRBFullNode<'T>)
                    |> shrinkLeftNode owner shouldKeepExpandedLeftNode
                else
                    (parentL, None)
                    |> shrinkLeftNode owner shouldKeepExpandedLeftNode
            | childL', Some childR' ->
                let parentL = this.UpdateChildSAbs owner shift (this.NodeSize - 1) childL' (childL'.TreeSize (down shift))
                let parentR = right.UpdateChildSAbs owner shift 0 childR' (childR'.TreeSize (down rightShift))
                (parentL :?> RRBFullNode<'T>).ConcatNodes owner shift (parentR :?> RRBFullNode<'T>)
                |> shrinkLeftNode owner shouldKeepExpandedLeftNode
        elif shift < rightShift then
            let childR = right.FirstChild :?> RRBFullNode<'T>
            match this.MergeTree owner shift tailOpt (down rightShift) childR shouldKeepExpandedLeftNode with
            | child', None ->
                let parentR = right.UpdateChildSAbs owner shift 0 child' (child'.TreeSize (down rightShift))
                (parentR, None)  // TODO: Test this: do I need to shrink parentR?
                |> shrinkLeftNode owner shouldKeepExpandedLeftNode
            | childL', Some childR' ->
                let parentL = (childL' :?> RRBFullNode<'T>).NewParent owner (down rightShift) [|childL'|]
                let parentR = right.UpdateChildSAbs owner rightShift 0 childR' (childR'.TreeSize (down rightShift))
                (parentL :?> RRBFullNode<'T>).ConcatNodes owner rightShift (parentR :?> RRBFullNode<'T>)
                |> shrinkLeftNode owner shouldKeepExpandedLeftNode
        else // shift > rightShift
            let childL = this.LastChild :?> RRBFullNode<'T>
            match childL.MergeTree owner (down shift) tailOpt rightShift right shouldKeepExpandedLeftNode with
            | child', None ->
                let parentL = this.UpdateChildSAbs owner shift (this.NodeSize - 1) child' (child'.TreeSize (down shift))
                (parentL, None)  // TODO: Test this
                |> shrinkLeftNode owner shouldKeepExpandedLeftNode
            | childL', Some childR' ->
                let parentL = this.UpdateChildSAbs owner shift (this.NodeSize - 1) childL' (childL'.TreeSize (down shift))
                let parentR = (childR' :?> RRBFullNode<'T>).NewParent owner (down shift) [|childR'|]
                (parentL :?> RRBFullNode<'T>).ConcatNodes owner shift (parentR :?> RRBFullNode<'T>)
                |> shrinkLeftNode owner shouldKeepExpandedLeftNode

    // TODO: Write "member this.NewParent" for other types of nodes (because expanded nodes will want to create an expanded parent)
    // TODO: Nope, we've done that but now we need to **change the API** because "this" might not always be the left node
    // So instead, the API needs to become "siblings" as an array instead of "rightSibling"
    abstract member NewParent : OwnerToken -> int -> RRBNode<'T> [] -> RRBNode<'T>
    default this.NewParent owner shift siblings =
        RRBNode<'T>.MkNode owner (up shift) siblings
(*

    let setOrRemoveFirstChild<'T> thread shift newChild parentArray =
        if newChild |> Array.isEmpty
        then parentArray |> Array.copyAndRemoveFirst
        else parentArray |> Array.copyAndSet 0 (NodeCreation.mkRRBNode<'T> thread (down shift) newChild |> box)

    let setOrRemoveLastChild<'T> thread shift newChild parentArray =
        if newChild |> Array.isEmpty
        then parentArray |> Array.copyAndPop
        else parentArray |> Array.copyAndSetLast (NodeCreation.mkRRBNode<'T> thread (down shift) newChild |> box)

    let rec mergeTree thread aShift (a:Node) bShift (b:Node) (tail : 'T[]) =
        if aShift <= Literals.blockSizeShift && bShift <= Literals.blockSizeShift then
            // At twig level on both nodes
            mergeWithTail thread aShift a tail b
        else
            if aShift < bShift then
                let aR, bL = mergeTree thread aShift a (down bShift) (b |> getChildNode 0) tail
                let a' = if aR |> Array.isEmpty then [||] else [|NodeCreation.mkRRBNode<'T> thread (down bShift) aR |> box|]
                let b' = b.Children |> setOrRemoveFirstChild<'T> thread bShift bL
                mergeArrays<'T> thread bShift a' b'
            elif aShift > bShift then
                let aR, bL = mergeTree thread (down aShift) (getLastChildNode a) bShift b tail
                let a' = a.Children |> setOrRemoveLastChild<'T>  thread aShift aR
                let b' = if bL |> Array.isEmpty then [||] else [|NodeCreation.mkRRBNode<'T> thread (down aShift) bL |> box|]
                mergeArrays<'T> thread aShift a' b'
            else
                let aR,  bL  = getLastChildNode a, b |> getChildNode 0
                let aR', bL' = mergeTree thread (down aShift) aR (down bShift) bL tail
                let a' = a.Children |> setOrRemoveLastChild<'T>  thread aShift aR'
                let b' = b.Children |> setOrRemoveFirstChild<'T> thread bShift bL'
                mergeArrays<'T> thread aShift a' b'

*)

and [<StructuredFormatDisplay("RelaxedNode({StringRepr})")>] RRBRelaxedNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[], sizeTable : int[]) =
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

    member this.StringRepr : string = sprintf "length=%d, sizetable=%A, children=%A" this.NodeSize this.SizeTable this.Children

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBRelaxedNode<'T>(owner, Array.copy children, Array.copy sizeTable) :> RRBNode<'T>

    override this.Shrink owner = this.GetEditableNode owner
    override this.Expand owner =
        let node' = this.GetEditableNode owner :?> RRBRelaxedNode<'T>
        RRBExpandedRelaxedNode<'T>(owner, node'.Children, node'.SizeTable) :> RRBNode<'T>

    abstract member ToFullNodeIfNeeded : int -> RRBNode<'T>
    default this.ToFullNodeIfNeeded shift =
        if RRBMath.isSizeTableFullAtShift shift sizeTable sizeTable.Length
        then RRBFullNode<'T>(this.Owner, this.Children) :> RRBNode<'T>
        else this :> RRBNode<'T>

    override this.IndexesAndChild shift treeIdx =
        let mutable localIdx = radixIndex shift treeIdx
        while this.SizeTable.[localIdx] <= treeIdx do
            localIdx <- localIdx + 1
        let child = this.Children.[localIdx]
        let nextTreeIdx = if localIdx = 0 then treeIdx else treeIdx - this.SizeTable.[localIdx - 1]
        localIdx, child, nextTreeIdx

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
        let sizeTable' = this.SizeTable |> Array.copyAndInsertAt localIdx (newChildSize + (if localIdx <= 0 then 0 else this.SizeTable.[localIdx-1]))
        for i = localIdx + 1 to this.NodeSize do
            sizeTable'.[i] <- sizeTable'.[i] + newChildSize
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.RemoveChild owner shift localIdx =
        let oldChildSize = this.SizeTable.[localIdx] - (if localIdx <= 0 then 0 else this.SizeTable.[localIdx-1])
        let children' = this.Children |> Array.copyAndRemoveAt localIdx
        let sizeTable' = this.SizeTable |> Array.copyAndRemoveAt localIdx
        for i = localIdx to this.NodeSize - 2 do
            sizeTable'.[i] <- sizeTable'.[i] - oldChildSize
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
        if n = this.NodeSize then this :> RRBNode<'T> else
        let children' = this.Children |> Array.truncate n
        let sizeTable' = this.SizeTable |> Array.truncate n
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'

    override this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
        if skip = 0 then this :> RRBNode<'T> else
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
        for i = 0 to this.NodeSize - skip - 1 do
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
        let skip = this.NodeSize - n
        let l, r = this.Children |> Array.splitAt skip
        let lS, rS = this.SizeTable |> Array.splitAt skip
        if not (lS |> Array.isEmpty) then
            let lastSize = lS |> Array.last
            for i = 0 to rS.Length - 1 do
                rS.[i] <- rS.[i] - lastSize
        let node' = RRBNode<'T>.MkNodeKnownSize owner shift r rS
        ((l, lS), node')

    override this.AppendNChildren owner shift n newChildren shouldExpandRightChildIfNeeded =
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

    override this.AppendNChildrenS owner shift n newChildren sizes shouldExpandRightChildIfNeeded =
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
        this.SizeTable.CopyTo(sizeTable', n)
        let mutable prevSizeTableEntry = 0
        use eC = newChildren.GetEnumerator()
        for i = 0 to n - 1 do
            if eC.MoveNext() then
                children'.[i] <- eC.Current
                let childSize = eC.Current.TreeSize (down shift)
                let nextSizeTableEntry = prevSizeTableEntry + childSize
                sizeTable'.[i] <- nextSizeTableEntry
                prevSizeTableEntry <- nextSizeTableEntry
        for i = n to newSize - 1 do
            sizeTable'.[i] <- sizeTable'.[i] + prevSizeTableEntry
        RRBNode<'T>.MkNodeKnownSize owner shift children' sizeTable'
        // TODO: That's *almost* identical to the version in RRBFullNode<'T> - separate out the common code and combine it

    override this.PrependNChildrenS owner shift n newChildren sizes =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let size = this.NodeSize
        let newSize = size + n
        let children' = Array.zeroCreate newSize
        this.Children.CopyTo(children', n)
        let sizeTable' = Array.zeroCreate newSize
        this.SizeTable.CopyTo(sizeTable', n)
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


    // TODO: If there's a good way to implement this, do so. Might be more efficient, might not.
    // override this.InsertAndSlideChildrenLeft owner shift localIdx newChild leftSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSlideChildrenRight owner shift localIdx newChild rightSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSplitNode owner shift localIdx newChild =
    //     failwith "Not implemented"



// === EXPANDED NODES ===

// Expanded nodes are used in transient trees. Their arrays are always Literals.blockSize in size, so an int field
// is used to keep track of the node's actual size. The rest of the node is filled with nulls (or the default value
// of 'T in the case of leaves). This allows appends to be *very* fast.

and [<StructuredFormatDisplay("ExpandedFullNode({StringRepr})")>] RRBExpandedFullNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[], ?realSize : int) =
    inherit RRBFullNode<'T>(ownerToken, Array.expandToBlockSize children)

    member val CurrentLength : int = defaultArg realSize (Array.length children) with get, set
    override this.NodeSize = this.CurrentLength
    override this.SlotCount = this.Children |> Seq.take this.NodeSize |> Seq.sumBy (fun child -> child.NodeSize)

    member this.StringRepr : string = sprintf "length=%d, children=%A%s"
                                              this.NodeSize
                                              (this.Children |> Array.truncate this.NodeSize)
                                              (if this.NodeSize >= Literals.blockSize then "" else sprintf " (plus %d nulls)" (Literals.blockSize - this.NodeSize))

    override this.Shrink owner =
        if this.IsEditableBy owner then
            RRBFullNode<'T>(owner, this.Children) :> RRBNode<'T>
        else
            let size = this.NodeSize
            let children' =
                if size = Literals.blockSize
                then Array.copy this.Children
                else this.Children |> Array.truncate size
            RRBFullNode<'T>(owner, children') :> RRBNode<'T>

    override this.Expand owner = this.GetEditableNode owner

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedFullNode<'T>(owner, Array.copy this.Children, this.NodeSize) :> RRBNode<'T>

    override this.SetNodeSize newSize = this.CurrentLength <- newSize

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
        // Expanded nodes always have their rightmost child, and only that child, expanded
        if oldSize > 0 then
            let lastChild = node'.LastChild
            let shrunkLastChild = lastChild.Shrink owner
            if not (isSameObj lastChild shrunkLastChild) then
                node'.Children.[oldSize - 1] <- shrunkLastChild
        node'.Children.[oldSize] <- newChild.Expand owner
        node'.SetNodeSize (oldSize + 1)
        // Full nodes are allowed to have their last item be non-full, so we have to check that
        if trulyFull then
            node' :> RRBNode<'T>
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
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', node'.NodeSize) :> RRBNode<'T>
        // TODO: Might be able to optimize this by first checking if we're truly full, and then if we're not, convert to a relaxed node FIRST before calling RelaxedNode.AppendChildS

    override this.AppendChildS owner shift newChild _newChildSize =
        this.AppendChild owner shift newChild

    override this.InsertChild owner shift localIdx newChild =
        this.InsertChildS owner shift localIdx newChild (newChild.TreeSize (down shift))

    override this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let oldSize = node'.NodeSize
        for i = oldSize - 1 downto localIdx do
            node'.Children.[i+1] <- node'.Children.[i]
        node'.Children.[localIdx] <- newChild
        node'.SetNodeSize (oldSize + 1)
        if newChildSize = (1 <<< shift) then
            // Inserted a full child, so this is still a full node
            node' :> RRBNode<'T>
        else
            node'.ToRelaxedNodeIfNeeded shift

    override this.RemoveChild owner shift localIdx =
        if localIdx = this.NodeSize - 1 then this.RemoveLastChild owner shift else
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let newSize = node'.NodeSize - 1
        for i = localIdx to newSize - 1 do
            node'.Children.[i] <- node'.Children.[i+1]
        node'.Children.[newSize] <- null
        node'.SetNodeSize newSize
        // Removing a child from a full node can never make it non-full
        node' :> RRBNode<'T>

    override this.RemoveLastChild owner shift =
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let newSize = node'.NodeSize - 1
        node'.Children.[newSize] <- null
        node'.SetNodeSize newSize
        if newSize > 0 then
            let lastChild = node'.LastChild
            let expandedLastChild = lastChild.Expand owner
            if not (isSameObj lastChild expandedLastChild) then
                node'.Children.[newSize - 1] <- expandedLastChild
        // Removing the last child from a full node can never make it non-full
        node' :> RRBNode<'T>

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
        if n = this.NodeSize then this :> RRBNode<'T> else
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
        node'.SetNodeSize n
        if n > 0 then
            let lastChild = node'.LastChild
            let expandedLastChild = lastChild.Expand owner
            if not (isSameObj lastChild expandedLastChild) then
                node'.Children.[n - 1] <- expandedLastChild
        node' :> RRBNode<'T>
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    override this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
        if skip = 0 then this :> RRBNode<'T> else
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        for i = 0 to n - 1 do
            node'.Children.[i] <- node'.Children.[i + skip]
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
        node'.SetNodeSize n
        node' :> RRBNode<'T>
        // Cannot become a relaxed node, since full nodes have all but their last child full (and the last child may be full)

    override this.SplitAndKeepNLeft owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeft, n must be at least one, got %d. This node: %A" n this
#endif
        let r = Array.sub this.Children n (this.NodeSize - n)
        let rLen = r.Length
        if rLen > 0 then
            let lastChild = r.[rLen - 1]
            let shrunkLastChild = lastChild.Shrink owner
            if not (isSameObj lastChild shrunkLastChild) then
                r.[rLen - 1] <- shrunkLastChild
        let node' = this.KeepNLeft owner shift n
        (node', r)

    override this.SplitAndKeepNLeftS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNLeftS, n must be at least one, got %d. This node: %A" n this
#endif
        let size = this.NodeSize
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let lS, rS = sizeTable |> Array.splitAt n
        let lastSize = lS |> Array.last
        for i = 0 to rS.Length - 1 do
            rS.[i] <- rS.[i] - lastSize
        let node', r = this.SplitAndKeepNLeft owner shift n
        (node', (r, rS))

    override this.SplitAndKeepNRight owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRight, n must be at least one, got %d. This node: %A" n this
#endif
        let l = Array.sub this.Children 0 (this.NodeSize - n)
        let node' = this.KeepNRight owner shift n
        (l, node')

    override this.SplitAndKeepNRightS owner shift n =
#if DEBUG
        if n <= 0 then
            failwithf "In SplitAndKeepNRightS, n must be at least one, got %d. This node: %A" n this
#endif
        let size = this.NodeSize
        let skip = size - n
        let l = Array.sub this.Children 0 skip
        let sizeTable = this.BuildSizeTable shift size (size-1)
        let lS = Array.sub sizeTable 0 skip
        let node' = this.KeepNRight owner shift n
        ((l, lS), node')

    override this.AppendNChildren owner shift n newChildren shouldExpandRightChildIfNeeded =
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let size = node'.NodeSize
        // Expanded nodes always have their rightmost child, and only that child, expanded
        let lastChild = node'.LastChild
        let shrunkLastChild = lastChild.Shrink owner
        if not (isSameObj lastChild shrunkLastChild) then
            node'.Children.[size - 1] <- shrunkLastChild
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
        if shouldExpandRightChildIfNeeded then
            let newLastChild = node'.LastChild
            let expandedNewLastChild = newLastChild.Expand owner
            if not (isSameObj newLastChild expandedNewLastChild) then
                node'.Children.[newSize - 1] <- expandedNewLastChild
        if stillFull then
            node' :> RRBNode<'T>
        else
            node'.ToRelaxedNodeIfNeeded shift

    override this.AppendNChildrenS owner shift n newChildren sizes shouldExpandRightChildIfNeeded =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let size = node'.NodeSize
        // Expanded nodes always have their rightmost child, and only that child, expanded
        let lastChild = node'.LastChild
        let shrunkLastChild = lastChild.Shrink owner
        if not (isSameObj lastChild shrunkLastChild) then
            node'.Children.[size - 1] <- shrunkLastChild
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
        if shouldExpandRightChildIfNeeded then
            let newLastChild = node'.LastChild
            let expandedNewLastChild = newLastChild.Expand owner
            if not (isSameObj newLastChild expandedNewLastChild) then
                node'.Children.[newSize - 1] <- expandedNewLastChild
        if stillFull then
            node' :> RRBNode<'T>
        else
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', newSize) :> RRBNode<'T>

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
            node' :> RRBNode<'T>
        else
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', newSize) :> RRBNode<'T>

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
            node' :> RRBNode<'T>
        else
            RRBExpandedRelaxedNode<'T>(owner, node'.Children, sizeTable', newSize) :> RRBNode<'T>

    override this.ExpandLastChildIfNeeded owner =
        // TODO: Turn this into ExpandRightSpineIfNeeded by calling recursively on the (newly?)-expanded child
        if this.NodeSize = 0 then this :> RRBNode<'T> else
        let node' = this.GetEditableNode owner :?> RRBExpandedFullNode<'T>
        let lastChild = this.LastChild
        let expandedLastChild = lastChild.Expand owner
        if not (isSameObj lastChild expandedLastChild) then
            node'.Children.[node'.NodeSize - 1] <- expandedLastChild
        node' :> RRBNode<'T>

    // ===== END of NODE MANIPULATION functions =====

    override this.MkArrayForRebalance owner shift length =
        if this.IsEditableBy owner then this.Children else Array.zeroCreate length

    override this.MkNodeForRebalance owner shift arr len =
        if isSameObj arr this.Children && this.IsEditableBy owner then
            for i = len to this.NodeSize - 1 do
                arr.[i] <- null
            this.SetNodeSize len
            this.ToRelaxedNodeIfNeeded shift
        else
            RRBNode<'T>.MkNode owner shift arr

    override this.MakeLeftNodeForSplit owner shift children sizes =
        (RRBNode<'T>.MkNodeKnownSize owner shift children sizes).Expand owner

    // override this.InsertAndSlideChildrenLeft owner shift localIdx newChild leftSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSlideChildrenRight owner shift localIdx newChild rightSibling =
    //     failwith "Not implemented"

    // override this.InsertAndSplitNode owner shift localIdx newChild =
    //     failwith "Not implemented"

    override this.NewParent owner shift siblings =
        let arr = Array.zeroCreate<RRBNode<'T>> Literals.blockSize
        let size = siblings |> Array.length
#if DEBUG
        if size <= 0 then failwith <| sprintf "Empty array passed to this.NewParent -- not allowed! owner=%A shift=%A this=%A" owner shift this
#endif
        for i = 0 to size - 2 do
            arr.[i] <- siblings.[i].Shrink owner
        arr.[size - 1] <- siblings.[size - 1].Expand owner
        // TODO: Examine the logic of this "if" statement and see if we can instead write a static RRBExpandedRelaxedNode.Create method that will take care of this
        if size = 1 || (this.NodeSize = Literals.blockSize && this.FullNodeIsTrulyFull shift) then
            RRBExpandedFullNode<'T>(owner, arr, size) :> RRBNode<'T>
        else
            let sizeTable = RRBNode<'T>.CreateSizeTableS (up shift) arr size
            RRBExpandedRelaxedNode<'T>(owner, arr, sizeTable, size) :> RRBNode<'T>



and [<StructuredFormatDisplay("ExpandedRelaxedNode({StringRepr})")>] RRBExpandedRelaxedNode<'T>(ownerToken : OwnerToken, children : RRBNode<'T>[], sizeTable : int[], ?realSize : int) =
    inherit RRBRelaxedNode<'T>(ownerToken, Array.expandToBlockSize children, Array.expandToBlockSize sizeTable)

    member val CurrentLength : int = defaultArg realSize (Array.length children) with get, set
    override this.NodeSize = this.CurrentLength
    override this.SlotCount = this.Children |> Seq.take this.NodeSize |> Seq.sumBy (fun child -> child.NodeSize)

    member this.StringRepr : string = sprintf "length=%d, sizetable=%A%s, children=%A%s"
                                              this.NodeSize
                                              (this.SizeTable |> Array.truncate this.NodeSize)
                                              (if this.NodeSize >= Literals.blockSize then "" else sprintf " (plus %d nulls)" (Literals.blockSize - this.NodeSize))
                                              (this.Children |> Array.truncate this.NodeSize)
                                              (if this.NodeSize >= Literals.blockSize then "" else sprintf " (plus %d nulls)" (Literals.blockSize - this.NodeSize))

    override this.Shrink owner =
        if this.IsEditableBy owner then
            RRBRelaxedNode<'T>(owner, this.Children, this.SizeTable) :> RRBNode<'T>
        else
            let size = this.NodeSize
            let children' =
                if size = Literals.blockSize
                then Array.copy this.Children
                else this.Children |> Array.truncate size
            let sizeTable' =
                if size = Literals.blockSize
                then Array.copy this.SizeTable
                else this.SizeTable |> Array.truncate size
            RRBRelaxedNode<'T>(owner, children', sizeTable') :> RRBNode<'T>

    override this.Expand owner = this.GetEditableNode owner

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBExpandedRelaxedNode<'T>(owner, Array.copy this.Children, Array.copy this.SizeTable, this.NodeSize) :> RRBNode<'T>

    override this.SetNodeSize newSize = this.CurrentLength <- newSize

    override this.ToFullNodeIfNeeded shift =
        if RRBMath.isSizeTableFullAtShift shift sizeTable this.NodeSize
        then RRBExpandedFullNode<'T>(this.Owner, this.Children, this.NodeSize) :> RRBNode<'T>
        else this :> RRBNode<'T>

    // ===== NODE MANIPULATION =====
    // These low-level functions only create new nodes (or modify an expanded node), without doing any bounds checking.

    override this.AppendChild owner shift newChild =
        let newChildSize = newChild.TreeSize (down shift)
        this.AppendChildS owner shift newChild newChildSize

    override this.AppendChildS owner shift newChild newChildSize =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let oldSize = node'.NodeSize
        if oldSize > 0 then
            // Expanded nodes always have their rightmost child, and only that child, expanded
            let lastChild = node'.LastChild
            let shrunkLastChild = lastChild.Shrink owner
            if not (isSameObj lastChild shrunkLastChild) then
                node'.Children.[oldSize - 1] <- shrunkLastChild
        node'.Children.[oldSize] <- newChild.Expand owner
        node'.SizeTable.[oldSize] <- node'.SizeTable.[oldSize - 1] + newChildSize
        node'.SetNodeSize (oldSize + 1)
        node' :> RRBNode<'T>

    override this.InsertChild owner shift localIdx newChild =
        this.InsertChildS owner shift localIdx newChild (newChild.TreeSize (down shift))

    override this.InsertChildS owner shift localIdx newChild newChildSize =
        if localIdx = this.NodeSize then this.AppendChildS owner shift newChild newChildSize else
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let oldSize = node'.NodeSize
        for i = oldSize - 1 downto localIdx do
            node'.Children.[i+1] <- node'.Children.[i]
            node'.SizeTable.[i+1] <- node'.SizeTable.[i] + newChildSize
        node'.Children.[localIdx] <- newChild
        node'.SizeTable.[localIdx] <- newChildSize + (if localIdx <= 0 then 0 else node'.SizeTable.[localIdx-1])
        node'.SetNodeSize (oldSize + 1)
        node'.ToFullNodeIfNeeded shift

    override this.RemoveChild owner shift localIdx =
        if localIdx = this.NodeSize - 1 then this.RemoveLastChild owner shift else
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
        if newSize > 0 then
            node'.ToFullNodeIfNeeded shift
        else
            node' :> RRBNode<'T>

    override this.RemoveLastChild owner shift =
        // TODO: First make sure that "owner" isn't null at this point, because that's causing a failure in one of my tests
        // [push 1063; rev(); pop 118] --> after the rev(), we're a transient node that has become persistent so our owner is now null -- but we failed to check that.
        // ... I think that's been fixed, but let's make sure we test this scenario.
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let newSize = node'.NodeSize - 1
        node'.Children.[newSize] <- null
        node'.SizeTable.[newSize] <- 0
        node'.SetNodeSize newSize
        if newSize > 0 then
            let lastChild = node'.LastChild
            let expandedLastChild = lastChild.Expand owner
            if not (isSameObj lastChild expandedLastChild) then
                node'.Children.[newSize - 1] <- expandedLastChild
            node'.ToFullNodeIfNeeded shift
        else
            node' :> RRBNode<'T>

    // No need to override UpdateChildSRel and UpdateChildSAbs; versions from compact RRBRelaxedNode will work just fine in expanded nodes

    override this.KeepNLeft owner shift n =
        if n = this.NodeSize then this :> RRBNode<'T> else
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        for i = n to node'.NodeSize - 1 do
            node'.Children.[i] <- null
            node'.SizeTable.[i] <- 0
        node'.SetNodeSize n
        if n > 0 then
            let lastChild = node'.LastChild
            let expandedLastChild = lastChild.Expand owner
            if not (isSameObj lastChild expandedLastChild) then
                node'.Children.[n - 1] <- expandedLastChild
            node'.ToFullNodeIfNeeded shift
        else
            node' :> RRBNode<'T>

    override this.KeepNRight owner shift n =
        let skip = this.NodeSize - n
        if skip = 0 then this :> RRBNode<'T> else
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
        let rLen = r.Length
        if rLen > 0 then
            let lastChild = r.[rLen - 1]
            let shrunkLastChild = lastChild.Shrink owner
            if not (isSameObj lastChild shrunkLastChild) then
                r.[rLen - 1] <- shrunkLastChild
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
        let node', r = this.SplitAndKeepNLeft owner shift n
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

    override this.AppendNChildren owner shift n newChildren shouldExpandRightChildIfNeeded =
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        // Expanded nodes always have their rightmost child, and only that child, expanded
        let lastChild = node'.LastChild
        let shrunkLastChild = lastChild.Shrink owner
        if not (isSameObj lastChild shrunkLastChild) then
            node'.Children.[size - 1] <- shrunkLastChild
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
        if shouldExpandRightChildIfNeeded then
            let newLastChild = node'.LastChild
            let expandedNewLastChild = newLastChild.Expand owner
            if not (isSameObj newLastChild expandedNewLastChild) then
                node'.Children.[newSize - 1] <- expandedNewLastChild
        node' :> RRBNode<'T>

    override this.AppendNChildrenS owner shift n newChildren sizes shouldExpandRightChildIfNeeded =
        // Note: "sizes" should be a sequence of size table entries, i.e. cumulative: instead of [3;4;3;2;4] it should be [3;7;10;12;16]
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let size = node'.NodeSize
        // Expanded nodes always have their rightmost child, and only that child, expanded
        let lastChild = node'.LastChild
        let shrunkLastChild = lastChild.Shrink owner
        if not (isSameObj lastChild shrunkLastChild) then
            node'.Children.[size - 1] <- shrunkLastChild
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
        if shouldExpandRightChildIfNeeded then
            let newLastChild = node'.LastChild
            let expandedNewLastChild = newLastChild.Expand owner
            if not (isSameObj newLastChild expandedNewLastChild) then
                node'.Children.[newSize - 1] <- expandedNewLastChild
        node' :> RRBNode<'T>

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
        node' :> RRBNode<'T>

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
        node' :> RRBNode<'T>

    override this.ExpandLastChildIfNeeded owner =
        if this.NodeSize = 0 then this :> RRBNode<'T> else
        let node' = this.GetEditableNode owner :?> RRBExpandedRelaxedNode<'T>
        let lastChild = this.LastChild
        let expandedLastChild = lastChild.Expand owner
        if not (isSameObj lastChild expandedLastChild) then
            node'.Children.[node'.NodeSize - 1] <- expandedLastChild
        node' :> RRBNode<'T>

    // ===== END of NODE MANIPULATION functions =====

    override this.MkArrayForRebalance owner shift length =
        if this.IsEditableBy owner then this.Children else Array.zeroCreate length

    override this.MkNodeForRebalance owner shift arr len =
        if isSameObj arr this.Children && this.IsEditableBy owner then
            for i = len to this.NodeSize - 1 do
                arr.[i] <- null
                this.SizeTable.[i] <- 0
            this.SetNodeSize len
            RRBNode<'T>.PopulateSizeTableS shift this.Children len this.SizeTable
            this.ToFullNodeIfNeeded shift
        else
            RRBNode<'T>.MkNode owner shift arr

    override this.MakeLeftNodeForSplit owner shift children sizes =
        (RRBNode<'T>.MkNodeKnownSize owner shift children sizes).Expand owner

    override this.NewParent owner shift siblings =
        let arr = Array.zeroCreate<RRBNode<'T>> Literals.blockSize
        let size = siblings |> Array.length
#if DEBUG
        if size <= 0 then failwith <| sprintf "Empty array passed to this.NewParent -- not allowed! owner=%A shift=%A this=%A" owner shift this
#endif
        for i = 0 to size - 2 do
            arr.[i] <- siblings.[i].Shrink owner
        arr.[size - 1] <- siblings.[size - 1].Expand owner
        // TODO: Examine the logic of this "if" statement and see if we can instead write a static RRBExpandedRelaxedNode.Create method that will take care of this
        if size = 1 || (this.NodeSize = Literals.blockSize && this.FullNodeIsTrulyFull shift) then
            RRBExpandedFullNode<'T>(owner, arr, size) :> RRBNode<'T>
        else
            let sizeTable = RRBNode<'T>.CreateSizeTableS (up shift) arr size
            RRBExpandedRelaxedNode<'T>(owner, arr, sizeTable, size) :> RRBNode<'T>



and [<StructuredFormatDisplay("{StringRepr}")>] RRBLeafNode<'T>(ownerToken : OwnerToken, items : 'T[]) =
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

    member this.StringRepr : string = sprintf "L%d" this.NodeSize

    override this.Shrink owner = this.GetEditableNode owner  // TODO: For efficiency, return "this" if owner is same as ours *even* if it's nullOwner
    override this.Expand owner = this.GetEditableNode owner

    override this.GetEditableNode owner =
        if this.IsEditableBy owner
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(owner, Array.copy items) :> RRBNode<'T>

    override this.GetEditableEmptyNodeOfLengthN owner len =
        if this.IsEditableBy owner && this.Items.Length = len
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(owner, Array.zeroCreate len) :> RRBNode<'T>

    // TODO: This one might not be needed anymore
    member this.LeafNodeWithItems owner (newItems : 'T []) =
        if this.NodeSize = newItems.Length then  // NOT this.NodeSize here
            let newNode = this.GetEditableNode owner :?> RRBLeafNode<'T>
            newItems.CopyTo(newNode.Items, 0)
            newNode :> RRBNode<'T>
        else
            RRBNode<'T>.MkLeaf owner newItems

    member this.UpdatedItem owner localIdx newItem =
        let node = this.GetEditableNode owner :?> RRBLeafNode<'T>
        node.Items.[localIdx] <- newItem
        node :> RRBNode<'T>

    override this.UpdatedTree owner shift treeIdx newItem =
        this.UpdatedItem owner treeIdx newItem

    member this.InsertedItem owner localIdx item =
        // Leaf nodes are never expanded and cannot insert in place so there's no point in checking the owner.
        let newItems = this.Items |> Array.copyAndInsertAt localIdx item
        RRBLeafNode<'T>(owner, newItems)

    member this.AppendedItem owner item =
        // Leaf nodes are never expanded and cannot insert in place so there's no point in checking the owner.
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
                let newLeft  = RRBNode<'T>.MkLeaf owner newLeftItems
                let newRight = this.LeafNodeWithItems owner newRightItems
                SlidItemsLeft (newLeft, newRight)
            | Some parent, idx when idx < (parent.NodeSize - 1) && parent.Children.[idx + 1].NodeSize < Literals.blockSize ->
                // Room in the right sibling
                let rightSib = parent.Children.[idx + 1] :?> RRBLeafNode<'T>
                let newLeftItems, newRightItems = Array.appendAndInsertAndSplitEvenly localIdx item this.Items rightSib.Items
                // Note that we DON'T use LeafNodeWithItems here: expanded leaves may only be the rightmost leaf in the tree,
                // so if this is an expanded note that would benefit from using LeafNodeWithItems, we should never reach this branch
                let newLeft  = RRBNode<'T>.MkLeaf owner newLeftItems
                let newRight = RRBNode<'T>.MkLeaf owner newRightItems
                SlidItemsRight (newLeft, newRight)
            | _ ->
                // Don't need to get fancy with SplitNode here, though expanded leaf nodes will want to do something slightly more clever (ensuring that the new right-hand node is still expanded)
                let newLeftItems, newRightItems = Array.insertAndSplitEvenly localIdx item this.Items
                let newLeft  = RRBNode<'T>.MkLeaf owner newLeftItems
                let newRight = this.LeafNodeWithItems owner newRightItems
                SplitNode (newLeft, newRight)

    member this.PopLastItem owner =
        let resultItem = this.Items |> Array.last
        let resultLeaf = this.Items |> Array.copyAndPop |> RRBNode<'T>.MkLeaf owner
        resultItem, resultLeaf

    override this.GetTreeItem _shift localIdx =
        this.Items.[localIdx]

    // TODO: Consider whether this.RemovedItem should mirror PopLastItem or not. There's not nearly as much demand for RemoveAndReturn from the middle of a list as there is to pop the last item.
    override this.RemovedItem owner shift shouldCheckForRebalancing localIdx =
        let resultItem = this.Items.[localIdx]
        let resultLeaf = Array.copyAndRemoveAt localIdx this.Items |> RRBNode<'T>.MkLeaf owner
        resultItem, resultLeaf

    override this.KeepNTreeItems owner shift treeIdx =
        this.Items |> Array.truncate treeIdx |> RRBNode<'T>.MkLeaf owner

    override this.SkipNTreeItems owner shift treeIdx =
        this.Items |> Array.skip treeIdx |> RRBNode<'T>.MkLeaf owner

    override this.SplitTree owner shift treeIdx =
        let l, r = this.Items |> Array.splitAt treeIdx
        (l |> RRBNode<'T>.MkLeaf owner), (r |> RRBNode<'T>.MkLeaf owner)

let emptyNode<'T> = RRBNode<'T>.MkFullNode nullOwner Array.empty
