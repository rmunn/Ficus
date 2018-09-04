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

[<AbstractClass>]
type RRBNode<'T>(thread : Thread ref) =
    let thread = thread
    member this.Thread : Thread ref = thread
    member this.SetThread (t : Thread) : unit = thread := t

    abstract member Shrink : unit -> RRBNode<'T>
    abstract member Expand : Thread ref -> RRBNode<'T>

    abstract member NodeSize : int          // How many children does this single node have?
    abstract member TreeSize : int -> int   // How many total items are found in this node's entire descendant tree?

    abstract member GetEditableNode : Thread ref -> RRBNode<'T>

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
        RRBRelaxedNode<'T>.CreateWithSizeTable(thread, shift, children, sizeTable) :> RRBNode<'T>
    static member MkFullNode (thread : Thread ref) (children : RRBNode<'T>[]) =
        // if children.Length = 1 then SingletonNode<'T>(thread, entries) :> Node<'T> else  // TODO: Do we want an RRBSingletonNode class as well?
        RRBFullNode<'T>.Create(thread, children)

    abstract member UpdatedTree : Thread ref -> int -> int -> 'T -> RRBNode<'T>

and RRBFullNode<'T>(thread : Thread ref, children : RRBNode<'T>[]) =
    inherit RRBNode<'T>(thread)
    static member Create (thread : Thread ref, children : RRBNode<'T>[]) = RRBFullNode<'T>(thread, children) :> RRBNode<'T>

    member this.Children = children

    override this.NodeSize = children.Length
    override this.TreeSize shift =
        // A full node is allowed to have an incomplete rightmost entry, but all but its rightmost entry must be complete.
        // Therefore, we can shortcut this calculation for most of the nodes, but we do need to calculate the rightmost node.
        ((this.NodeSize - 1) <<< shift) + (children.[this.NodeSize - 1]).TreeSize (down shift)

    // TODO: Should GetEditableNode return the base class? Or should it return the derived class, and therefore be non-inherited?
    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBFullNode<'T>(mutator, Array.copy children) :> RRBNode<'T>

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
        node :> RRBNode<'T>

    override this.UpdatedTree mutator shift treeIdx newItem =
        // TODO: Implement
        let localIdx, child, nextIdx = this.IndexesAndChild shift treeIdx
        let newNode = child.UpdatedTree mutator (down shift) nextIdx newItem
        this.UpdatedChildSameSize mutator localIdx newNode

and RRBRelaxedNode<'T>(thread : Thread ref, children : RRBNode<'T>[], sizeTable : int[]) =
    inherit RRBFullNode<'T>(thread, children)

    static member Create (thread : Thread ref, shift : int, children : RRBNode<'T>[]) =
        let sizeTbl = RRBNode<'T>.CreateSizeTable shift children
        RRBRelaxedNode<'T>.CreateWithSizeTable(thread, shift, children, sizeTbl)

    static member CreateWithSizeTable (thread : Thread ref, shift : int, children : RRBNode<'T>[], sizeTbl : int[]) =
        if isSizeTableFullAtShift shift sizeTbl then
            RRBFullNode<'T>(thread, children) :> RRBNode<'T>
        else
            RRBRelaxedNode<'T>(thread, children, sizeTbl) :> RRBNode<'T>

    member this.SizeTable = sizeTable

    override this.NodeSize = children.Length
    override this.TreeSize _ = sizeTable.[this.NodeSize - 1]

    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBRelaxedNode<'T>(mutator, Array.copy children, Array.copy sizeTable) :> RRBNode<'T>

and RRBLeafNode<'T>(thread : Thread ref, items : 'T[]) =
    inherit RRBNode<'T>(thread)

    member this.Items = items

    override this.NodeSize = items.Length
    override this.TreeSize _ = items.Length

    override this.Shrink() = this :> RRBNode<'T>
    override this.Expand _ = this :> RRBNode<'T>

    override this.GetEditableNode mutator =
        if this.IsEditableBy mutator
        then this :> RRBNode<'T>
        else RRBLeafNode<'T>(mutator, Array.copy items) :> RRBNode<'T>

    member this.UpdatedItem mutator localIdx newItem =
        let node = this.GetEditableNode mutator :?> RRBLeafNode<'T>
        node.Items.[localIdx] <- newItem
        node :> RRBNode<'T>

    override this.UpdatedTree mutator shift treeIdx newItem =
        this.UpdatedItem mutator treeIdx newItem
