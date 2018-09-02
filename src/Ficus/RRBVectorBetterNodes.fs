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

    /// Used in replacing leaf nodes
    let copyAndAddNToSizeTable incIdx n oldST =
        let newST = Array.copy oldST
        for i = incIdx to oldST.Length - 1 do
            newST.[i] <- newST.[i] + n
        newST

    let inline copyAndSubtractNFromSizeTable decIdx n oldST =
        copyAndAddNToSizeTable decIdx (-n) oldST

[<AbstractClass>]
type Node<'T>(thread : Thread ref) =  // TODO: Rename this to RRBNode once I won't be confused by it
    let thread = thread
    member this.Thread : Thread ref = thread
    member this.SetThread (t : Thread) : unit = thread := t

    abstract member Shrink : unit -> Node<'T>
    abstract member Expand : Thread ref -> Node<'T>

    // Example of how the node-construction code can work
    static member MkLeaf (items : 'T[]) = items
    static member MkNode (children : Node<'T>[]) =
        if children.Length = Literals.blockSize then
            RRBFullNode<'T>.Create(children)
        else
            RRBRelaxedNode<'T>.Create(children)

and RRBFullNode<'T>(thread : Thread ref) =
    inherit Node<'T>(thread)
    static member Create(children : Node<'T>[]) = 5

    override this.Shrink() = this :> Node<'T>
    override this.Expand _ = this :> Node<'T>

and RRBRelaxedNode<'T>(thread : Thread ref) =
    inherit Node<'T>(thread)
    static member Create(children : Node<'T>[]) = 42

    override this.Shrink() = this :> Node<'T>
    override this.Expand _ = this :> Node<'T>
