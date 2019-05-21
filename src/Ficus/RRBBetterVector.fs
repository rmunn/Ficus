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
open RRBVectorBetterNodes

[<AbstractClass>]
[<StructuredFormatDisplay("{StringRepr}")>]
type RRBVector<'T>() =
    abstract member Empty : unit -> RRBVector<'T>
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
    // abstract member Transient : unit -> TransientRRBTree<'T>

// type RRBSapling<'T> internal (count, shift : int, root : 'T [], tail : 'T [], tailOffset : int)  =
//     inherit RRBVector<'T>()
//     // A "sapling" is a short tree, with shift = 0 (e.g., no more than two leaves, one in the root and one in the tail)
//     let hashCode = ref None
//     let thread = ref null  // TOCONVERT: When transients are implemented, this needs to change in the transient. Maybe there's a GetThread() method/field in all vectors.
//     static member EmptyTree : RRBSapling<'T> = RRBSapling<'T>(0,0,Array.empty,Array.empty,0)
//     static member Singleton (item : 'T) : RRBSapling<'T> = RRBSapling<'T>(1,0,Array.empty,Array.singleton item,0)

//     static member internal OfTail (tail : 'T[]) = RRBSapling<'T>(tail.Length, 0, Array.empty, tail, 0)
//     static member internal OfRootAndTail (root : 'T[]) (tail : 'T[]) = RRBSapling<'T>(root.Length + tail.Length, 0, root, tail, root.Length)

//     override this.GetHashCode() =
//         // This MUST follow the same algorithm as the GetHashCode() method from PersistentVector so that our shortcut in the Equals() logic will be valid
//         match !hashCode with
//         | None ->
//             let mutable hash = 1
//             for x in this.IterItems() do
//                 hash <- 31 * hash + Unchecked.hash x
//             hashCode := Some hash
//             hash
//         | Some hash -> hash

//     override this.Equals(other) =
//         match other with
//         | :? RRBSapling<'T> as other ->
//             if this.Length <> other.Length then false else
//             if this.GetHashCode() <> other.GetHashCode() then false else
//             Seq.forall2 (Unchecked.equals) (this.IterItems()) (other.IterItems())
//         // | :? PersistentVector<'T> as other ->  // TODO: Once this is merged into FSharpx.Collections, uncomment this block so we can get efficient comparison
//         //     if this.Length <> other.Length then false else
//         //     if this.GetHashCode() <> other.GetHashCode() then false else
//         //     Seq.forall2 (Unchecked.equals) (this.IterItems()) (other.IterItems())
//         | _ -> false

//     override this.ToString() =
//         sprintf "RRBSapling<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" count shift tailOffset root tail

//     override this.StringRepr = this.ToString()

//     member internal this.Shift = shift
//     member internal this.Root = root
//     member internal this.Tail = tail
//     member internal this.TailOffset = tailOffset

//     override this.Empty = RRBSapling<'T>(0,0,Array.empty,Array.empty,0) :> RRBVector<'T>
//     override this.IsEmpty() = count = 0
//     override this.Length = count
//     override this.IterLeaves() = if root.Length = 0 then Seq.singleton tail else seq { yield root; yield tail }
//     override this.RevIterLeaves() = if root.Length = 0 then Seq.singleton tail else seq { yield tail; yield root }
//     override this.IterItems() = seq { yield! root; yield! tail }
//     override this.RevIterItems() = seq {
//         for i = tail.Length - 1 downto 0 do
//             yield tail.[i]
//         for i = root.Length - 1 downto 0 do
//             yield root.[i] }

//     override this.Push item =
//         let tailLen = count - tailOffset
//         if tailLen < Literals.blockSize then
//             // Easy: just add new item in tail and we're done
//             RRBSapling<'T>(count + 1, shift, root, tail |> Array.copyAndAppend item, tailOffset) :> RRBVector<'T>
//         elif root.Length < Literals.blockSize then
//             // While we're building a new root anyway, let's ensure that it's a full array
//             let root' = Array.zeroCreate Literals.blockSize
//             let rootLen = root.Length
//             let extraRootItems = Literals.blockSize - rootLen
//             Array.blit root 0 root' 0 rootLen
//             Array.blit tail 0 root' rootLen extraRootItems
//             let tailLenMinusExtra = tailLen - extraRootItems
//             let tail' = Array.zeroCreate (tailLenMinusExtra + 1)
//             Array.blit tail extraRootItems tail' 0 tailLenMinusExtra
//             tail'.[tailLenMinusExtra] <- item
//             RRBSapling<'T>(count + 1, shift, root', tail', Literals.blockSize) :> RRBVector<'T>
//         else
//             // Full root and full tail means we're upgrading to a full tree
//             let treeRoot = Node(thread, [|box root; box tail|])
//             RRBTree<'T>(count + 1, Literals.blockSizeShift, treeRoot, [|item|], Literals.blockSize * 2) :> RRBVector<'T>

//     override this.Peek() = if count = 0 then failwith "Can't peek an empty vector" else tail |> Array.last

//     override this.Pop() =
//         if count <= 0 then invalidOp "Can't pop from an empty vector" else
//         if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
//         if count - tailOffset > 1 then
//             RRBSapling<'T>(count - 1, shift, root, Array.copyAndPop tail, tailOffset) :> RRBVector<'T>
//         else
//             RRBSapling<'T>(count - 1, shift, Array.empty, root, 0) :> RRBVector<'T>

//     override this.Take takeCount =
//         let newLen = Operators.min count takeCount
//         if takeCount < tailOffset then
//             RRBSapling<'T>(newLen, 0, Array.empty, root |> Array.truncate newLen, 0) :> RRBVector<'T>
//         elif takeCount = tailOffset then
//             RRBSapling<'T>(newLen, 0, Array.empty, root, 0) :> RRBVector<'T>
//         else
//             RRBSapling<'T>(newLen, 0, root, tail |> Array.truncate (newLen - tailOffset), tailOffset) :> RRBVector<'T>

//     override this.Skip skipCount =
//         if skipCount < tailOffset then
//             // TODO: Should we rebalance the root here so that it's a full blockSize items? Answer: No, that invariant isn't needed for saplings
//             RRBSapling<'T>(count - skipCount, 0, root |> Array.skip skipCount, tail, tailOffset - skipCount) :> RRBVector<'T>
//         elif skipCount = tailOffset then
//             RRBSapling<'T>(count - skipCount, 0, Array.empty, tail, 0) :> RRBVector<'T>
//         else
//             RRBSapling<'T>(count - skipCount, 0, Array.empty, tail |> Array.skip (skipCount - tailOffset), 0) :> RRBVector<'T>

//     override this.Split splitIdx = this.Take splitIdx, this.Skip splitIdx

//     override this.Slice (fromIdx, toIdx) =
//         // TODO: Turn this into a single operation, which should be a little bit more efficient
//         (this.Skip fromIdx).Take (toIdx - fromIdx + 1)

//     override this.GetSlice (fromIdx, toIdx) =
//         // TODO: Turn this into a single operation, which should be a little bit more efficient
//         let step1 = match fromIdx with
//                     | None -> this :> RRBVector<'T>
//                     | Some idx -> this.Skip idx
//         let step2 = match toIdx with
//                     | None -> step1
//                     | Some idx -> step1.Take (idx - (defaultArg fromIdx 0) + 1)
//         step2

//     override this.Append otherVec =
//         if count = 0 then otherVec
//         elif otherVec.Length = 0 then this :> RRBVector<'T>
//         else
//             // This will be complicated. TOCHECHECK: Test thoroughly since I'm not certain I got it all
//             match otherVec with
//             | :? RRBSapling<'T> as b ->
//                 if b.Root.Length = 0 then
//                     if tail.Length + b.Tail.Length <= Literals.blockSize then
//                         RRBSapling<'T>(count + b.Length, shift, root, Array.append tail b.Tail, tailOffset) :> RRBVector<'T>
//                     elif root.Length = 0 then
//                         // Two tail-only vectors can always turn into a single sapling
//                         let root', tail' = Array.appendAndSplitAt Literals.blockSize tail b.Tail
//                         RRBSapling<'T>(count + b.Length, shift, root', tail', Literals.blockSize) :> RRBVector<'T>
//                     else
//                         RRBHelpers.buildTreeFromTwoSaplings thread root tail b.Root b.Tail
//                 else
//                     RRBHelpers.buildTreeFromTwoSaplings thread root tail b.Root b.Tail
//             | :? RRBTree<'T> as b ->
//                 // Left side sapling, right side full tree. Convert left side to a "full" tree before appending
//                 let treeRoot = if root.Length = 0 then NodeCreation.mkNode thread [||] else NodeCreation.mkRRBNode<'T> thread Literals.blockSizeShift [|box root|]
//                 // Occasionally, we can end up with a vector that needs adjusting (e.g., because the tail that was pushed down was short, so the invariant is no longer true).
//                 match RRBHelpers.mergeTree thread Literals.blockSizeShift treeRoot b.Shift b.Root tail with
//                 | [||], rootItems
//                 | rootItems, [||] -> RRBTree<'T>(count + b.Length, b.Shift, NodeCreation.mkRRBNode<'T> thread b.Shift rootItems, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>
//                 | rootItemsL, rootItemsR ->
//                     let rootL = NodeCreation.mkRRBNode<'T> thread b.Shift rootItemsL
//                     let rootR = NodeCreation.mkRRBNode<'T> thread b.Shift rootItemsR
//                     let newShift = RRBMath.up b.Shift
//                     let newRoot = NodeCreation.mkRRBNode<'T> thread newShift [|box rootL; box rootR|]
//                     RRBTree<'T>(count + b.Length, newShift, newRoot, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>

//     override this.Insert idx item =
//         let idx = if idx < 0 then idx + count else idx
//         if idx > count then
//             invalidArg "idx" "Tried to insert past the end of the vector"
//         elif idx = count then
//             this.Push item
//         elif idx >= tailOffset then
//             (this :> IRRBInternal<'T>).InsertIntoTail (idx - tailOffset) item
//         else
//             // We already know the root is not empty, otherwise we would have hit "idx >= tailOffset" just above
//             if root.Length < Literals.blockSize then
//                 let root' = root |> Array.copyAndInsertAt idx item
//                 RRBSapling<'T>(count + 1, 0, root', tail, tailOffset + 1) :> RRBVector<'T>
//             elif tail.Length < Literals.blockSize then
//                 let newRootItems = root |> Array.copyAndInsertAt idx item
//                 let root' = Array.sub newRootItems 0 Literals.blockSize
//                 let tail' = tail |> Array.copyAndInsertAt 0 (Array.last newRootItems)
//                 RRBSapling<'T>(count + 1, 0, root', tail', tailOffset) :> RRBVector<'T>
//             else
//                 // Full root and full tail at shift 0: handle this case specially for efficiency's sake
//                 let newItems = Array.appendAndInsertAt idx item root tail
//                 let a = newItems.[..Literals.blockSize-1]
//                 let b = newItems.[Literals.blockSize..Literals.blockSize*2-1]
//                 let tail' = newItems.[Literals.blockSize*2..]
//                 let root' = NodeCreation.mkNode thread [|box a; box b|]
//                 RRBTree<'T>(count + 1, Literals.blockSizeShift, root', tail', Literals.blockSize*2) :> RRBVector<'T>

//     override this.Remove idx = (this :> IRRBInternal<'T>).RemoveImpl true idx

//     override this.Update idx newItem =
//         let idx = if idx < 0 then idx + count else idx
//         if idx >= count then
//             invalidArg "idx" "Tried to update item past the end of the vector"
//         if idx >= tailOffset then
//             let newTail = tail |> Array.copyAndSet (idx - tailOffset) newItem
//             RRBSapling<'T>(count, shift, root, newTail, tailOffset) :> RRBVector<'T>
//         else
//             let newRoot = root |> Array.copyAndSet idx newItem
//             RRBSapling<'T>(count, shift, newRoot, tail, tailOffset) :> RRBVector<'T>

//     override this.GetItem idx =
//         if idx < tailOffset
//         then root.[idx]
//         else tail.[idx - tailOffset]

//     interface IRRBInternal<'T> with

//         member this.InsertIntoTail tailIdx item =
//             if count - tailOffset < Literals.blockSize then
//                 // Tail isn't full yet: easy
//                 RRBSapling<'T>(count + 1, shift, root, tail |> Array.copyAndInsertAt tailIdx item, tailOffset) :> RRBVector<'T>
//             elif count < Literals.blockSize * 2 then
//                 // Short root, so rebalance to make a full root so we don't have to do that again next time
//                 // TODO: Write an array extension function to do this in a single step
//                 let fatTail = tail |> Array.copyAndInsertAt tailIdx item
//                 let root', tail' = Array.appendAndSplitAt Literals.blockSize root fatTail
//                 RRBSapling<'T>(count + 1, shift, root', tail', Literals.blockSize) :> RRBVector<'T>
//             else
//                 // Full root and tail
//                 let fatTail = tail |> Array.copyAndInsertAt tailIdx item
//                 let newRoot = Node(thread, [|box root; fatTail.[..Literals.blockSize-1] |> box|])
//                 RRBTree<'T>(count + 1, Literals.blockSizeShift, newRoot, fatTail.[Literals.blockSize..], Literals.blockSize * 2) :> RRBVector<'T>

//         member this.RemoveFromTailAtTailIdx idx =
//             if count <= 0 then invalidOp "Can't remove from an empty vector" else
//             if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
//             if count - tailOffset > 1 then
//                 RRBSapling<'T>(count - 1, shift, root, Array.copyAndRemoveAt idx tail, tailOffset) :> RRBVector<'T>
//             else
//                 RRBSapling<'T>(count - 1, shift, Array.empty, root, 0) :> RRBVector<'T>

//         member this.RemoveImpl _ idx =
//             // RRBSaplings don't need the shouldCheckForRebalancing parameter
//             let idx = if idx < 0 then idx + count else idx
//             if idx >= count then
//                 invalidArg "idx" "Tried to remove past the end of the vector"
//             elif count = 0 then
//                 failwith "Can't remove from an empty vector"
//             elif idx >= tailOffset then
//                 (this :> IRRBInternal<'T>).RemoveFromTailAtTailIdx(idx - tailOffset)
//             else
//                 RRBSapling<'T>(count - 1, 0, root |> Array.copyAndRemoveAt idx, tail, tailOffset - 1) :> RRBVector<'T>

//         member this.RemoveWithoutRebalance idx = (this :> IRRBInternal<'T>).RemoveImpl false idx // Will be used in RRBVector.windowed implementation

//         member this.Transient() =
//             // TODO: See if this can be optimized for various cases like "tail is full", or "root is full" (as opposed to root not being full, which can happen in a split)
//             if count <= Literals.blockSize then
//                 let tail' = Array.append root tail
//                 let rootNode = ExpandedNode.InCurrentThread()
//                 TransientRRBTree<'T>(count, Literals.blockSizeShift, rootNode, tail', 0)
//             else
//                 let root', tail' = Array.appendAndSplitAt Literals.blockSize root tail
//                 let rootNode = ExpandedNode.InCurrentThread()
//                 rootNode.Array.[0] <- box root'
//                 rootNode.CurrentLength <- 1
//                 TransientRRBTree<'T>(count, Literals.blockSizeShift, rootNode, tail', Literals.blockSize)

//     // override this.Update : int -> 'T -> RRBVector<'T>
//     // override this.Item : int -> 'T

// and [<StructuredFormatDisplay("{StringRepr}")>] RRBTree<'T> internal (count, shift : int, root : Node, tail : 'T [], tailOffset : int)  =
//     inherit RRBVector<'T>()
//     let hashCode = ref None
//     let thread = ref null  // TOCONVERT: When transients are implemented, this needs to change in the transient. Maybe there's a GetThread() method/field in all vectors.
//     // static member Empty() : RRBTree<'T> = RRBTree<'T>(0,0,RRBHelpers.emptyNode,Array.empty,0)
//     override this.Empty = RRBSapling<'T>(0,0,Array.empty,Array.empty,0) :> RRBVector<'T>
//     override this.IsEmpty() = count = 0

//     new (vecLen,shift:int,root:Node,tail:'T[]) =
//         let tailLen = Array.length tail
//         RRBTree<'T>(vecLen, shift, root, tail, vecLen - tailLen)

//     override this.GetHashCode() =
//         // This MUST follow the same algorithm as the GetHashCode() method from PersistentVector so that we can have a shortcut in the Equals() logic will be valid
//         match !hashCode with
//         | None ->
//             let mutable hash = 1
//             for x in this do
//                 hash <- 31 * hash + Unchecked.hash x
//             hashCode := Some hash
//             hash
//         | Some hash -> hash

//     override this.Equals(other) =
//         match other with
//         | :? RRBTree<'T> as other ->
//             if this.Length <> other.Length then false else
//             if this.GetHashCode() <> other.GetHashCode() then false else
//             Seq.forall2 (Unchecked.equals) this other
//         // | :? PersistentVector<'T> as other ->
//         //     if this.Length <> other.Length then false else
//         //     if this.GetHashCode() <> other.GetHashCode() then false else
//         //     Seq.forall2 (Unchecked.equals) this other
//         | _ -> false

//     override this.ToString() =
//         sprintf "RRBVector<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" count shift tailOffset root tail

//     override this.StringRepr = this.ToString()
//     member internal this.Shift = shift
//     member internal this.Root = root
//     member internal this.Tail = tail
//     member internal this.TailOffset = tailOffset

//     interface System.Collections.Generic.IEnumerable<'T> with
//         member this.GetEnumerator() = this.IterItems().GetEnumerator()

//     interface System.Collections.IEnumerable with
//         member this.GetEnumerator() = this.IterItems().GetEnumerator() :> System.Collections.IEnumerator

//     override this.Length = count

//     override this.IterLeaves() =
//         seq { yield! RRBHelpers.iterLeaves shift root
//               yield tail }

//     override this.RevIterLeaves() =
//         seq { yield tail
//               yield! RRBHelpers.revIterLeaves shift root }

//     override this.IterItems() = seq { for arr in this.IterLeaves() do yield! arr }

//     override this.RevIterItems() =
//         seq {
//             for arr in this.RevIterLeaves() do
//                 for i = (arr.Length - 1) downto 0 do
//                     yield arr.[i]
//         }

//     override this.Push item =
//         if count - tailOffset < Literals.blockSize then
//             // Easy: just add new item in tail and we're done
//             RRBTree<'T>(count + 1, shift, root, tail |> Array.copyAndAppend item) :> RRBVector<'T>
//         else
//             let root', shift' = root.PushTailDown thread shift tail  // This does all the work
//             RRBTree<'T>(count + 1, shift', root', [|item|]) :> RRBVector<'T>

//     override this.Peek() =
//         if count = 0
//         then failwith "Can't peek an empty vector"
//         else tail |> Array.last

//     override this.Pop() =
//         if count <= 0 then invalidOp "Can't pop from an empty vector" else
//         if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
//         if count - tailOffset > 1 then
//             RRBTree<'T>(count - 1, shift, root, Array.copyAndPop tail) :> RRBVector<'T>
//         else
//             RRBTree<'T>.promoteTail thread shift root (count - 1) :> RRBVector<'T>

//     override this.Take takeCount =
//         if takeCount >= count then this :> RRBVector<'T>
//         elif takeCount <= 0 then RRBSapling<'T>.EmptyTree :> RRBVector<'T>   // TODO: Decide whether a negative count in Take() should be an exception or not
//         elif takeCount > tailOffset then
//             let tailCount = takeCount - tailOffset
//             RRBTree<'T>(takeCount, shift, root, Array.sub tail 0 tailCount, tailOffset) :> RRBVector<'T>
//         elif takeCount = tailOffset then
//             // We don't have to slice into the tree at all: just promote new tail as if we'd just popped the last item from the old tail
//             RRBTree<'T>.promoteTail thread shift root takeCount :> RRBVector<'T>
//         else
//             let newRoot = RRBHelpers.leftSlice<'T> thread shift takeCount root
//             RRBTree<'T>.promoteTail thread shift newRoot takeCount :> RRBVector<'T>

//     override this.Skip skipCount =
//         if skipCount >= count then RRBSapling<'T>.EmptyTree :> RRBVector<'T>
//         elif skipCount <= 0 then this :> RRBVector<'T>   // TODO: Decide whether a negative count in Skip() should be an exception or not
//         elif skipCount > tailOffset then
//             let tailStart = skipCount - tailOffset
//             RRBSapling<'T>(count - skipCount, 0, Array.empty, tail.[tailStart..], 0) :> RRBVector<'T>
//         elif skipCount = tailOffset then
//             RRBSapling<'T>(count - skipCount, 0, Array.empty, tail, 0) :> RRBVector<'T>
//         else
//             let newRoot = RRBHelpers.rightSlice<'T> thread shift skipCount root
//             RRBTree<'T>(count - skipCount, shift, newRoot, tail, tailOffset - skipCount).AdjustTree() // :> RRBVector<'T>

//     override this.Split splitIdx =
//         this.Take splitIdx, this.Skip splitIdx

//     override this.Slice (fromIdx, toIdx) =
//         (this.Skip fromIdx).Take (toIdx - fromIdx + 1)

//     // Slice notation vec.[from..to] turns into a this.GetSlice call with option indices
//     override this.GetSlice (fromIdx, toIdx) =
//         let step1 = match fromIdx with
//                     | None -> this :> RRBVector<'T>
//                     | Some idx -> this.Skip idx
//         let step2 = match toIdx with
//                     | None -> step1
//                     | Some idx -> step1.Take (idx - (defaultArg fromIdx 0) + 1)
//         step2

//     override this.Append (otherVec : RRBVector<'T>) =  // Appends vector B at the end of this vector
//         if count = 0 then otherVec
//         elif otherVec.Length = 0 then this :> RRBVector<'T>
//         else
//             // This will be complicated. TOCHECK: Test thoroughly since I'm not certain I got it all
//             match otherVec with
//             | :? RRBSapling<'T> as b ->
//                 if b.Root.Length = 0 then
//                     if tail.Length + b.Tail.Length <= Literals.blockSize then
//                         RRBTree<'T>(count + b.Length, shift, root, Array.append tail b.Tail, tailOffset) :> RRBVector<'T>
//                     else
//                         let newLeaf, newTail = Array.appendAndSplitAt Literals.blockSize tail b.Tail
//                         let newRoot, newShift = root.PushTailDown thread shift newLeaf
//                         RRBTree<'T>(count + b.Length, newShift, newRoot, newTail) :> RRBVector<'T>
//                 else
//                     let lastTwig = RRBHelpers.getRightmostTwig shift root
//                     let tLen = tail.Length
//                     if b.Root.Length = Literals.blockSize && ((lastTwig :? RRBNode && lastTwig.NodeSize < Literals.blockSize) || tLen = Literals.blockSize) then
//                         // Can safely push tail without messing up the invariant
//                         let tempRoot, tempShift = root.PushTailDown thread shift tail
//                         let newRoot, newShift = tempRoot |> RRBHelpers.appendLeafWithGrowth thread tempShift b.Root
//                         RRBTree<'T>(count + b.Length, newShift, newRoot, b.Tail) :> RRBVector<'T>
//                     else
//                         // Have to shift some nodes, so there's no advantage in keeping the right tree's root and tail intact.
//                         let items = Array.append3 tail b.Root b.Tail
//                         let len = items.Length
//                         if len <= Literals.blockSize then
//                             RRBTree<'T>(count + b.Length, shift, root, items, tailOffset) :> RRBVector<'T>
//                         elif len <= Literals.blockSize * 2 then
//                             let newLeaf, newTail = items |> Array.splitAt Literals.blockSize
//                             let newRoot, newShift = root.PushTailDown thread shift newLeaf
//                             RRBTree<'T>(count + b.Length, newShift, newRoot, newTail) :> RRBVector<'T>
//                         else
//                             let newLeaf1, rest = items |> Array.splitAt Literals.blockSize
//                             let newLeaf2, newTail = rest |> Array.splitAt Literals.blockSize
//                             let tempRoot, tempShift = root.PushTailDown thread shift newLeaf1
//                             let newRoot, newShift = tempRoot.PushTailDown thread tempShift newLeaf2
//                             RRBTree<'T>(count + b.Length, newShift, newRoot, newTail) :> RRBVector<'T>
//             | :? RRBTree<'T> as b ->
//                 let aRoot, aShift = root, shift
//                 let aTwig = RRBHelpers.getRightmostTwig shift root
//                 let bTwig = RRBHelpers.getLeftmostTwig b.Shift b.Root
//                 let aRoot, aShift, aTail =
//                     if RRBHelpers.isThereRoomToMergeTheTail<'T> aTwig bTwig tail.Length then
//                         root, shift, tail
//                     else
//                         // Push a's tail down, then merge the resulting tree
//                         let thisRootAfterPush, shift' = root.PushTailDown thread shift tail
//                         thisRootAfterPush, shift', [||]
//                 let higherShift = max aShift b.Shift
//                 // Occasionally, we can end up with a vector that needs adjusting (e.g., because the tail that was pushed down was short, so the invariant is no longer true).
//                 match RRBHelpers.mergeTree thread aShift aRoot b.Shift b.Root aTail with
//                 | [||], rootItems
//                 | rootItems, [||] -> RRBTree<'T>(count + b.Length, higherShift, NodeCreation.mkRRBNode<'T> thread higherShift rootItems, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>
//                 | rootItemsL, rootItemsR ->
//                     let rootL = NodeCreation.mkRRBNode<'T> thread higherShift rootItemsL
//                     let rootR = NodeCreation.mkRRBNode<'T> thread higherShift rootItemsR
//                     let newShift = RRBMath.up higherShift
//                     let newRoot = NodeCreation.mkRRBNode<'T> thread newShift [|box rootL; box rootR|]
//                     RRBTree<'T>(count + b.Length, newShift, newRoot, b.Tail, count + b.TailOffset).AdjustTree() // :> RRBVector<'T>

//     override this.Insert idx (item : 'T) =
//         let idx = if idx < 0 then idx + count else idx
//         if idx > count then
//             invalidArg "idx" "Tried to insert past the end of the vector"
//         elif idx = count then
//             this.Push item
//         elif idx >= tailOffset then
//             (this :> IRRBInternal<'T>).InsertIntoTail (idx - tailOffset) item
//         else
//             match RRBHelpers.insertIntoTree thread shift idx item None 0 root with
//             | RRBHelpers.SlideResult.SimpleInsertion newRootItems ->
//                 RRBTree<'T>(count + 1, shift, NodeCreation.mkRRBNode<'T> thread shift newRootItems, tail, tailOffset + 1).AdjustTree() // :> RRBVector<'T>
//             | RRBHelpers.SlideResult.SlidItemsLeft (l,r)
//             | RRBHelpers.SlideResult.SlidItemsRight (l,r)
//             | RRBHelpers.SlideResult.SplitNode (l,r) ->
//                 let a = NodeCreation.mkRRBNode<'T> thread shift l
//                 let b = NodeCreation.mkRRBNode<'T> thread shift r
//                 RRBTree<'T>(count + 1, (RRBMath.up shift), NodeCreation.mkRRBNode<'T> thread (RRBMath.up shift) [|a;b|], tail, tailOffset + 1).AdjustTree() // :> RRBVector<'T>

//     member internal this.AdjustTree() =
//         let shorter : RRBVector<'T> = this.ShortenTree()
//         match shorter with
//         | :? RRBTree<'T> as tree -> tree.ShiftNodesFromTailIfNeeded()
//         | _ -> shorter // No need to shift nodes on a sapling

//     member internal this.ShortenTree() =
//         // TODO: Convert to sapling if shift hits 0
//         if shift <= 0 then this :> RRBVector<'T>
//         else
//             if root.NodeSize > 1 then this :> RRBVector<'T>
//             elif root.NodeSize = 0 then RRBSapling<'T>(count, 0, Array.empty, tail, tailOffset) :> RRBVector<'T>
//             elif shift <= Literals.blockSizeShift then
//                 RRBSapling<'T>(count, 0, (root.Array.[0] :?> 'T[]), tail, tailOffset) :> RRBVector<'T>
//             else
//                 RRBTree<'T>(count, RRBMath.down shift, (root |> RRBHelpers.getChildNode 0), tail, tailOffset).ShortenTree()

//     member internal this.ShiftNodesFromTailIfNeeded() =
//         // Minor optimization: save most-often-needed vector properties in locals so we don't hit property accesses over and over
//         let shift = this.Shift
//         let root : Node = this.Root
//         if shift < Literals.blockSizeShift then
//             // Can't happen now that we've split RRBTrees and RRBSaplings
//             this :> RRBVector<'T>
//             // let len = root.NodeSize
//             // if len = 0 || len = Literals.blockSize then this else
//             // let items = Array.append root.Array this.Tail
//             // if items.Length <= Literals.blockSize then
//             //     RRBTree<'T>(this.Length, 0, emptyNode, items, 0)
//             // else
//             //     let root', tail' = items |> Array.splitAt Literals.blockSize
//             //     RRBTree<'T>(this.Length, 0, mkNode root', tail', Literals.blockSize)
//         else
//             let lastTwig = RRBHelpers.getRightmostTwig shift root
//             match lastTwig with
//             | :? RRBNode -> this :> RRBVector<'T>  // Only need to shift nodes from tail if the last twig was a full node
//             | _ ->
//                 // TODO: Implement this on the nodes themselves
//                 let tail = this.Tail
//                 let lastLeaf = (lastTwig.Array.[lastTwig.NodeSize - 1]) :?> 'T []
//                 let shiftCount = Literals.blockSize - lastLeaf.Length
//                 if shiftCount = 0 then
//                     this :> RRBVector<'T>
//                 elif shiftCount >= tail.Length then
//                     // Would shift everything out of the tail, so instead we'll promote a new tail
//                     let lastLeaf, root' = root.RemoveLastLeaf<'T> thread shift
//                     let tail' = tail |> Array.append lastLeaf
//                     // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
//                     RRBTree<'T>(this.Length, shift, root', tail').AdjustTree()
//                 else
//                     let nodesToShift, tail' = tail |> Array.splitAt shiftCount
//                     let lastLeaf' = Array.append lastLeaf nodesToShift
//                     let root' = root |> RRBHelpers.replaceLastLeaf thread shift lastLeaf'
//                     RRBTree<'T>(this.Length, shift, root', tail') :> RRBVector<'T>

//     static member internal promoteTail thread shift root newLength =
//         let newTail, newRoot = root.RemoveLastLeaf<'T> thread shift
//         RRBTree<'T>(newLength, shift, newRoot, newTail).AdjustTree()

//     interface IRRBInternal<'T> with

//         member this.InsertIntoTail tailIdx (item : 'T) =
//             if count - tailOffset < Literals.blockSize then
//                 // Easy: just add new item in tail and we're done
//                 RRBTree<'T>(count + 1, shift, root, tail |> Array.copyAndInsertAt tailIdx item) :> RRBVector<'T>
//             else
//                 let tailItemsToPush, remaining =
//                     tail
//                     |> Array.copyAndInsertAt tailIdx item
//                     |> Array.splitAt Literals.blockSize
//                 let root', shift' = root.PushTailDown thread shift tailItemsToPush  // This does all the work
//                 RRBTree<'T>(count + 1, shift', root', remaining) :> RRBVector<'T>

//         member this.RemoveFromTailAtTailIdx idx =
//             if count <= 0 then invalidOp "Can't remove from an empty vector" else
//             if count = 1 then RRBSapling<'T>.EmptyTree :> RRBVector<'T> else
//             if count - tailOffset > 1 then
//                 RRBTree<'T>(count - 1, shift, root, Array.copyAndRemoveAt idx tail) :> RRBVector<'T>
//             else
//                 RRBTree<'T>.promoteTail thread shift root (count - 1) // :> RRBVector<'T>

//         member this.RemoveImpl shouldCheckForRebalancing idx =
//             let idx = if idx < 0 then idx + count else idx
//             if idx >= count then
//                 invalidArg "idx" "Tried to remove past the end of the vector"
//             elif count = 0 then
//                 failwith "Can't remove from an empty vector"
//             elif idx >= tailOffset then
//                 (this :> IRRBInternal<'T>).RemoveFromTailAtTailIdx(idx - tailOffset)
//             elif shift = 0 then
//                 failwith "Deliberate failure at shift 0 to prove this is no longer possible"
//                 let newRoot = root.Array |> Array.copyAndRemoveAt idx |> NodeCreation.mkNode thread
//                 RRBTree<'T>(count - 1, 0, newRoot, tail, tailOffset - 1).ShortenTree() // :> RRBVector<'T>  // No need for adjustTree at 0 shift
//             else
//                 let newRoot = RRBHelpers.removeFromTree<'T> thread shift shouldCheckForRebalancing idx root |> NodeCreation.mkRRBNode<'T> thread shift
//                 RRBTree<'T>(count - 1, shift, newRoot, tail, tailOffset - 1).AdjustTree() // :> RRBVector<'T>

//         member this.RemoveWithoutRebalance idx = (this :> IRRBInternal<'T>).RemoveImpl false idx // Will be used in RRBVector.windowed implementation

//         member this.Transient() =
//             let rec expandTree mutator shift (node : Node) =
//                 if shift > Literals.blockSizeShift then
//                     let lastIdx = node.NodeSize - 1
//                     let lastChild = node.Array.[lastIdx] :?> Node |> expandTree mutator (RRBMath.down shift)
//                     let newNode = node.Expand mutator
//                     newNode.Array.[lastIdx] <- box lastChild
//                     newNode
//                 else
//                     node.Expand mutator
//             let thread = ref Thread.CurrentThread
//             let root' = root |> expandTree thread shift
//             let tail' = Array.zeroCreate Literals.blockSizeShift
//             tail.CopyTo(tail', 0)
//             TransientRRBTree<'T>(count, shift, root', tail', tailOffset)

//     override this.Remove idx = (this :> IRRBInternal<'T>).RemoveImpl true idx

//     override this.Update idx newItem =
//         let idx = if idx < 0 then idx + count else idx
//         if idx >= count then
//             invalidArg "idx" "Tried to update item past the end of the vector"
//         if idx >= tailOffset then
//             let newTail = tail |> Array.copyAndSet (idx - tailOffset) newItem
//             RRBTree<'T>(count, shift, root, newTail, tailOffset) :> RRBVector<'T>
//         else
//             let newRoot = root.UpdatedTree thread shift idx newItem
//             RRBTree<'T>(count, shift, newRoot, tail, tailOffset) :> RRBVector<'T>

//     override this.GetItem idx =
//         if idx < tailOffset
//         then RRBHelpers.getItemFromLeaf<'T> shift idx root
//         else tail.[idx - tailOffset]

type RRBPersistentVector<'T> internal (count, shift : int, root : RRBNode<'T>, tail : 'T [], tailOffset : int) =
    inherit RRBVector<'T>()

    member this.Count = count
    member this.Shift = shift
    member this.Root = root
    member this.Tail = tail
    member this.TailOffset = tailOffset

    member this.Transient() =
        let newToken = mkOwnerToken()
        let root' = (this.Root :?> RRBFullNode<'T>).ExpandRightSpine newToken this.Shift
        let tailLen = this.Length - this.TailOffset
        let newTail = Array.sub this.Tail 0 tailLen
        RRBTransientVector<'T>(this.Count, this.Shift, root', newTail, this.TailOffset)

    // abstract member Empty : RRBVector<'T>  // Or maybe it should be unit -> RRBVector<'T>
    override this.Empty() = RRBPersistentVector<'T>(0, Literals.blockSizeShift, emptyNode, Array.empty, 0) :> RRBVector<'T>

    // abstract member IsEmpty : unit -> bool
    override this.IsEmpty() = this.Length = 0

    // abstract member StringRepr : string
    override this.StringRepr = this.ToString()

    // abstract member Length : int
    override this.Length = this.Count

    // abstract member IterLeaves : unit -> seq<'T []>
    override this.IterLeaves() = seq {
        yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
        yield this.Tail
    }

    // abstract member RevIterLeaves : unit -> seq<'T []>
    override this.RevIterLeaves() = seq {
        yield this.Tail
        yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
    }

    // abstract member IterItems : unit -> seq<'T>
    override this.IterItems() =
        this.IterLeaves() |> Seq.collect id

    // abstract member RevIterItems : unit -> seq<'T>
    override this.RevIterItems() = seq {
        for arr in this.RevIterLeaves() do
            for i = arr.Length - 1 downto 0 do
                yield arr.[i]
    }

    // // abstract member GetEnumerator : unit -> IEnumerator<'T>

    // abstract member Push : 'T -> RRBVector<'T>
    override this.Push newItem =
        let tailLen = this.Length - this.TailOffset
        if tailLen >= Literals.blockSize then
            let tailNode = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
            let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Root.Owner this.Shift tailNode
            RRBPersistentVector<'T>(this.Length + 1, newShift, newRoot, [|newItem|], this.Length) :> RRBVector<'T>
        else
            let newTail = this.Tail |> Array.copyAndAppend newItem
            RRBPersistentVector<'T>(this.Length + 1, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>

    // abstract member Peek : unit -> 'T
    override this.Peek() =
        if this.Length <= 0 then failwith "Can't get last item from an empty vector"
        else
            let tailLen = this.Length - this.TailOffset
#if DEBUG
            if tailLen = 0 then failwith "Tail should never be empty"
#endif
            this.Tail.[tailLen - 1]

    // abstract member Pop : unit -> RRBVector<'T>
    override this.Pop() =
        if this.Length <= 0 then failwith "Can't pop from an empty vector"
        else
            if this.Tail.Length > 1 then
                let newTail = this.Tail |> Array.copyAndPop
                RRBPersistentVector<'T>(this.Length - 1, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
            else
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Root.Owner this.Shift
            RRBPersistentVector<'T>(this.Length - 1, this.Shift, newRoot, newTailNode.Items, this.TailOffset - newTailNode.NodeSize) :> RRBVector<'T>  // FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty

    // abstract member Take : int -> RRBVector<'T>
    override this.Take idx =
        this.EnsureValidIndexLengthAllowed idx
        if idx = 0 then
            this.Empty()
        elif idx = this.Length then
            this :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Root.Owner this.Shift
            RRBPersistentVector<'T>(idx, this.Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize) :> RRBVector<'T>  // Shift can't change during this step, not yet. FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let newTail = this.Tail |> Array.truncate (idx - this.TailOffset)
            RRBPersistentVector<'T>(idx, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
        else
            let tmpRoot = this.Root.KeepNTreeItems this.Root.Owner this.Shift idx
            let newTailNode, newRoot = (tmpRoot :?> RRBFullNode<'T>).PopLastLeaf tmpRoot.Owner this.Shift
            // FIXME: Will have to adjust this tree in case the height can now be shortened, and/or in case new tail and last leaf need to be tweaked to maintain the invariant
            RRBPersistentVector<'T>(idx, this.Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize) :> RRBVector<'T>  // Shift can't change during this step, not yet. FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty

    // abstract member Skip : int -> RRBVector<'T>
    override this.Skip idx =
        this.EnsureValidIndexLengthAllowed idx
        if idx = 0 then
            this :> RRBVector<'T>
        elif idx = this.Length then
            this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we'll have an empty root
            RRBPersistentVector<'T>(this.Tail.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, this.Tail, 0) :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailR = this.Tail |> Array.skip (idx - this.TailOffset)
            RRBPersistentVector<'T>(tailR.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, tailR, 0) :> RRBVector<'T>
        else
            let newRoot = this.Root.SkipNTreeItems this.Root.Owner this.Shift idx
            // FIXME: Will have to adjust this tree in case the height can now be shortened, and/or in case new tail and last leaf need to be tweaked to maintain the invariant
            RRBPersistentVector<'T>(this.Length - idx, this.Shift, newRoot, this.Tail, this.TailOffset - idx) :> RRBVector<'T>

    // abstract member Split : int -> RRBVector<'T> * RRBVector<'T>
    override this.Split idx =
        this.EnsureValidIndexLengthAllowed idx
        if idx = 0 then
            this.Empty(), this :> RRBVector<'T>
        elif idx = this.Length then
            this :> RRBVector<'T>, this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Root.Owner this.Shift
            let newLeft = RRBPersistentVector<'T>(idx, this.Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize) :> RRBVector<'T>  // Shift can't change during this step, not yet. FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty
            let newRight = RRBPersistentVector<'T>(this.Tail.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, this.Tail, 0) :> RRBVector<'T>
            newLeft, newRight
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailL, tailR = this.Tail |> Array.splitAt (idx - this.TailOffset)
            let newLeft = RRBPersistentVector<'T>(idx, this.Shift, this.Root, tailL, this.TailOffset) :> RRBVector<'T>
            let newRight = RRBPersistentVector<'T>(tailR.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, tailR, 0) :> RRBVector<'T>
            newLeft, newRight
        else
            let rootL, rootR = this.Root.SplitTree this.Root.Owner this.Shift idx
            // TOCHECK: Uncomment the below lines and see if they're also correct
            // let rootSizeL = rootL.TreeSize this.Shift
            // let rootSizeR = rootR.TreeSize this.Shift
            // let newTailNodeL, newRootL = (rootL :?> RRBFullNode<'T>).PopLastLeaf rootL.Owner this.Shift
            // let newLeft = RRBPersistentVector<'T>(rootSizeL, this.Shift, newRootL, newTailNodeL.Items, this.TailOffset) :> RRBVector<'T>
            // let newRight = RRBPersistentVector<'T>(rootSizeR + this.Tail.Length, this.Shift, rootR, this.Tail, rootSizeR) :> RRBVector<'T>
            let newTailNodeL, newRootL = (rootL :?> RRBFullNode<'T>).PopLastLeaf rootL.Owner this.Shift
            let newLeft = RRBPersistentVector<'T>(idx, this.Shift, newRootL, newTailNodeL.Items, idx - newTailNodeL.NodeSize) :> RRBVector<'T>
            let newRight = RRBPersistentVector<'T>(this.Length - idx, this.Shift, rootR, this.Tail, this.Length - idx - this.Tail.Length) :> RRBVector<'T>
            // FIXME: Going to have to adjust the tree for both newLeft AND newRight in this one, since either one could have become a tall, thin tree
            newLeft, newRight

    // abstract member Slice : int * int -> RRBVector<'T>
    override this.Slice (start, stop) =
        (this.Skip start).Take stop

    // abstract member GetSlice : int option * int option -> RRBVector<'T>
    override this.GetSlice (start, stop) =
        match start, stop with
        | None, None -> this :> RRBVector<'T>
        | None, Some stop -> this.Take stop
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice (start, stop)

    // abstract member Append : RRBVector<'T> -> RRBVector<'T>
    override this.Append other =
        match other with
        | :? RRBPersistentVector<'T> as right ->
            let newLen = this.Length + right.Length
            if right.TailOffset <= 0 then
                // Right is a tail-only vector
                let tailLenL = this.Length - this.TailOffset
                let tailLenR = right.Length - right.TailOffset
                if tailLenL + tailLenR <= Literals.blockSize then
                    let newTail = Array.append this.Tail right.Tail
                    RRBPersistentVector<'T>(newLen, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
                else
                    let newLeafItems, newTail = Array.appendAndSplitAt Literals.blockSize this.Tail right.Tail
                    let newLeaf = RRBNode<'T>.MkLeaf this.Root.Owner newLeafItems :?> RRBLeafNode<'T>
                    if this.TailOffset <= 0 then
                        // Can't use AppendLeaf in an empty root, so we create the first twig by hand
                        // FIXME: Yes, we can actually. So this if-else isn't actually necessary! I think. TODO: Test this.
                        let newRoot = RRBNode<'T>.MkFullNode this.Root.Owner [|newLeaf|]
                        RRBPersistentVector<'T>(newLen, Literals.blockSizeShift, newRoot, newTail, Literals.blockSize) :> RRBVector<'T>
                    else
                        let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Root.Owner this.Shift newLeaf
                        RRBPersistentVector<'T>(newLen, newShift, newRoot, newTail, this.TailOffset + Literals.blockSize) :> RRBVector<'T>
            elif this.TailOffset <= 0 then
                // Right has a root and a tail, but we're a tail-only node
                let tailNode = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
                let newRoot, newShift = (right.Root :?> RRBFullNode<'T>).PrependLeaf right.Root.Owner right.Shift tailNode
                RRBPersistentVector<'T>(newLen, newShift, newRoot, right.Tail, right.TailOffset + tailNode.NodeSize) :> RRBVector<'T>
            else
                // Right has a root and a tail, and so do we
                let tailNode = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
                match (this.Root :?> RRBFullNode<'T>).MergeTree this.Root.Owner this.Shift (Some tailNode) right.Shift (right.Root :?> RRBFullNode<'T>) false with
                | newRoot, None ->
                    RRBPersistentVector<'T>(newLen, max this.Shift right.Shift, newRoot, right.Tail, this.Length + right.TailOffset) :> RRBVector<'T>
                | newLeft, Some newRight ->
                    let newRoot = (newLeft :?> RRBFullNode<'T>).NewParent this.Root.Owner this.Shift [|newLeft; newRight|]
                    let oldShift = max this.Shift right.Shift
                    RRBPersistentVector<'T>(newLen, (RRBMath.up oldShift), newRoot, right.Tail, this.Length + right.TailOffset) :> RRBVector<'T>
        // Transient vectors may only stay transient if appended to a transient of the same owner; here, we're a persistent
        | :? RRBTransientVector<'T> as right ->
            this.Append (right.Persistent())


    // abstract member Insert : int -> 'T -> RRBVector<'T>
    override this.Insert idx newItem =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            if this.Length - this.TailOffset < Literals.blockSize then
                let newTail = this.Tail |> Array.copyAndInsertAt (this.TailOffset - idx) newItem
                RRBPersistentVector<'T>(this.Length, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
            else
                this :> RRBVector<'T>  // TODO: Fixme
        else
            let newRoot, newShift =
                match this.Root.InsertedTree this.Root.Owner this.Shift idx newItem None 0 with
                | SimpleInsertion(newCurrent) -> newCurrent, this.Shift
                | SplitNode(newCurrent, newRight) -> (newCurrent :?> RRBFullNode<'T>).NewParent this.Root.Owner this.Shift [|newCurrent; newRight|], (RRBMath.up this.Shift)
                | SlidItemsLeft(newLeft, newCurrent) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
                | SlidItemsRight(newCurrent, newRight) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
            RRBPersistentVector<'T>(this.Length, newShift, newRoot, this.Tail, this.TailOffset) :> RRBVector<'T>

    // abstract member Remove : int -> RRBVector<'T>
    override this.Remove idx =
        this.RemoveImpl idx false

    member internal this.RemoveWithoutRebalance idx =
        this.RemoveImpl idx true

    member internal this.RemoveImpl idx shouldCheckForRebalancing =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            let newTail = this.Tail  |> Array.copyAndRemoveAt (this.TailOffset - idx)
            RRBPersistentVector<'T>(this.Length, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
        else
            let root' = this.Root.RemovedItem this.Root.Owner this.Shift shouldCheckForRebalancing idx |> snd  // TODO: We don't actually want to return the removed item
            RRBPersistentVector<'T>(this.Length, this.Shift, root', this.Tail, this.TailOffset) :> RRBVector<'T>

    // abstract member Update : int -> 'T -> RRBVector<'T>
    override this.Update idx newItem =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            let newTail = this.Tail |> Array.copyAndSet (this.TailOffset - idx) newItem
            RRBPersistentVector<'T>(this.Length, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
        else
            let root' = this.Root.UpdatedTree this.Root.Owner this.Shift idx newItem
            RRBPersistentVector<'T>(this.Length, this.Shift, root', this.Tail, this.TailOffset) :> RRBVector<'T>

    // abstract member GetItem : int -> 'T
    override this.GetItem idx =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            tail.[idx - this.TailOffset]
        else
            this.Root.GetTreeItem this.Shift idx

    member this.EnsureValidIndex idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx >= this.Length then failwith "Index must not be past the end of the vector"
        else ()

    member this.EnsureValidIndexLengthAllowed idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx > this.Length then failwith "Index must not be more than one past the end of the vector"
        else ()


and RRBTransientVector<'T> internal (count, shift : int, root : RRBNode<'T>, tail : 'T [], tailOffset : int) =
    inherit RRBVector<'T>()

    member val Count = count with get, set
    member val Shift = shift with get, set
    member val Root = root with get, set
    member val Tail = tail with get, set
    member val TailOffset = tailOffset with get, set

    // new() = RRBTransientVector(0, 5, emptyNode, Array.empty, 0)
    new (token : OwnerToken) =
        let root = RRBExpandedFullNode<'T>(token, Array.zeroCreate Literals.blockSize, 0)
        RRBTransientVector<'T>(0, Literals.blockSizeShift, root, Array.zeroCreate Literals.blockSize, 0)

    new () =
        RRBTransientVector<'T>(mkOwnerToken())

    static member MkEmpty() = RRBTransientVector<'T>()
    static member internal MkEmptyWithToken token = RRBTransientVector<'T>(token)

    member this.Persistent() =
        let root' = (this.Root :?> RRBFullNode<'T>).ShrinkRightSpine nullOwner this.Shift
        let tailLen = this.Length - this.TailOffset
        RRBPersistentVector<'T>(this.Count, this.Shift, root', this.Tail |> Array.truncate tailLen, this.TailOffset)

    // abstract member Empty : unit -> RRBVector<'T>
    override this.Empty() =
        this.Count <- 0
        this.Shift <- Literals.blockSize
        let root = this.Root :?> RRBFullNode<'T>
        root.SetNodeSize 0
        Array.fill root.Children 0 Literals.blockSize null
        this.Root <- root
        Array.fill this.Tail 0 Literals.blockSize Unchecked.defaultof<'T>
        this.TailOffset <- 0
        this :> RRBVector<'T>

    // abstract member IsEmpty : unit -> bool
    override this.IsEmpty() = this.Length = 0

    // abstract member StringRepr : string
    override this.StringRepr = this.ToString()

    // abstract member Length : int
    override this.Length = this.Count

    // abstract member IterLeaves : unit -> seq<'T []>
    override this.IterLeaves() = seq {
        yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
        yield this.Tail
    }

    member this.IterEditableLeaves() =
        let owner = this.Root.Owner
        seq {
            yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> (leaf.GetEditableNode owner :?> RRBLeafNode<'T>).Items)
            yield this.Tail
        }

    // abstract member RevIterLeaves : unit -> seq<'T []>
    override this.RevIterLeaves() = seq {
        yield this.Tail
        yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
    }

    member this.RevIterEditableLeaves() =
        let owner = this.Root.Owner
        seq {
            yield this.Tail
            yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> (leaf.GetEditableNode owner :?> RRBLeafNode<'T>).Items)
        }

    // abstract member IterItems : unit -> seq<'T>
    override this.IterItems() =
        this.IterLeaves() |> Seq.collect id

    // abstract member RevIterItems : unit -> seq<'T>
    override this.RevIterItems() = seq {
        for arr in this.RevIterLeaves() do
            for i = arr.Length - 1 downto 0 do
                yield arr.[i]
    }

    // // abstract member GetEnumerator : unit -> IEnumerator<'T>

    // abstract member Push : 'T -> RRBVector<'T>
    override this.Push newItem =
        let tailLen = this.Length - this.TailOffset
        if tailLen >= Literals.blockSize then
            let tailNode = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
            let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Root.Owner this.Shift tailNode
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.TailOffset <- this.Count
            this.Count <- this.Count + 1
            this.Shift <- newShift
            this.Tail.[0] <- newItem
            Array.fill this.Tail 1 (Literals.blockSize - 1) Unchecked.defaultof<'T>
            this :> RRBVector<'T>
        else
            this.Count <- this.Count + 1
            this.Tail.[tailLen] <- newItem
            this :> RRBVector<'T>

    // abstract member Peek : unit -> 'T
    override this.Peek() =
        if this.Length <= 0 then failwith "Can't get last item from an empty vector"
        else
            let tailLen = this.Length - this.TailOffset
#if DEBUG
            if tailLen = 0 then failwith "Tail should never be empty"
#endif
            this.Tail.[tailLen - 1]

    // abstract member Pop : unit -> RRBVector<'T>
    override this.Pop() =
        if this.Length <= 0 then failwith "Can't pop from an empty vector"
        else
            let tailLen = this.Length - this.TailOffset
            if tailLen > 1 then
                this.Count <- this.Count - 1
                this.Tail.[tailLen - 1] <- Unchecked.defaultof<'T>
            else
                this.Count <- this.Count - 1
                let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Root.Owner this.Shift
                if not <| isSameObj newRoot this.Root then
                    this.Root <- newRoot
                this.TailOffset <- this.TailOffset - newTailNode.NodeSize
                this.Tail <- (newTailNode.GetEditableNode this.Root.Owner :?> RRBLeafNode<'T>).Items
            this :> RRBVector<'T>  // FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty

    // abstract member Take : int -> RRBVector<'T>
    override this.Take idx =
        this.EnsureValidIndexLengthAllowed idx
        if idx = 0 then
            this.Empty()
        elif idx = this.Length then
            this :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Root.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.TailOffset <- idx - newTailNode.NodeSize
            this.Tail <- (newTailNode.GetEditableNode this.Root.Owner :?> RRBLeafNode<'T>).Items
            this :> RRBVector<'T>  // Shift can't change during this step, not yet. FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty
        elif idx > this.TailOffset then
            // Splitting the tail in two
            this.Count <- idx
            let newTailLen = idx - this.TailOffset
            Array.fill this.Tail newTailLen (Literals.blockSize - newTailLen) Unchecked.defaultof<'T>
            this :> RRBVector<'T>
        else
            let tmpRoot = this.Root.KeepNTreeItems this.Root.Owner this.Shift idx
            let newTailNode, newRoot = (tmpRoot :?> RRBFullNode<'T>).PopLastLeaf tmpRoot.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.TailOffset <- idx - newTailNode.NodeSize
            this.Tail <- (newTailNode.GetEditableNode this.Root.Owner :?> RRBLeafNode<'T>).Items
            // FIXME: Will have to adjust this tree in case the height can now be shortened, and/or in case new tail and last leaf need to be tweaked to maintain the invariant
            this :> RRBVector<'T>  // Shift can't change during this step, not yet. FIXME: We'll need to check for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty

    // abstract member Skip : int -> RRBVector<'T>
    override this.Skip idx =
        this.EnsureValidIndexLengthAllowed idx
        if idx = 0 then
            this :> RRBVector<'T>
        elif idx = this.Length then
            this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we'll have an empty root
            Array.fill (this.Root :?> RRBFullNode<'T>).Children 0 Literals.blockSize null
            this.Count <- this.Tail.Length
            this.Shift <- Literals.blockSizeShift
            this.TailOffset <- 0
            this :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            Array.fill (this.Root :?> RRBFullNode<'T>).Children 0 Literals.blockSize null
            this.Count <- this.Length - idx
            this.Shift <- Literals.blockSizeShift
            this.TailOffset <- 0
            let idx' = idx - this.TailOffset
            Array.fill this.Tail idx' (Literals.blockSize - idx') Unchecked.defaultof<'T>
            for i = 0 to Literals.blockSize - 1 - idx' do
                this.Tail.[i] <- this.Tail.[i + idx']
            this :> RRBVector<'T>
        else
            let newRoot = this.Root.SkipNTreeItems this.Root.Owner this.Shift idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.Count <- this.Length - idx
            this.TailOffset <- this.TailOffset - idx
            // FIXME: Will have to adjust this tree in case the height can now be shortened, and/or in case new tail and last leaf need to be tweaked to maintain the invariant
            this :> RRBVector<'T>

    // abstract member Split : int -> RRBVector<'T> * RRBVector<'T>
    override this.Split idx =
        // NOTE: Remember that in transients, we need to create the right vector with the same owner token
        // And "this" must remain the left vector. So when idx = 0, we can't just do "this.Empty()" as that
        // would erase the root. Instead, we must first copy the node and hand the copy to right to be its
        // root, or else hand our original node to right and make a brand-new empty node for "this". Don't
        // know yet which is better. If anyone had pointers to our original root... well, they shouldn't.
        // And if anyone had pointers to nodes further down in the tree, copying the root won't do any harm.
        this.EnsureValidIndexLengthAllowed idx
        if idx = 0 then
            let right = RRBTransientVector<'T>(this.Count, this.Shift, this.Root, this.Tail, this.TailOffset)
            this.Count <- 0
            this.Shift <- Literals.blockSizeShift
            this.Root <- RRBExpandedFullNode<'T>(this.Root.Owner, Array.zeroCreate Literals.blockSize, 0)
            this.Tail <- Array.zeroCreate Literals.blockSize
            this.TailOffset <- 0
            this :> RRBVector<'T>, right :> RRBVector<'T>
        elif idx = this.Length then
            let right = RRBTransientVector<'T>(this.Root.Owner)
            this :> RRBVector<'T>, right :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail for the left (this)
            let right = RRBTransientVector<'T>(this.Root.Owner)
            right.Tail <- this.Tail
            right.Count <- this.Count - this.TailOffset
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Root.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.Tail <- newTailNode.Items |> Array.expandToBlockSize
            this.TailOffset <- idx - newTailNode.NodeSize
            // FIXME: We'll need to check "this" (but not "right") for newly-last leaf satisfying invariant, and also potentially shrink tree height if root becomes size 1 or even empty
            this :> RRBVector<'T>, right :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailIdx = idx - this.TailOffset
            let tailLen = this.Count - this.TailOffset
            let right = RRBTransientVector<'T>(this.Root.Owner)
            Array.blit this.Tail tailIdx right.Tail 0 (tailLen - tailIdx)
            Array.fill this.Tail tailIdx (tailLen - tailIdx) Unchecked.defaultof<'T>
            this.Count <- idx
            right.Count <- tailLen
            this :> RRBVector<'T>, right :> RRBVector<'T>
        else
            let rootL, rootR = this.Root.SplitTree this.Root.Owner this.Shift idx
            let right = RRBTransientVector<'T>(this.Length - idx, this.Shift, rootR, this.Tail, this.Length - idx - this.Tail.Length)
            let newTailNodeL, newRootL = (rootL :?> RRBFullNode<'T>).PopLastLeaf rootL.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRootL this.Root then
                this.Root <- newRootL
            this.Tail <- newTailNodeL.Items |> Array.expandToBlockSize
            this.TailOffset <- idx - newTailNodeL.NodeSize
            // FIXME: Going to have to adjust the tree for both "this" AND "right" in this one, since either one could have become a tall, thin tree
            this :> RRBVector<'T>, right :> RRBVector<'T>

    // abstract member Slice : int * int -> RRBVector<'T>
    override this.Slice (start, stop) =
        (this.Skip start).Take stop

    // abstract member GetSlice : int option * int option -> RRBVector<'T>
    override this.GetSlice (start, stop) =
        match start, stop with
        | None, None -> this :> RRBVector<'T>
        | None, Some stop -> this.Take stop
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice (start, stop)

    // abstract member Append : RRBVector<'T> -> RRBVector<'T>
    override this.Append other =
        match other with
        | :? RRBTransientVector<'T> as right when this.Root.IsEditableBy right.Root.Owner ->
            // We can only merge transient trees when they have the same owner, otherwise it's impossible to
            // tell which owner token to give to the new tree. Get it wrong, and there could be some persistent
            // trees out there with references to nodes with that owner token, and so we might end up editing
            // some nodes in-place even though a persistent tree has a reference to that node, which breaks
            // the entire promise of a persistent data structure. The only safe way to merge two trees and have
            // the result be transient is if they have the same owner token, which can only happen if they
            // came from the same original transient tree that was later split.
            let newLen = this.Length + right.Length
            if right.TailOffset <= 0 then
                // Right is a tail-only vector
                let tailLenL = this.Length - this.TailOffset
                let tailLenR = right.Length - right.TailOffset
                if tailLenL + tailLenR <= Literals.blockSize then
                    right.Tail.CopyTo(this.Tail, tailLenL)
                    this.Count <- newLen
                    this :> RRBVector<'T>
                else
                    let newTail = Array.zeroCreate Literals.blockSize
                    let splitIdxR = Literals.blockSize - tailLenR
                    Array.blit right.Tail 0 this.Tail tailLenL splitIdxR
                    Array.blit right.Tail splitIdxR newTail 0 (tailLenR - splitIdxR)
                    let newLeaf = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
                    if this.TailOffset <= 0 then
                        // We CAN use AppendChild in an empty root!
                        let newRoot = (this.Root :?> RRBFullNode<'T>).AppendChild this.Root.Owner this.Shift newLeaf
                        if not <| isSameObj newRoot this.Root then
                            this.Root <- newRoot
                        this.Count <- newLen
                        this.TailOffset <- this.TailOffset + newLeaf.NodeSize
                        this :> RRBVector<'T>
                    else
                        let newRoot = (this.Root :?> RRBFullNode<'T>).AppendChild this.Root.Owner this.Shift newLeaf
                        if not <| isSameObj newRoot this.Root then
                            this.Root <- newRoot
                        this.Count <- newLen
                        this.TailOffset <- this.TailOffset + newLeaf.NodeSize
                        this :> RRBVector<'T>
                        // TODO: The "then" and "else" blocks here are identical. Write test to make sure we can append into a tail-only left transient, then collapse this "if" into a single copy of this block.
            elif this.TailOffset <= 0 then
                // Right has a root and a tail, but we're a tail-only node
                let tailNode = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
                let newRoot, newShift = (right.Root :?> RRBFullNode<'T>).PrependLeaf right.Root.Owner right.Shift tailNode
                if not <| isSameObj newRoot right.Root then
                    right.Root <- newRoot
                // Calling code expects "this" to contain the results, so we steal everything from the right-hand tree
                this.Root <- right.Root
                this.Shift <- newShift
                this.Count <- newLen
                this.TailOffset <- right.TailOffset + tailNode.NodeSize
                this.Tail <- right.Tail
                // FIXME: Figure out how we're going to "invalidate" the right tree now, since we can't set its root owner to null as its root is now our root
                right :> RRBVector<'T>
            else
                // Right has a root and a tail, and so do we
                let tailNode = RRBNode<'T>.MkLeaf this.Root.Owner this.Tail :?> RRBLeafNode<'T>
                let newRoot, newShift =
                    match (this.Root :?> RRBFullNode<'T>).MergeTree this.Root.Owner this.Shift (Some tailNode) right.Shift (right.Root :?> RRBFullNode<'T>) true with
                    | newRoot, None ->
                        newRoot, (max this.Shift right.Shift)
                    | newLeft, Some newRight ->
                        let newRoot = (newLeft :?> RRBFullNode<'T>).NewParent this.Root.Owner this.Shift [|newLeft; newRight|]
                        let oldShift = max this.Shift right.Shift
                        newRoot, (RRBMath.up oldShift)
                this.Count <- newLen
                this.Shift <- newShift
                this.Root <- newRoot
                this.Tail <- right.Tail
                this.TailOffset <- this.Length + right.TailOffset
                this :> RRBVector<'T>
        // Transient vectors may only stay transient if appended to a transient of the same owner
        | :? RRBTransientVector<'T> as right ->
            this.Append (right.Persistent())
        | :? RRBPersistentVector<'T> as right ->
            this.Persistent().Append right
        // TODO: Implement .Persistent() (and also .Transient() on persistent trees)

    // abstract member Insert : int -> 'T -> RRBVector<'T>
    override this.Insert idx newItem =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            if this.Length - this.TailOffset < Literals.blockSize then
                this.Tail.[this.TailOffset - idx] <- newItem
                this :> RRBVector<'T>
            else
                this :> RRBVector<'T>  // TODO: Fixme with PushTailDown
        else
            let newRoot, newShift =
                match this.Root.InsertedTree this.Root.Owner this.Shift idx newItem None 0 with
                | SimpleInsertion(newCurrent) -> newCurrent, this.Shift
                | SplitNode(newCurrent, newRight) -> (newCurrent :?> RRBFullNode<'T>).NewParent this.Root.Owner this.Shift [|newCurrent; newRight|], (RRBMath.up this.Shift)
                | SlidItemsLeft(newLeft, newCurrent) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
                | SlidItemsRight(newCurrent, newRight) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.Shift <- newShift
            this :> RRBVector<'T>

    // abstract member Remove : int -> RRBVector<'T>
    override this.Remove idx =
        this.RemoveImpl idx false

    member internal this.RemoveWithoutRebalance idx =
        this.RemoveImpl idx true

    member internal this.RemoveImpl idx shouldCheckForRebalancing =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            let tailIdx = idx - this.TailOffset
            let tailLen = this.Length - this.TailOffset
            for i = tailIdx to tailLen - 2 do
                this.Tail.[i] <- this.Tail.[i + 1]
            this.Tail.[tailLen - 1] <- Unchecked.defaultof<'T>
        else
            let root' = this.Root.RemovedItem this.Root.Owner this.Shift shouldCheckForRebalancing idx |> snd  // TODO: We don't actually want to return the removed item
            if not <| isSameObj root' this.Root then
                this.Root <- root'
            this.TailOffset <- this.TailOffset - 1
        this.Count <- this.Count - 1
        this :> RRBVector<'T>

    // abstract member Update : int -> 'T -> RRBVector<'T>
    override this.Update idx newItem =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            this.Tail.[this.TailOffset - idx] <- newItem
        else
            let root' = this.Root.UpdatedTree this.Root.Owner this.Shift idx newItem
            if not <| isSameObj root' this.Root then
                this.Root <- root'
        this :> RRBVector<'T>

    // abstract member GetItem : int -> 'T
    override this.GetItem idx =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            tail.[idx - this.TailOffset]
        else
            this.Root.GetTreeItem this.Shift idx

    member this.EnsureValidIndex idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx >= this.Length then failwith "Index must not be past the end of the vector"
        else ()

    member this.EnsureValidIndexLengthAllowed idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx > this.Length then failwith "Index must not be more than one past the end of the vector"
        else ()

(*
and [<StructuredFormatDisplay("{StringRepr}")>] TransientRRBTree<'T> internal (count, shift : int, root : Node, tail : 'T [], tailOffset : int)  =
    // Design of transient trees: you can push items onto the end, but that's it. They do NOT inherit from RRBVector; they are limited-purpose things.
    // They can be created from a persistent tree or they can be converted into a persistent tree.

    // inherit RRBVector<'T>()    // DISABLED. The new design is for these to be tree builders and that's it. Append only.

    let mutable count = count
    let mutable shift = shift
    let mutable root = root
    let mutable tail = tail
    let mutable tailOffset = tailOffset

    let hashCode = ref None

    // TODO: Why does uncommenting this constructor cause this.Persistent(), below, to produce a type error?
    // new() = TransientRRBTree<'T>(0, 0, ExpandedNode.InCurrentThread(), Array.zeroCreate<'T> Literals.blockSize, 0)

    member this.Persistent() : RRBVector<'T> =
        let tailLen = (count - tailOffset)
        if tailLen < Literals.blockSize then
            tail <- Array.sub tail 0 tailLen
        if tailOffset = 0 then
            RRBSapling<'T>(count, 0, [||], tail, tailOffset) :> RRBVector<'T>
        elif root.NodeSize = 1 then
            RRBSapling<'T>(count, 0, root.Array.[0] :?> 'T[], tail, tailOffset) :> RRBVector<'T>
        else
            root.SetThread null
            root <- root.Shrink()
            let mutable curShift = shift
            let mutable curNode = root
            while curShift > Literals.blockSizeShift && curNode.NodeSize > 0 do
                let lastIdx = curNode.NodeSize - 1
                let lastChild = (curNode.Array.[lastIdx] :?> Node).Shrink()
                curNode.Array.[lastIdx] <- box lastChild
                curNode <- lastChild
                curShift <- (RRBMath.down curShift)
            RRBTree<'T>(count, shift, root, tail, tailOffset) :> RRBVector<'T>

    member this.GetItem idx =
        if idx < tailOffset
        then RRBHelpers.getItemFromLeaf<'T> shift idx root
        else tail.[idx - tailOffset]

    member this.Length = count
    member internal this.Shift = shift
    member internal this.Root = root
    member internal this.Tail = tail
    member internal this.TailOffset = tailOffset
    member internal this.Thread = root.Thread

    static member Empty<'T>() = TransientRRBTree<'T>(0, Literals.blockSizeShift, ExpandedNode.InCurrentThread(), Array.zeroCreate<'T> Literals.blockSize, 0)
    member this.IsEmpty() = count = 0

    override this.ToString() =
        sprintf "TransientRRBTree<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" count shift tailOffset root tail

    member this.StringRepr = this.ToString()

    member this.Push (item : 'T) =
        let tailLen = count - tailOffset
        if tailLen < Literals.blockSize then
            // Easy: just add new item in tail and we're done
            tail.[tailLen] <- item
        elif tailOffset = 0 then
            // Empty root means this is the first time we're pushing the tail down
            root <- root.AppendChild<'T> this.Thread shift (box tail) Literals.blockSize
            tailOffset <- Literals.blockSize
            tail <- Array.zeroCreate Literals.blockSize
            tail.[0] <- item
        else
            // TODO: For transients, we need a special version of pushTailDown that creates an ExpandedNode as the new root, and shrinks the current node
            let root', shift' = root.PushTailDown this.Thread shift tail  // This does all the work
            root <- root'
            shift <- shift'
            tailOffset <- tailOffset + Literals.blockSize
            tail <- Array.zeroCreate Literals.blockSize
            tail.[0] <- item
        count <- count + 1
        this

    member this.Update (idx : int) (newItem : 'T) =
        let idx = if idx < 0 then idx + count else idx
        if idx >= count then
            invalidArg "idx" "Tried to update item past the end of the vector"
        if idx >= tailOffset then
            tail.[idx - tailOffset] <- newItem
            this
        else
            let newRoot = root.UpdatedTree this.Thread shift idx newItem
            if not (LanguagePrimitives.PhysicalEquality newRoot root) then
                root <- newRoot
            this
*)
(* Type error above is:
A use of the function '.ctor' does not match a type inferred elsewhere. The inferred type of the function is    int * int * 'T [] * 'T [] * int -> RRBSapling<'T>.    The type of the function required at this point of use is    int * int * 'T0 [] * 'T0 [] * int -> RRBSapling<'T0>    This error may be due to limitations associated with generic recursion within a 'let rec' collection or within a group of classes. Consider giving a full type signature for the targets of recursive calls including type annotations for both argument and return types.

Even stranger was:
A use of the function '.ctor' does not match a type inferred elsewhere. The inferred type of the function is    Microsoft.FSharp.Core.int * Microsoft.FSharp.Core.int * Ficus.RRBVector.Node * 'T Microsoft.FSharp.Core.[] * Microsoft.FSharp.Core.int -> Ficus.RRBVector.RRBTree<'T>.    The type of the function required at this point of use is    Microsoft.FSharp.Core.int * Microsoft.FSharp.Core.int * Ficus.RRBVector.Node * 'T Microsoft.FSharp.Core.[] * Microsoft.FSharp.Core.int -> Ficus.RRBVector.RRBTree<'T>    This error may be due to limitations associated with generic recursion within a 'let rec' collection or within a group of classes. Consider giving a full type signature for the targets of recursive calls including type annotations for both argument and return types.
*)
// BASIC API
(*
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
    let ofSeq (s : seq<'T>) =
        let mutable transient = TransientRRBTree.Empty<'T>()
        for item in s do
            transient <- transient.Push item
        transient.Persistent()
    let inline ofArray (a : 'T[]) = a |> ofSeq
    let inline ofList (l : 'T list) = l |> ofSeq

    // TODO: Try improving average and averageBy by using iterLeafArrays(), summing up each array, and then dividing by count at the end. MIGHT be faster than Seq.average.
    let inline average (vec : RRBVector<'T>) = vec |> Seq.average
    let inline averageBy f (vec : RRBVector<'T>) = vec |> Seq.averageBy f
    let choose (chooser : 'T -> 'U option) (vec : RRBVector<'T>) : RRBVector<'U> =
        let mutable transient = TransientRRBTree.Empty<'U>()
        for item in vec do
            match chooser item with
            | None -> ()
            | Some value -> transient <- transient.Push value
        transient.Persistent()
    // Alternate version. TODO: Benchmark
    let chooseAlt (chooser : 'T -> 'U option) (vec : RRBVector<'T>) : RRBVector<'U> =
        vec |> Seq.choose chooser |> ofSeq

    let chunkBySize chunkSize (vec : RRBVector<'T>) =
        let mutable transient = TransientRRBTree.Empty()
        let mutable remaining = vec
        while remaining.Length > 0 do
            transient <- transient.Push (remaining.Take chunkSize)
            remaining <- remaining.Skip chunkSize
        transient.Persistent()

    let concat (vecs : seq<RRBVector<'T>>) =
        // TODO: Implement concatenation transient RRBVectors so this will be faster (no need to build and throw away so many intermediate result vectors)
        // TODO: Actually, benchmark that and see if it really is all that much faster, considering the complications inherent in concatenating transients
        let mutable result = RRBSapling<'T>.EmptyTree :> RRBVector<'T>
        for vec in vecs do
            result <- result.Append vec
        result

    let inline collect (f : 'T -> RRBVector<'T>) (vec : RRBVector<'T>) =
        // TODO: Benchmark the following two options, because I have no idea which is slower.
        // Option 1, the merging version
        vec |> Seq.map f |> concat

        // Option 2, the one-at-a-time version
        // let mutable transient = TransientRRBTree.Empty()
        // for src in vec do
        //     for item in f src do
        //         transient <- transient.Push item
        // transient.Persistent()

    let inline compareWith f (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) = (vec1, vec2) ||> Seq.compareWith f
    let inline countBy f (vec : RRBVector<'T>) = vec |> Seq.countBy f |> ofSeq
    let inline contains item (vec : RRBVector<'T>) = vec |> Seq.contains item
    let inline distinct (vec : RRBVector<'T>) = vec |> Seq.distinct |> ofSeq
    let inline distinctBy f (vec : RRBVector<'T>) = vec |> Seq.distinctBy f |> ofSeq
    let inline empty<'T> = RRBSapling<'T>.EmptyTree :> RRBVector<'T>
    let exactlyOne (vec : RRBVector<'T>) =
        if vec.Length <> 1 then invalidArg "vec" <| sprintf "exactlyOne called on a vector of %d items (requires a vector of exactly 1 item)" vec.Length
        vec.Peek()
    let except (vec : RRBVector<'T>) (excludedVec : RRBVector<'T>) =
        let excludedSet = System.Collections.Generic.HashSet<'T>(excludedVec)
        let mutable transient = TransientRRBTree.Empty<'T>()
        for item in vec do
            if not (excludedSet.Contains item) then transient <- transient.Push item
        transient.Persistent()
    let inline exists f (vec : RRBVector<'T>) = vec |> Seq.exists f
    let inline exists2 f (vec1 : RRBVector<'T>) (vec2 : RRBVector<'U>) = (vec1, vec2) ||> Seq.exists2 f
    let filter pred (vec : RRBVector<'T>) =
        let mutable transient = TransientRRBTree.Empty<'T>()
        for item in vec do
            if pred item then transient <- transient.Push item
        transient.Persistent()
    let filteri pred (vec : RRBVector<'T>) =
        let mutable transient = TransientRRBTree.Empty<'T>()
        let mutable i = 0
        for item in vec do
            if pred i item then transient <- transient.Push item
            i <- i + 1
        transient.Persistent()
    let filter2 pred (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) =
        let mutable transient = TransientRRBTree.Empty<'T>()
        for item1, item2 in Seq.zip vec1 vec2  do
            if pred item1 item2 then transient <- transient.Push item
        transient.Persistent()
    let filteri2 pred (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) =
        let mutable transient = TransientRRBTree.Empty<'T>()
        let mutable i = 0
        for item1, item2 in Seq.zip vec1 vec2  do
            if pred i item1 item2 then transient <- transient.Push item
            i <- i + 1
        transient.Persistent()
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

    let indexed (vec : RRBVector<'T>) =
        // TODO: Benchmark the ofSeq version vs. the "unrolled" one below
        vec |> Seq.indexed |> ofSeq

        // "Unrolled" version, which may or may not be faster
        // let mutable transient = TransientRRBTree.Empty<int * 'T>()
        // let mutable i = 0
        // for item in vec do
        //     transient <- transient.Push (i, item)
        //     i <- i + 1
        // transient.Persistent()

    //  vec |> Seq.indexed |> RRBHelpers.buildTreeOfSeqWithKnownSize (ref null) vec.Length
    let inline init size f = Seq.init size f |> ofSeq
    let inline isEmpty (vec : RRBVector<'T>) = vec.IsEmpty()
    // Marking the `item` function as "inline" gets error FS1114: The value 'Ficus.RRBVector.RRBVectorModule.item' was marked inline but was not bound in the optimization environment
    // What does that mean?
    let item idx (vec : RRBVector<'T>) = vec.[idx]
    let inline iter f (vec : RRBVector<'T>) = vec |> Seq.iter f
    let inline iter2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1, vec2) ||> Seq.iter2 f
    let inline iteri f (vec : RRBVector<'T>) = vec |> Seq.iteri f
    let inline iteri2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = (vec1, vec2) ||> Seq.iteri2 f
    let inline last (vec : RRBVector<'T>) =
        if vec.Length = 0 then invalidArg "vec" "Can't get last item of empty vector"
        vec.[vec.Length - 1]
    let inline length (vec : RRBVector<'T>) = vec.Length
    // TODO: Make map family of functions use IterEditableLeaves when vector is transient (see FIXME below)
    // TODO: Go through the functions and find any that make the transient be the same size, or just a little larger (e.g. scan), and use IterEditableLeaves there too (with one Insert for scan)
    let inline map f (vec : RRBVector<'T>) = Seq.map f vec |> ofSeq  // FIXME: This needs to be "smarter" if dealing with transient trees, since we need to maintain owner tokens
    let inline map2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = Seq.map2 f vec1 vec2 |> ofSeq
    let inline map3 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) (vec3 : RRBVector<'T3>) = Seq.map3 f vec1 vec2 vec3 |> ofSeq
    let inline mapi f (vec : RRBVector<'T>) = Seq.mapi f vec |> ofSeq
    let inline mapi2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) = Seq.mapi2 f vec1 vec2 |> ofSeq

    let mapFold folder initState (vec : RRBVector<'T>) =
        if isEmpty vec then empty<'T> else
        let mutable transient = TransientRRBTree.Empty<'T>()
        let mutable state = initState
        for item in vec do
            let item',state' = folder state item
            transient <- transient.Push item'
            state <- state'
        transient.Persistent()

    let mapFoldBack folder (vec : RRBVector<'T>) initState =
        if isEmpty vec then empty<'T> else
        let mutable transient = TransientRRBTree.Empty<'T>()
        let mutable state = initState
        for item in vec.RevIterItems() do
            let item',state' = folder item state
            transient <- transient.Push item'
            state <- state'
        transient.Persistent()

    let inline max (vec : RRBVector<'T>) = vec |> Seq.max
    let inline maxBy f (vec : RRBVector<'T>) = vec |> Seq.maxBy f
    let inline min (vec : RRBVector<'T>) = vec |> Seq.min
    let inline minBy f (vec : RRBVector<'T>) = vec |> Seq.minBy f
    let inline pairwise (vec : RRBVector<'T>) = vec |> Seq.pairwise |> ofSeq
    let partition pred (vec : RRBVector<'T>) =
        let mutable trueItems = TransientRRBTree.Empty<'T>()
        let mutable falseItems = TransientRRBTree.Empty<'T>()
        for item in vec do
            if pred item then trueItems <- trueItems.Push item else falseItems <- falseItems.Push item
        trueItems.Persistent(), falseItems.Persistent()

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
    let replicate count value = // TODO: Implement this better once we have transient RRBVectors (or once we can do updates on transient PersistentVectors)
        if count = 0 then empty<'T> else
        let mutable transient = TransientRRBTree.Empty<'T>()
        for i = 1 to count do
            transient <- transient.Push value
        transient.Persistent()

    let rev (vec : RRBVector<'T>) =
        if isEmpty vec then empty<'T> else
        let mutable transient = TransientRRBTree.Empty<'T>()
        for item in vec.RevIterItems() do
            transient <- transient.Push item
        transient.Persistent()

    let inline scan f initState (vec : RRBVector<'T>) = vec |> Seq.scan f initState |> ofSeq
    let scanBack initState f (vec : RRBVector<'T>) =
        let f' = flip f
        vec.RevIterItems() |> Seq.scan f' initState |> ofSeq

    // TODO: Why does this first version cause a type error, while the second one doesn't?
    // let singleton<'T> (item : 'T) = RRBSapling<'T>(1, 0, [||], [|item|], 0) :> RRBVector<'T>
    let singleton (item : 'T) = RRBSapling.Singleton item :> RRBVector<'T>

    let inline skip count (vec : RRBVector<'T>) = vec.Skip count
    let skipWhile pred (vec : RRBVector<'T>) = // TODO: Test this
        let rec loop pred n =
            if n >= vec.Length then empty<'T>
            elif pred vec.[n] then loop pred (n+1)
            else vec.Skip n
        loop pred 0
    // TODO: Implement a sort-in-place algorithm (perhaps TimSort from Python?) on transients, then benchmark against this simple version of sorting
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
        let mutable vec1 = TransientRRBTree.Empty<'T1>()
        let mutable vec2 = TransientRRBTree.Empty<'T2>()
        for a, b in vec do
            vec1 <- vec1.Push a
            vec2 <- vec2.Push b
        vec1.Persistent(), vec2.Persistent()
    let unzip3 (vec : RRBVector<'T1 * 'T2 * 'T3>) =
        let mutable vec1 = TransientRRBTree.Empty<'T1>()
        let mutable vec2 = TransientRRBTree.Empty<'T2>()
        let mutable vec3 = TransientRRBTree.Empty<'T3>()
        for a, b, c in vec do
            vec1 <- vec1.Push a
            vec2 <- vec2.Push b
            vec3 <- vec3.Push c
        vec1.Persistent(), vec2.Persistent(), vec3.Persistent()
    let inline where pred (vec : RRBVector<'T>) = filter pred vec

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
                yield RRBSapling<'T>.OfTail tail :> RRBVector<'T>
                while itemEnumerator.MoveNext() do
                    tail <- tail |> Array.popFirstAndPush itemEnumerator.Current
                    yield RRBSapling<'T>.OfTail tail :> RRBVector<'T>
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
                yield RRBSapling<'T>.OfRootAndTail root tail :> RRBVector<'T>
                while itemEnumerator.MoveNext() do
                    root <- root |> Array.popFirstAndPush tail.[0]
                    tail <- tail |> Array.popFirstAndPush itemEnumerator.Current
                    yield RRBSapling<'T>.OfRootAndTail root tail :> RRBVector<'T>
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

    let inline zip (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        (vec1, vec2) ||> Seq.zip |> ofSeq
    let inline zip3 (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) (vec3 : RRBVector<'T3>) =
        (vec1, vec2, vec3) |||> Seq.zip3 |> ofSeq
*)
