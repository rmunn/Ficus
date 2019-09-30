// Look for TOCHECK to find places where I'm unsure of the correct logic

/// Relaxed Radix Balanced Vector
///
/// Original concept: Phil Bagwell and Tiark Rompf
/// https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf
///
/// Partly based on work by Jean Niklas L'orange: http://hypirion.com/thesis

module Ficus.RRBVector

open System.Threading
open RRBArrayExtensions
open RRBVectorNodes

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
    // TODO: Expose .Transient() and .Persistent() on RRBVector interface

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

type RRBPersistentVector<'T> internal (count, shift : int, root : RRBNode<'T>, tail : 'T [], tailOffset : int) =
    // TODO: Consider specifying that the root must always be an RRBFullNode<'T>, so we don't have to do nearly as many casts
    inherit RRBVector<'T>()

    member this.Count = count
    member this.Shift = shift
    member this.Root = root
    member this.Tail = tail
    member this.TailOffset = tailOffset

    override this.ToString() =
        sprintf "RRBPersistentVector<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" count shift tailOffset root tail

    internal new (token : OwnerToken) =
        let root = RRBFullNode<'T>(token, Array.empty)
        RRBPersistentVector<'T>(0, Literals.blockSizeShift, root, Array.empty, 0)

    new () =
        RRBPersistentVector<'T>(0, Literals.blockSizeShift, emptyNode, Array.empty, 0)

    static member MkEmpty() = RRBPersistentVector<'T>()
    static member internal MkEmptyWithToken token = RRBPersistentVector<'T>(token)

    // TODO: Expose .Transient() and .Persistent() on RRBVector interface
    member this.Transient() =
        let newToken = mkOwnerToken()
        let newRoot = (this.Root :?> RRBFullNode<'T>).ExpandRightSpine newToken this.Shift
        let tailLen = this.Count - this.TailOffset
        let newTail = Array.zeroCreate Literals.blockSize
        this.Tail.CopyTo(newTail, 0)
        RRBTransientVector<'T>(this.Count, this.Shift, newRoot, newTail, this.TailOffset)

    member internal this.AdjustTree() =
        let v : RRBVector<'T> = this.ShiftNodesFromTailIfNeeded()
        (v :?> RRBPersistentVector<'T>).ShortenTree()

    member internal this.ShortenTree() =
        if this.Shift <= Literals.blockSizeShift then this :> RRBVector<'T>
        else
            if this.Root.NodeSize > 1 then this :> RRBVector<'T>
            elif this.Root.NodeSize = 1 then
                RRBPersistentVector<'T>(this.Count, RRBMath.down this.Shift, (this.Root :?> RRBFullNode<'T>).FirstChild, this.Tail, this.TailOffset).ShortenTree()
            else // Empty root
                RRBPersistentVector<'T>(this.Count, Literals.blockSizeShift, emptyNode, this.Tail, this.TailOffset) :> RRBVector<'T>

    member internal this.ShiftNodesFromTailIfNeeded() =
        if this.TailOffset <= 0 || this.Root.NodeSize = 0 then
            // Empty root, so no need to shift any nodes
            this :> RRBVector<'T>
        else
            let lastTwig = (this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift
            // If parent of last leaf is a relaxed node, this automatically satisfies the invariant
            if lastTwig :? RRBRelaxedNode<'T> then
                this :> RRBVector<'T>
            else
                let lastLeaf = lastTwig.LastChild :?> RRBLeafNode<'T>
                let shiftCount = Literals.blockSize - lastLeaf.NodeSize
                let tailLen = this.Count - this.TailOffset
                if shiftCount <= 0 then
                    this :> RRBVector<'T>
                elif shiftCount >= tailLen then
                    // Would shift everything out of the tail, so instead we'll promote a new tail
                    let removedLeaf, newRoot = (this.Root :?> RRBFullNode<'T>).RemoveLastLeaf nullOwner this.Shift
                    let newTail = this.Tail |> Array.append removedLeaf.Items
                    // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
                    RRBPersistentVector<'T>(this.Count, this.Shift, newRoot, newTail, this.TailOffset - removedLeaf.NodeSize).ShiftNodesFromTailIfNeeded()
                else
                    let itemsToShift, newTail = this.Tail |> Array.splitAt shiftCount
                    let newLeaf = RRBLeafNode<'T>(nullOwner, Array.append lastLeaf.Items itemsToShift)
                    let newRoot = (this.Root :?> RRBFullNode<'T>).ReplaceLastLeaf nullOwner this.Shift newLeaf shiftCount
                    // No need to recurse here
                    RRBPersistentVector<'T>(this.Count, this.Shift, newRoot, newTail, this.TailOffset + shiftCount) :> RRBVector<'T>

    // abstract member Empty : RRBVector<'T>  // Or maybe it should be unit -> RRBVector<'T>
    override this.Empty() = RRBPersistentVector<'T>(0, Literals.blockSizeShift, emptyNode, Array.empty, 0) :> RRBVector<'T>

    // abstract member IsEmpty : unit -> bool
    override this.IsEmpty() = this.Count = 0

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
        yield! (this.Root :?> RRBFullNode<'T>).RevLeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
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
        let tailLen = this.Count - this.TailOffset
        if tailLen < Literals.blockSize then
            let newTail = this.Tail |> Array.copyAndAppend newItem
            RRBPersistentVector<'T>(this.Count + 1, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
        else
            let tailNode = RRBNode<'T>.MkLeaf nullOwner this.Tail :?> RRBLeafNode<'T>
            let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf nullOwner this.Shift tailNode
            RRBPersistentVector<'T>(this.Count + 1, newShift, newRoot, [|newItem|], this.Count) :> RRBVector<'T>

    // abstract member Peek : unit -> 'T
    override this.Peek() =
        if this.Count <= 0 then failwith "Can't get last item from an empty vector"
        else
            let tailLen = this.Count - this.TailOffset
#if DEBUG
            if tailLen = 0 then failwith "Tail should never be empty"
#endif
            this.Tail.[tailLen - 1]

    // abstract member Pop : unit -> RRBVector<'T>
    override this.Pop() =
        if this.Count <= 0 then failwith "Can't pop from an empty vector"
        elif this.Count = 1 then this.Empty()
        else
            if this.Tail.Length > 1 then
                let newTail = this.Tail |> Array.copyAndPop
                RRBPersistentVector<'T>(this.Count - 1, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
            else
                let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
                RRBPersistentVector<'T>(this.Count - 1, this.Shift, newRoot, newTailNode.Items, this.TailOffset - newTailNode.NodeSize).AdjustTree()

    // abstract member Take : int -> RRBVector<'T>
    override this.Take idx =
        if idx >= this.Count then
            this :> RRBVector<'T>
        elif idx <= 0 then  // TODO: Allow taking negative items, which is like Skip (negative count + length), i.e. Take -5 will return the last five items of the list
            this.Empty()
        elif idx = this.TailOffset then
            // Dropping the tail and nothing else, so we promote a new tail
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
            RRBPersistentVector<'T>(idx, this.Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize).AdjustTree()
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let newTail = this.Tail |> Array.truncate (idx - this.TailOffset)
            RRBPersistentVector<'T>(idx, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
        else
            let tmpRoot = this.Root.KeepNTreeItems nullOwner this.Shift idx
            let newTailNode, newRoot = (tmpRoot :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
            RRBPersistentVector<'T>(idx, this.Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize).AdjustTree()

    // abstract member Skip : int -> RRBVector<'T>
    override this.Skip idx =
        if idx <= 0 then
            this :> RRBVector<'T>
        elif idx >= this.Count then
            this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we'll have an empty root
            RRBPersistentVector<'T>(this.Tail.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, this.Tail, 0) :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let newTail = this.Tail |> Array.skip (idx - this.TailOffset)
            RRBPersistentVector<'T>(newTail.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, newTail, 0) :> RRBVector<'T>
        else
            let newRoot = this.Root.SkipNTreeItems nullOwner this.Shift idx
            RRBPersistentVector<'T>(this.Count - idx, this.Shift, newRoot, this.Tail, this.TailOffset - idx).AdjustTree()

    // abstract member Split : int -> RRBVector<'T> * RRBVector<'T>
    override this.Split idx =
        this.EnsureValidIndexLengthAllowed idx
        if idx = this.Count then
            this :> RRBVector<'T>, this.Empty()
        elif idx = 0 then
            this.Empty(), this :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
            let newLeft = RRBPersistentVector<'T>(idx, this.Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize).AdjustTree()
            let newRight = RRBPersistentVector<'T>(this.Tail.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, this.Tail, 0) :> RRBVector<'T>
            newLeft, newRight
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailL, tailR = this.Tail |> Array.splitAt (idx - this.TailOffset)
            let newLeft = RRBPersistentVector<'T>(idx, this.Shift, this.Root, tailL, this.TailOffset) :> RRBVector<'T>
            let newRight = RRBPersistentVector<'T>(tailR.Length, Literals.blockSizeShift, RRBNode<'T>.MkFullNode nullOwner Array.empty, tailR, 0) :> RRBVector<'T>
            newLeft, newRight
        else
            let rootL, rootR = this.Root.SplitTree nullOwner this.Shift idx
            // TOCHECK: Uncomment the below lines and see if they're also correct
            // let rootSizeL = rootL.TreeSize this.Shift
            // let rootSizeR = rootR.TreeSize this.Shift
            // let newTailNodeL, newRootL = (rootL :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
            // let newLeft = RRBPersistentVector<'T>(rootSizeL, this.Shift, newRootL, newTailNodeL.Items, rootSizeL - newTailNodeL.NodeSize).AdjustTree()
            // let newRight = RRBPersistentVector<'T>(rootSizeR + this.Tail.Length, this.Shift, rootR, this.Tail, rootSizeR).AdjustTree()
            let newTailNodeL, newRootL = (rootL :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
            // Have to adjust the tree for both newLeft AND newRight in this one, since either one could have become a tall, thin tree during the split
            let newLeft = RRBPersistentVector<'T>(idx, this.Shift, newRootL, newTailNodeL.Items, idx - newTailNodeL.NodeSize).AdjustTree()
            let newRight = RRBPersistentVector<'T>(this.Count - idx, this.Shift, rootR, this.Tail, this.Count - idx - this.Tail.Length).AdjustTree()
            newLeft, newRight

    // abstract member Slice : int * int -> RRBVector<'T>
    override this.Slice (start, stop) =
        (this.Skip start).Take (stop - start + 1)

    // abstract member GetSlice : int option * int option -> RRBVector<'T>
    override this.GetSlice (start, stop) =
        match start, stop with
        | None, None -> this :> RRBVector<'T>
        | None, Some stop -> this.Take (stop + 1)  // vec.[..5] should return all indices from 0 to 5, i.e. 6 in total
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice (start, stop)

    // abstract member Append : RRBVector<'T> -> RRBVector<'T>
    override this.Append other =
        match other with
        | :? RRBPersistentVector<'T> as right ->
            if this.Count = 0 then right :> RRBVector<'T>
            elif right.Count = 0 then this :> RRBVector<'T> else
            let newLen = this.Count + right.Count
            if right.TailOffset <= 0 then
                // Right is a tail-only vector
                let tailLenL = this.Count - this.TailOffset
                let tailLenR = right.Count - right.TailOffset
                if tailLenL + tailLenR <= Literals.blockSize then
                    let newTail = Array.append this.Tail right.Tail
                    RRBPersistentVector<'T>(newLen, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
                else
                    let newLeafItems, newTail = Array.appendAndSplitAt Literals.blockSize this.Tail right.Tail
                    let newLeaf = RRBNode<'T>.MkLeaf nullOwner newLeafItems :?> RRBLeafNode<'T>
                    if this.TailOffset <= 0 then
                        // Can't use AppendLeaf in an empty root, so we create the first twig by hand
                        // FIXME: Yes, we can actually. So this if-else isn't actually necessary! I think. TODO: Test this.
                        let newRoot = RRBNode<'T>.MkFullNode nullOwner [|newLeaf|]
                        RRBPersistentVector<'T>(newLen, Literals.blockSizeShift, newRoot, newTail, Literals.blockSize) :> RRBVector<'T>
                    else
                        let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf nullOwner this.Shift newLeaf
                        // The new leaf that was pushed down cannot have been short, so here we don't need to adjust the tree to maintain the invariant.
                        RRBPersistentVector<'T>(newLen, newShift, newRoot, newTail, this.TailOffset + Literals.blockSize) :> RRBVector<'T>
            elif this.TailOffset <= 0 then
                // Right has a root and a tail, but we're a tail-only node
                let tailNode = RRBNode<'T>.MkLeaf nullOwner this.Tail :?> RRBLeafNode<'T>
                let newRoot, newShift = (right.Root :?> RRBFullNode<'T>).PrependLeaf nullOwner right.Shift tailNode
                RRBPersistentVector<'T>(newLen, newShift, newRoot, right.Tail, right.TailOffset + tailNode.NodeSize) :> RRBVector<'T>
            else
                // Right has a root and a tail, and so do we
                // TODO FIXME: Determine if there are any merge scenarios which break the invariant, by running lots of merges that hit this code branch (Original code had .AdjustTree everywhere, but is it needed?)
                // One scenario I can think of is a left tree of [M*M-1] TM, while the right tree is [M] T5 or something. TODO: Work that out more precisely.
                let tailNode = RRBNode<'T>.MkLeaf nullOwner this.Tail :?> RRBLeafNode<'T>
                // Can the tail be merged into the two twig nodes? Now's the time to find out, while we can still push it down to form a new root
                let tailCanFit = ((this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift).HasRoomToMergeTheTail Literals.blockSizeShift tailNode ((right.Root :?> RRBFullNode<'T>).LeftmostTwig right.Shift)
                let mergedShift, mergedTree =
                    if tailCanFit
                    then max this.Shift right.Shift, (this.Root :?> RRBFullNode<'T>).MergeTree nullOwner this.Shift (Some tailNode) right.Shift (right.Root :?> RRBFullNode<'T>) false
                    else
                        let tmpRoot, tmpShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf nullOwner this.Shift tailNode
                        max tmpShift right.Shift, (tmpRoot :?> RRBFullNode<'T>).MergeTree nullOwner tmpShift None right.Shift (right.Root :?> RRBFullNode<'T>) false
                match mergedTree with
                | newRoot, None ->
                    RRBPersistentVector<'T>(newLen, mergedShift, newRoot, right.Tail, this.Count + right.TailOffset).AdjustTree()
                | newLeft, Some newRight ->
                    let newRoot = (newLeft :?> RRBFullNode<'T>).NewParent nullOwner mergedShift [|newLeft; newRight|]
                    RRBPersistentVector<'T>(newLen, (RRBMath.up mergedShift), newRoot, right.Tail, this.Count + right.TailOffset).AdjustTree()
        // Transient vectors may only stay transient if appended to a transient of the same owner; here, we're a persistent
        | :? RRBTransientVector<'T> as right ->
            this.Append (right.Persistent())
        | _ ->
            this.Append (other :?> RRBPersistentVector<'T>)  // WILL throw if we create a new subclass. TODO: Decide whether shutting up the compiler like this is really a good idea.

    // abstract member Insert : int -> 'T -> RRBVector<'T>
    override this.Insert idx newItem =
        this.EnsureValidIndexLengthAllowed idx
        if idx >= this.TailOffset then
            if this.Count - this.TailOffset < Literals.blockSize then
                let newTail = this.Tail |> Array.copyAndInsertAt (idx - this.TailOffset) newItem
                RRBPersistentVector<'T>(this.Count + 1, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
            else
                let newLeafItems, newTailItem = this.Tail |> Array.copyAndInsertIntoFullArray (idx - this.TailOffset) newItem
                let newTail = Array.singleton newTailItem
                let newLeafNode = RRBNode<'T>.MkLeaf nullOwner newLeafItems :?> RRBLeafNode<'T>
                let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf nullOwner this.Shift newLeafNode
                // Pushing a full tail down into a leaf can't break the invariant, so no need to adjust the tree here
                RRBPersistentVector<'T>(this.Count + 1, newShift, newRoot, newTail, this.Count) :> RRBVector<'T>
        else
            let newRoot, newShift =
                match this.Root.InsertedTree nullOwner this.Shift idx newItem None 0 with
                | SimpleInsertion(newCurrent) -> newCurrent, this.Shift
                | SplitNode(newCurrent, newRight) -> (newCurrent :?> RRBFullNode<'T>).NewParent nullOwner this.Shift [|newCurrent; newRight|], (RRBMath.up this.Shift)
                | SlidItemsLeft(newLeft, newCurrent) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
                | SlidItemsRight(newCurrent, newRight) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
            RRBPersistentVector<'T>(this.Count + 1, newShift, newRoot, this.Tail, this.TailOffset + 1).AdjustTree()  // TODO: Remove this AdjustTree() call and prove that tests fail when we do that (inserting into last leaf could cause invariant failure)

    // abstract member Remove : int -> RRBVector<'T>
    override this.Remove idx =
        this.RemoveImpl idx true

    member internal this.RemoveWithoutRebalance idx =
        this.RemoveImpl idx false

    member internal this.RemoveImpl idx shouldCheckForRebalancing =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            if this.Count - this.TailOffset > 1 then
                let newTail = this.Tail |> Array.copyAndRemoveAt (idx - this.TailOffset)
                RRBPersistentVector<'T>(this.Count - 1, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
            elif this.Count = 1 then
                this.Empty()
            else
                // Tail is now empty, so promote a new tail
                let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
                RRBPersistentVector<'T>(this.Count - 1, this.Shift, newRoot, newTailNode.Items, this.TailOffset - newTailNode.NodeSize).AdjustTree()
        else
            let newRoot = this.Root.RemovedItem nullOwner this.Shift shouldCheckForRebalancing idx
            RRBPersistentVector<'T>(this.Count - 1, this.Shift, newRoot, this.Tail, this.TailOffset - 1).AdjustTree()

    // abstract member Update : int -> 'T -> RRBVector<'T>
    override this.Update idx newItem =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            let newTail = this.Tail |> Array.copyAndSet (idx - this.TailOffset) newItem
            RRBPersistentVector<'T>(this.Count, this.Shift, this.Root, newTail, this.TailOffset) :> RRBVector<'T>
        else
            let newRoot = this.Root.UpdatedTree nullOwner this.Shift idx newItem
            RRBPersistentVector<'T>(this.Count, this.Shift, newRoot, this.Tail, this.TailOffset) :> RRBVector<'T>

    // abstract member GetItem : int -> 'T
    override this.GetItem idx =
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            tail.[idx - this.TailOffset]
        else
            this.Root.GetTreeItem this.Shift idx

    member this.EnsureValidIndex idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx >= this.Count then failwith "Index must not be past the end of the vector"
        else ()

    member this.EnsureValidIndexLengthAllowed idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx > this.Count then failwith "Index must not be more than one past the end of the vector"
        else ()


and RRBTransientVector<'T> internal (count, shift : int, root : RRBNode<'T>, tail : 'T [], tailOffset : int) =
    inherit RRBVector<'T>()

    member val Count = count with get, set
    member val Shift = shift with get, set
    member val Root = root with get, set
    member val Tail = tail with get, set
    member val TailOffset = tailOffset with get, set
    member val internal Owner = root.Owner with get, set

    override this.ToString() =
        sprintf "RRBTransientVector<owner=%s,length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>" (if !this.Owner |> isNull then "<null>" else !this.Owner) this.Count this.Shift this.TailOffset this.Root this.Tail

    // new() = RRBTransientVector(0, 5, emptyNode, Array.empty, 0)
    internal new (token : OwnerToken) =
        let root = RRBExpandedFullNode<'T>(token, Array.zeroCreate Literals.blockSize, 0)
        RRBTransientVector<'T>(0, Literals.blockSizeShift, root, Array.zeroCreate Literals.blockSize, 0)

    new () =
        RRBTransientVector<'T>(mkOwnerToken())

    static member MkEmpty() = RRBTransientVector<'T>()
    static member internal MkEmptyWithToken token = RRBTransientVector<'T>(token)

    member this.Invalidate() = this.Owner <- nullOwner

    member this.ThrowIfNotValid(?msg : string) =
        if isNull (!this.Owner) then
            let msg = defaultArg msg "any operations"
            invalidOp <| sprintf "This vector is no longer valid for %s" msg

    // TODO: Expose .Transient() and .Persistent() on RRBVector interface
    member this.Persistent() =
        let newRoot = (this.Root :?> RRBFullNode<'T>).ShrinkRightSpine nullOwner this.Shift
        let tailLen = this.Count - this.TailOffset
        this.Invalidate()
        RRBPersistentVector<'T>(this.Count, this.Shift, newRoot, this.Tail |> Array.truncate tailLen, this.TailOffset)

    member internal this.AdjustTree() =
        let v : RRBVector<'T> = this.ShiftNodesFromTailIfNeeded()
        (v :?> RRBTransientVector<'T>).ShortenTree()

    member internal this.ShortenTree() =
        if this.Shift <= Literals.blockSizeShift then this :> RRBVector<'T>
        else
            if this.Root.NodeSize > 1 then this :> RRBVector<'T>
            elif this.Root.NodeSize = 1 then
                this.Shift <- RRBMath.down this.Shift
                this.Root <- (this.Root :?> RRBFullNode<'T>).FirstChild
                this.ShortenTree()
            else // Empty root but shift was too large
                this.Shift <- Literals.blockSizeShift
                this :> RRBVector<'T>

    member internal this.ShiftNodesFromTailIfNeeded() =
        if this.TailOffset <= 0 || this.Root.NodeSize = 0 then
            // Empty root, so no need to shift any nodes
            this :> RRBVector<'T>
        else
            let lastTwig = (this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift
            // If parent of last leaf is a relaxed node, this automatically satisfies the invariant
            if lastTwig :? RRBRelaxedNode<'T> then
                this :> RRBVector<'T>
            else
                let lastLeaf = lastTwig.LastChild :?> RRBLeafNode<'T>
                let shiftCount = Literals.blockSize - lastLeaf.NodeSize
                let tailLen = this.Count - this.TailOffset
                if shiftCount <= 0 then
                    this :> RRBVector<'T>
                elif shiftCount >= tailLen then
                    // Would shift everything out of the tail, so instead we'll promote a new tail
                    let removedLeaf, newRoot = (this.Root :?> RRBFullNode<'T>).RemoveLastLeaf this.Owner this.Shift
                    let removedSize = removedLeaf.NodeSize
                    for i = tailLen - 1 downto 0 do
                        this.Tail.[i + removedSize] <- this.Tail.[i]
                    removedLeaf.Items.CopyTo(this.Tail, 0)
                    this.TailOffset <- this.TailOffset - removedSize
                    if not <| isSameObj newRoot this.Root then
                        this.Root <- newRoot
                    // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
                    this.ShiftNodesFromTailIfNeeded()
                else
                    let itemsToShift = Array.sub this.Tail 0 shiftCount
                    for i = 0 to tailLen - shiftCount - 1 do
                        this.Tail.[i] <- this.Tail.[i + shiftCount]
                    Array.fill this.Tail (tailLen - shiftCount) shiftCount Unchecked.defaultof<'T>
                    let newLeaf = RRBLeafNode<'T>(this.Owner, Array.append lastLeaf.Items itemsToShift)
                    let newRoot = (this.Root :?> RRBFullNode<'T>).ReplaceLastLeaf this.Owner this.Shift newLeaf shiftCount
                    if not <| isSameObj newRoot this.Root then
                        this.Root <- newRoot
                    this.TailOffset <- this.TailOffset + shiftCount
                    // No need to recurse here
                    this :> RRBVector<'T>

    // abstract member Empty : unit -> RRBVector<'T>
    override this.Empty() =
        this.ThrowIfNotValid()
        this.Count <- 0
        this.Shift <- Literals.blockSizeShift
        let root = this.Root :?> RRBFullNode<'T>
        root.SetNodeSize 0
        Array.fill root.Children 0 Literals.blockSize null
        this.Root <- root
        Array.fill this.Tail 0 Literals.blockSize Unchecked.defaultof<'T>
        this.TailOffset <- 0
        this :> RRBVector<'T>

    // abstract member IsEmpty : unit -> bool
    override this.IsEmpty() = this.Count = 0

    // abstract member StringRepr : string
    override this.StringRepr = this.ToString()

    // abstract member Length : int
    override this.Length = this.Count

    // abstract member IterLeaves : unit -> seq<'T []>
    override this.IterLeaves() =
        this.ThrowIfNotValid()
        seq {
            yield! (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
            yield this.Tail |> Array.truncate (this.Count - this.TailOffset)
        }

    member this.IterEditableLeavesWithoutTail() =
        this.ThrowIfNotValid()
        let owner = this.Owner
        (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift |> Seq.map (fun leaf -> (leaf.GetEditableNode owner :?> RRBLeafNode<'T>).Items)

    // abstract member RevIterLeaves : unit -> seq<'T []>
    override this.RevIterLeaves() =
        this.ThrowIfNotValid()
        seq {
            yield this.Tail |> Array.truncate (this.Count - this.TailOffset)
            yield! (this.Root :?> RRBFullNode<'T>).RevLeavesSeq this.Shift |> Seq.map (fun leaf -> leaf.Items)
        }

    member this.RevIterEditableLeavesWithoutTail() =
        this.ThrowIfNotValid()
        let owner = this.Owner
        (this.Root :?> RRBFullNode<'T>).RevLeavesSeq this.Shift |> Seq.map (fun leaf -> (leaf.GetEditableNode owner :?> RRBLeafNode<'T>).Items)

    // abstract member IterItems : unit -> seq<'T>
    override this.IterItems() =
        this.ThrowIfNotValid()
        this.IterLeaves() |> Seq.collect id

    // abstract member RevIterItems : unit -> seq<'T>
    override this.RevIterItems() =
        this.ThrowIfNotValid()
        seq {
            for arr in this.RevIterLeaves() do
                for i = arr.Length - 1 downto 0 do
                    yield arr.[i]
        }

    // // abstract member GetEnumerator : unit -> IEnumerator<'T>

    // abstract member Push : 'T -> RRBVector<'T>
    override this.Push newItem =
        this.ThrowIfNotValid()
        let tailLen = this.Count - this.TailOffset
        if tailLen < Literals.blockSize then
            this.Count <- this.Count + 1
            this.Tail.[tailLen] <- newItem
            this :> RRBVector<'T>
        else
            let tailNode = RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>
            let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift tailNode
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.TailOffset <- this.Count
            this.Count <- this.Count + 1
            this.Shift <- newShift
            this.Tail <- Array.zeroCreate Literals.blockSize
            this.Tail.[0] <- newItem
            this :> RRBVector<'T>

    // abstract member Peek : unit -> 'T
    override this.Peek() =
        this.ThrowIfNotValid()
        if this.Count <= 0 then failwith "Can't get last item from an empty vector"
        else
            let tailLen = this.Count - this.TailOffset
#if DEBUG
            if tailLen = 0 then failwith "Tail should never be empty"
#endif
            this.Tail.[tailLen - 1]

    // abstract member Pop : unit -> RRBVector<'T>
    override this.Pop() =
        this.ThrowIfNotValid()
        if this.Count <= 0 then failwith "Can't pop from an empty vector"
        elif this.Count = 1 then this.Empty()
        else
            let tailLen = this.Count - this.TailOffset
            if tailLen > 1 then
                this.Count <- this.Count - 1
                this.Tail.[tailLen - 1] <- Unchecked.defaultof<'T>
                this :> RRBVector<'T>
            else
                this.Count <- this.Count - 1
                let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift
                if not <| isSameObj newRoot this.Root then
                    this.Root <- newRoot
                this.TailOffset <- this.TailOffset - newTailNode.NodeSize
                this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
                this.AdjustTree()

    // abstract member Take : int -> RRBVector<'T>
    override this.Take idx =
        this.ThrowIfNotValid()
        if idx >= this.Count then
            this :> RRBVector<'T>
        elif idx <= 0 then  // TODO: Allow taking negative items, which is like Skip (negative count + length), i.e. Take -5 will return the last five items of the list
            this.Empty()
        elif idx = this.TailOffset then
            // Dropping the tail and nothing else, so we promote a new tail
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.TailOffset <- idx - newTailNode.NodeSize
            this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
            this.AdjustTree()
        elif idx > this.TailOffset then
            // Splitting the tail in two
            this.Count <- idx
            let newTailLen = idx - this.TailOffset
            Array.fill this.Tail newTailLen (Literals.blockSize - newTailLen) Unchecked.defaultof<'T>
            this :> RRBVector<'T>
        else
            let tmpRoot = this.Root.KeepNTreeItems this.Owner this.Shift idx
            let newTailNode, newRoot = (tmpRoot :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift
            let newRoot' = (newRoot :?> RRBFullNode<'T>).MaybeExpand this.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRoot' this.Root then
                this.Root <- newRoot'
            this.TailOffset <- idx - newTailNode.NodeSize
            this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
            this.AdjustTree()

    // abstract member Skip : int -> RRBVector<'T>
    override this.Skip idx =
        this.ThrowIfNotValid()
        if idx <= 0 then
            this :> RRBVector<'T>
        elif idx >= this.Count then
            this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we'll have an empty root
            this.Root.SetNodeSize 0
            Array.fill (this.Root :?> RRBFullNode<'T>).Children 0 Literals.blockSize null
            this.Count <- this.Count - idx
            this.Shift <- Literals.blockSizeShift
            this.TailOffset <- 0
            this :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            this.Root.SetNodeSize 0
            Array.fill (this.Root :?> RRBFullNode<'T>).Children 0 Literals.blockSize null
            let tailLen = this.Count - this.TailOffset
            let tailIdx = idx - this.TailOffset
            this.Count <- this.Count - idx
            this.Shift <- Literals.blockSizeShift
            // Shift remaining items down, then zero out the slots that have become empty
            for i = tailIdx to tailLen - 1 do
                this.Tail.[i - tailIdx] <- this.Tail.[i]
            Array.fill this.Tail (tailLen - tailIdx) tailIdx Unchecked.defaultof<'T>
            this.TailOffset <- 0
            this :> RRBVector<'T>
        else
            let newRoot = this.Root.SkipNTreeItems this.Owner this.Shift idx
            this.Count <- this.Count - idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.TailOffset <- this.TailOffset - idx
            this.AdjustTree()

    // abstract member Split : int -> RRBVector<'T> * RRBVector<'T>
    override this.Split idx =
        this.ThrowIfNotValid()
        // NOTE: Remember that in transients, we need to create the right vector with the same owner token
        // And "this" must remain the left vector. So when idx = 0, we can't just do "this.Empty()" as that
        // would erase the root. Instead, we must first copy the node and hand the copy to right to be its
        // root, or else hand our original node to right and make a brand-new empty node for "this". Don't
        // know yet which is better. If anyone had pointers to our original root... well, they shouldn't.
        // And if anyone had pointers to nodes further down in the tree, copying the root won't do any harm.
        this.EnsureValidIndexLengthAllowed idx
        if idx = this.Count then
            let right = RRBTransientVector<'T>(this.Owner)
            this :> RRBVector<'T>, right :> RRBVector<'T>
        elif idx = 0 then
            let right = RRBTransientVector<'T>(this.Count, this.Shift, this.Root, this.Tail, this.TailOffset)
            this.Count <- 0
            this.Shift <- Literals.blockSizeShift
            this.Root <- RRBExpandedFullNode<'T>(this.Owner, Array.zeroCreate Literals.blockSize, 0)
            this.Tail <- Array.zeroCreate Literals.blockSize
            this.TailOffset <- 0
            this :> RRBVector<'T>, right :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail for the left (this)
            let right = RRBTransientVector<'T>(this.Owner)
            right.Tail <- this.Tail
            right.Count <- this.Count - this.TailOffset
            let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
            this.TailOffset <- idx - newTailNode.NodeSize
            this.AdjustTree(), right :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailIdx = idx - this.TailOffset
            let tailLen = this.Count - this.TailOffset
            let right = RRBTransientVector<'T>(this.Owner)
            Array.blit this.Tail tailIdx right.Tail 0 (tailLen - tailIdx)
            Array.fill this.Tail tailIdx (tailLen - tailIdx) Unchecked.defaultof<'T>
            this.Count <- idx
            right.Count <- tailLen - tailIdx
            this :> RRBVector<'T>, right :> RRBVector<'T>
        else
            let rootL, rootR = this.Root.SplitTree this.Owner this.Shift idx
            let right = RRBTransientVector<'T>(this.Count - idx, this.Shift, rootR, this.Tail, this.TailOffset - idx)
            let newTailNodeL, newRootL = (rootL :?> RRBFullNode<'T>).PopLastLeaf rootL.Owner this.Shift
            this.Count <- idx
            if not <| isSameObj newRootL this.Root then
                this.Root <- newRootL
            this.Tail <- newTailNodeL.GetEditableArrayOfBlockSizeLength this.Owner
            this.TailOffset <- idx - newTailNodeL.NodeSize
            // Have to adjust the tree for both "this" AND "right" in this one, since either one could have become a tall, thin tree
            this.AdjustTree(), right.AdjustTree()

    // abstract member Slice : int * int -> RRBVector<'T>
    override this.Slice (start, stop) =
        this.ThrowIfNotValid()
        (this.Skip start).Take (stop - start + 1)

    // abstract member GetSlice : int option * int option -> RRBVector<'T>
    override this.GetSlice (start, stop) =
        this.ThrowIfNotValid()
        match start, stop with
        | None, None -> this :> RRBVector<'T>
        | None, Some stop -> this.Take (stop + 1)  // vec.[..5] should return all indices from 0 to 5, i.e. 6 in total
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice (start, stop)

    // abstract member Append : RRBVector<'T> -> RRBVector<'T>
    override this.Append other =
        this.ThrowIfNotValid()
        match other with
        | :? RRBTransientVector<'T> as right when this.Root.IsEditableBy right.Owner ->
            // We can only merge transient trees when they have the same owner, otherwise it's impossible to
            // tell which owner token to give to the new tree. Get it wrong, and there could be some persistent
            // trees out there with references to nodes with that owner token, and so we might end up editing
            // some nodes in-place even though a persistent tree has a reference to that node, which breaks
            // the entire promise of a persistent data structure. The only safe way to merge two trees and have
            // the result be transient is if they have the same owner token, which can only happen if they
            // came from the same original transient tree that was later split.
            if this.Count = 0 then
                // Calling code expects "this" to contain the results, so we steal everything from the right-hand tree
                this.Root <- right.Root
                this.Shift <- right.Shift
                this.Count <- right.Count
                this.TailOffset <- right.TailOffset
                this.Tail <- right.Tail
                right.Invalidate()
                this :> RRBVector<'T>
            elif right.Count = 0 then right.Invalidate(); this :> RRBVector<'T> else
            let newLen = this.Count + right.Count
            if right.TailOffset <= 0 then
                // Right is a tail-only vector
                let tailLenL = this.Count - this.TailOffset
                let tailLenR = right.Count - right.TailOffset
                if tailLenL + tailLenR <= Literals.blockSize then
                    Array.blit right.Tail 0 this.Tail tailLenL tailLenR
                    this.Count <- newLen
                    right.Invalidate()
                    this :> RRBVector<'T>
                else
                    let newTail = Array.zeroCreate Literals.blockSize
                    let splitIdx = Literals.blockSize - tailLenL
                    Array.blit right.Tail 0 this.Tail tailLenL splitIdx
                    Array.blit right.Tail splitIdx newTail 0 (tailLenR - splitIdx)
                    let newLeaf = RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>
                    this.Tail <- newTail
                    if this.TailOffset <= 0 then
                        // We CAN use AppendLeaf in an empty root!
                        let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift newLeaf
                        this.Count <- newLen
                        if not <| isSameObj newRoot this.Root then
                            this.Root <- newRoot
                        this.Shift <- newShift
                        this.TailOffset <- this.TailOffset + newLeaf.NodeSize
                        right.Invalidate()
                        this :> RRBVector<'T>
                    else
                        let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift newLeaf
                        this.Count <- newLen
                        if not <| isSameObj newRoot this.Root then
                            this.Root <- newRoot
                        this.Shift <- newShift
                        this.TailOffset <- this.TailOffset + newLeaf.NodeSize
                        right.Invalidate()
                        this :> RRBVector<'T>
                        // TODO: The "then" and "else" blocks here are identical. Write test to make sure we can append into a tail-only left transient, then collapse this "if" into a single copy of this block.
            elif this.TailOffset <= 0 then
                // Right has a root and a tail, but we're a tail-only node
                let tailLen = this.Count - this.TailOffset
                let tailNode =
                    if tailLen < Literals.blockSize
                    then RRBNode<'T>.MkLeaf this.Owner (this.Tail |> Array.truncate tailLen) :?> RRBLeafNode<'T>
                    else RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>
                let newRoot, newShift = (right.Root :?> RRBFullNode<'T>).PrependLeaf right.Owner right.Shift tailNode
                if not <| isSameObj newRoot right.Root then
                    right.Root <- newRoot
                // Calling code expects "this" to contain the results, so we steal everything from the right-hand tree
                this.Root <- right.Root
                this.Shift <- newShift
                this.Count <- newLen
                this.TailOffset <- right.TailOffset + tailNode.NodeSize
                this.Tail <- right.Tail
                right.Invalidate()
                this :> RRBVector<'T>
            else
                // Right has a root and a tail, and so do we
                let tailLen = this.Count - this.TailOffset
                let tailNode =
                    if tailLen < Literals.blockSize
                    then RRBNode<'T>.MkLeaf this.Owner (this.Tail |> Array.truncate tailLen) :?> RRBLeafNode<'T>
                    else RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>
                // Can the tail be merged into the two twig nodes? Now's the time to find out, while we can still push it down to form a new root
                let tailCanFit = ((this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift).HasRoomToMergeTheTail Literals.blockSizeShift tailNode ((right.Root :?> RRBFullNode<'T>).LeftmostTwig right.Shift)
                let newRoot, newShift =
                    let mergedTree =
                        if tailCanFit
                        then (this.Root :?> RRBFullNode<'T>).MergeTree this.Owner this.Shift (Some tailNode) right.Shift (right.Root :?> RRBFullNode<'T>) true
                        else
                            let tmpRoot, tmpShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift tailNode
                            (tmpRoot :?> RRBFullNode<'T>).MergeTree this.Owner tmpShift None right.Shift (right.Root :?> RRBFullNode<'T>) true
                    match mergedTree with
                    | newRoot, None ->
                        newRoot, (max this.Shift right.Shift)
                    | newLeft, Some newRight ->
                        let oldShift = max this.Shift right.Shift
                        // NOTE: Here we really do want newRight to be the instance on which we call NewParent.
                        // We've already shrunk newLeft's inside MergeTree, so newLeft is no longer an expanded node by now.
                        let newRoot = (newRight :?> RRBFullNode<'T>).NewParent this.Owner oldShift [|newLeft; newRight|]
                        newRoot, (RRBMath.up oldShift)
                this.TailOffset <- this.Count + right.TailOffset
                this.Count <- newLen
                this.Shift <- newShift
                this.Root <- newRoot
                this.Tail <- right.Tail
                right.Invalidate()
                this.AdjustTree()
        // Transient vectors may only stay transient if appended to a transient of the same owner
        | :? RRBTransientVector<'T> as right ->
            this.Persistent().Append (right.Persistent())
        | :? RRBPersistentVector<'T> as right ->
            this.Persistent().Append right
        | _ ->
            this.Persistent().Append other

    // abstract member Insert : int -> 'T -> RRBVector<'T>
    override this.Insert idx newItem =
        this.ThrowIfNotValid()
        this.EnsureValidIndexLengthAllowed idx
        if idx >= this.TailOffset then
            let tailLen = this.Count - this.TailOffset
            if tailLen < Literals.blockSize then
                let tailIdx = idx - this.TailOffset
                for i = tailLen - 1 downto tailIdx do
                    this.Tail.[i+1] <- this.Tail.[i]
                this.Tail.[tailIdx] <- newItem
                this.Count <- this.Count + 1
                this :> RRBVector<'T>
            else
                let newLeafItems, newTailItem = this.Tail |> Array.copyAndInsertIntoFullArray (idx - this.TailOffset) newItem
                let newTail = Array.zeroCreate Literals.blockSize
                newTail.[0] <- newTailItem
                let newLeafNode = RRBNode<'T>.MkLeaf this.Owner newLeafItems :?> RRBLeafNode<'T>
                let newRoot, newShift = (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift newLeafNode
                if not <| isSameObj newRoot this.Root then
                    this.Root <- newRoot
                this.Shift <- newShift
                this.TailOffset <- this.Count
                this.Count <- this.Count + 1
                this.Tail <- newTail
                // Pushing a full tail down into a leaf can't break the invariant, so no need to adjust the tree here
                this :> RRBVector<'T>
        else
            let newRoot, newShift =
                match this.Root.InsertedTree this.Owner this.Shift idx newItem None 0 with
                | SimpleInsertion(newCurrent) -> newCurrent, this.Shift
                | SplitNode(newCurrent, newRight) -> (newRight :?> RRBFullNode<'T>).NewParent this.Owner this.Shift [|newCurrent; newRight|], (RRBMath.up this.Shift)
                // Was: | SplitNode(newCurrent, newRight) -> (newCurrent :?> RRBFullNode<'T>).NewParent this.Owner this.Shift [|newCurrent; newRight|], (RRBMath.up this.Shift)
                // (Note "newCurrent", not newRight, as instance on which NewParent is called)
                | SlidItemsLeft(newLeft, newCurrent) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
                | SlidItemsRight(newCurrent, newRight) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.Shift <- newShift
            this.Count <- this.Count + 1
            this.TailOffset <- this.TailOffset + 1
            this.AdjustTree()  // TODO: Remove this AdjustTree() call and prove that tests fail when we do that (inserting into last leaf could cause invariant failure)

    // abstract member Remove : int -> RRBVector<'T>
    override this.Remove idx =
        this.RemoveImpl idx true

    member internal this.RemoveWithoutRebalance idx =
        this.RemoveImpl idx false

    member internal this.RemoveImpl idx shouldCheckForRebalancing =
        this.ThrowIfNotValid()
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            let tailLen = this.Count - this.TailOffset
            if tailLen > 1 then
                let tailIdx = idx - this.TailOffset
                for i = tailIdx to tailLen - 2 do
                    this.Tail.[i] <- this.Tail.[i + 1]
                this.Tail.[tailLen - 1] <- Unchecked.defaultof<'T>
                this.Count <- this.Count - 1
                this :> RRBVector<'T>
            elif this.Count = 1 then
                this.Empty()
            else
                // Tail is now empty, so promote a new tail
                let newTailNode, newRoot = (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift
                newTailNode.Items.CopyTo(this.Tail, 0)
                Array.fill this.Tail (newTailNode.NodeSize) (tailLen - newTailNode.NodeSize |> max 0) Unchecked.defaultof<'T>
                if not <| isSameObj newRoot this.Root then
                    this.Root <- newRoot
                this.Count <- this.Count - 1
                this.TailOffset <- this.TailOffset - newTailNode.NodeSize
                this.AdjustTree()
        else
            let newRoot = this.Root.RemovedItem this.Owner this.Shift shouldCheckForRebalancing idx
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
            this.Count <- this.Count - 1
            this.TailOffset <- this.TailOffset - 1
            this.AdjustTree()

    // abstract member Update : int -> 'T -> RRBVector<'T>
    override this.Update idx newItem =
        this.ThrowIfNotValid()
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            this.Tail.[idx - this.TailOffset] <- newItem
        else
            let newRoot = this.Root.UpdatedTree this.Owner this.Shift idx newItem
            if not <| isSameObj newRoot this.Root then
                this.Root <- newRoot
        this :> RRBVector<'T>

    // abstract member GetItem : int -> 'T
    override this.GetItem idx =
        this.ThrowIfNotValid()
        this.EnsureValidIndex idx
        if idx >= this.TailOffset then
            tail.[idx - this.TailOffset]
        else
            this.Root.GetTreeItem this.Shift idx

    member this.EnsureValidIndex idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx >= this.Count then failwith "Index must not be past the end of the vector"
        else ()

    member this.EnsureValidIndexLengthAllowed idx =
        if idx < 0 then failwith "Index must not be negative"
        elif idx > this.Count then failwith "Index must not be more than one past the end of the vector"
        else ()

let internal isTransient (vec : RRBVector<'T>) = vec :? RRBTransientVector<'T>

// TODO: This module is currently fully-named as Ficus.RRBVector.RRBVectorModule; is that actually what I want? Or should I
// change the name of the second part of that name so it's something like Ficus.Vectors.RRBVectorModule?
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

    let inline empty<'T> = RRBPersistentVector<'T>.MkEmpty() :> RRBVector<'T>

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
        let mutable transient = RRBTransientVector<'T>.MkEmpty()
        for item in s do
            transient <- transient.Push item :?> RRBTransientVector<'T>
        transient.Persistent() :> RRBVector<'T>
    let ofArray (a : 'T[]) =
        if a.Length <= Literals.blockSize then
            let tail = Array.copy a
            RRBPersistentVector<'T>(a.Length, Literals.blockSizeShift, emptyNode, tail, 0) :> RRBVector<'T>
        elif a.Length <= Literals.blockSize * 2 then
            let leaf, tail = a |> Array.splitAt Literals.blockSize
            RRBPersistentVector<'T>(a.Length, Literals.blockSizeShift, [|RRBNode<'T>.MkLeaf nullOwner leaf|] |> RRBNode<'T>.MkFullNode nullOwner, tail, Literals.blockSize) :> RRBVector<'T>
            // TODO: Perhaps this should be a static member of RRBVector, called FromArray? Might be nice to have this in the C# API
        else
            a |> ofSeq
    let inline ofList (l : 'T list) =
        let mutable transient = RRBTransientVector<'T>.MkEmpty()
        for item in l do
            transient <- transient.Push item :?> RRBTransientVector<'T>
        transient.Persistent() :> RRBVector<'T>

    // TODO: Try improving average and averageBy by using iterLeafArrays(), summing up each array, and then dividing by count at the end. MIGHT be faster than Seq.average.
    let inline average (vec : RRBVector<'T>) = vec |> Seq.average
    let inline averageBy f (vec : RRBVector<'T>) = vec |> Seq.averageBy f
    let choose (chooser : 'T -> 'U option) (vec : RRBVector<'T>) : RRBVector<'U> =
        // TODO: Might be able to consolidate most of this (as we did in RRBVector.except), as only the first and last lines really differ. Ditto for rest of "if vec |> isTransient" cases
        if vec |> isTransient then
            let mutable result = RRBTransientVector<'U>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            for item in vec do
                match chooser item with
                | None -> ()
                | Some value -> result <- result.Push value :?> RRBTransientVector<_>
            result :> RRBVector<_>
        else
            let mutable transient = RRBTransientVector<'U>.MkEmpty()
            for item in vec do
                match chooser item with
                | None -> ()
                | Some value -> transient <- transient.Push value :?> RRBTransientVector<_>
            transient.Persistent() :> RRBVector<_>
    // Alternate version (for persistent vectors only). TODO: Benchmark
    let chooseAlt (chooser : 'T -> 'U option) (vec : RRBVector<'T>) : RRBVector<'U> =
        if vec |> isTransient then failwith "DEBUG: chooseAlt only implemented for persistent vectors"
        vec |> Seq.choose chooser |> ofSeq

    let chunkBySize chunkSize (vec : RRBVector<'T>) =
        if chunkSize <= 0 && vec.Length > 0 then failwith "Chunk size must be greater than zero"
        if vec |> isTransient then
            let mutable result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            let mutable remaining = vec
            while remaining.Length > 0 do
                let chunk, rest = remaining.Split (min chunkSize remaining.Length)
                result <- result.Push chunk :?> RRBTransientVector<_>
                remaining <- rest
            result :> RRBVector<_>
        else
            let mutable transient = RRBTransientVector<_>.MkEmpty()
            let mutable remaining = vec
            while remaining.Length > 0 do
                let chunk, rest = remaining.Split (min chunkSize remaining.Length)
                transient <- transient.Push chunk :?> RRBTransientVector<_>
                remaining <- rest
            transient.Persistent() :> RRBVector<_>

    let concat (vecs : seq<RRBVector<'T>>) =
        // TODO: Benchmark this and see if it really is all that much faster, considering the complications inherent in concatenating transients
        if vecs |> Seq.isEmpty then
            empty<'T>
        elif (vecs |> Seq.head) :? RRBTransientVector<'T> then
            let t = (vecs |> Seq.head) :?> RRBTransientVector<'T>
            let token = t.Owner
            if (vecs |> Seq.forall (fun v -> v :? RRBTransientVector<'T> && (v :?> RRBTransientVector<'T>).Owner = token)) then
                let mutable result = t
                for vec in Seq.tail vecs do
                    result <- result.Append vec :?> RRBTransientVector<'T>
                result :> RRBVector<'T>
            else
                let mutable result = RRBPersistentVector<'T>.MkEmpty() :> RRBVector<'T>
                for vec in vecs do
                    result <- result.Append vec
                result
        else
            let mutable result = RRBPersistentVector<'T>.MkEmpty() :> RRBVector<'T>
            for vec in vecs do
                result <- result.Append vec
            result

    // TODO FIXME: Find everywhere where we make new transient vectors, and whenever possible make them take the owner token from the original vector

    let inline collect (f : 'T -> RRBVector<'T>) (vec : RRBVector<'T>) =
        // TODO: Benchmark the following two options, because I have no idea which is slower.
        // Option 1, the merging version
        vec |> Seq.map f |> concat

        // Option 2, the one-at-a-time version
        // let mutable transient = RRBTransientVector.MkEmpty()
        // for src in vec do
        //     for item in f src do
        //         transient <- transient.Push item :?> RRBTransientVector<'T>
        // transient.Persistent() :> RRBVector<'T>

    let inline compareWith f (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) = (vec1, vec2) ||> Seq.compareWith f

    let countBy f (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            for resultItem in vec |> Seq.countBy f do
                result.Push resultItem |> ignore
            result :> RRBVector<_>
        else
            vec |> Seq.countBy f |> ofSeq

    let inline contains item (vec : RRBVector<'T>) = vec |> Seq.contains item

    let distinct (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            for resultItem in vec |> Seq.distinct do
                result.Push resultItem |> ignore
            result :> RRBVector<_>
        else
            vec |> Seq.distinct |> ofSeq

    let distinctBy f (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            for resultItem in vec |> Seq.distinctBy f do
                result.Push resultItem |> ignore
            result :> RRBVector<_>
        else
            vec |> Seq.distinctBy f |> ofSeq

    let exactlyOne (vec : RRBVector<'T>) =
        if vec.Length <> 1 then invalidArg "vec" <| sprintf "exactlyOne called on a vector of %d items (requires a vector of exactly 1 item)" vec.Length
        vec.Peek()

    let except (vec : RRBVector<'T>) (excludedVec : RRBVector<'T>) =
        let excludedSet = System.Collections.Generic.HashSet<'T>(excludedVec)
        let mutable transient =
            if vec |> isTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        for item in vec do
            if not (excludedSet.Contains item) then transient <- transient.Push item :?> RRBTransientVector<'T>
        if vec |> isTransient then transient :> RRBVector<'T> else transient.Persistent() :> RRBVector<'T>

    let inline exists f (vec : RRBVector<'T>) = vec |> Seq.exists f
    let inline exists2 f (vec1 : RRBVector<'T>) (vec2 : RRBVector<'U>) = (vec1, vec2) ||> Seq.exists2 f

    let filter pred (vec : RRBVector<'T>) =
        let mutable transient =
            if vec |> isTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        for item in vec do
            if pred item then transient <- transient.Push item :?> RRBTransientVector<'T>
        if vec |> isTransient then transient :> RRBVector<'T> else transient.Persistent() :> RRBVector<'T>

    let filteri pred (vec : RRBVector<'T>) =
        let mutable transient =
            if vec |> isTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        let mutable i = 0
        for item in vec do
            if pred i item then transient <- transient.Push item :?> RRBTransientVector<'T>
            i <- i + 1
        if vec |> isTransient then transient :> RRBVector<'T> else transient.Persistent() :> RRBVector<'T>

    let filter2 pred (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && isSameObj (vec1 :?> RRBTransientVector<'T>).Owner (vec2 :?> RRBTransientVector<'T>).Owner
        let mutable transient =
            if resultShouldBeTransient
            then RRBTransientVector<'T * 'T>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T * 'T>.MkEmpty()
        for item1, item2 in Seq.zip vec1 vec2  do
            if pred item1 item2 then transient <- transient.Push (item1, item2) :?> RRBTransientVector<'T * 'T>
        if resultShouldBeTransient then transient :> RRBVector<_> else transient.Persistent() :> RRBVector<_>

    let filteri2 pred (vec1 : RRBVector<'T>) (vec2 : RRBVector<'T>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && isSameObj (vec1 :?> RRBTransientVector<'T>).Owner (vec2 :?> RRBTransientVector<'T>).Owner
        let mutable transient =
            if resultShouldBeTransient
            then RRBTransientVector<'T * 'T>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T * 'T>.MkEmpty()
        let mutable i = 0
        for item1, item2 in Seq.zip vec1 vec2  do
            if pred i item1 item2 then transient <- transient.Push (item1, item2) :?> RRBTransientVector<'T * 'T>
            i <- i + 1
        if resultShouldBeTransient then transient :> RRBVector<_> else transient.Persistent() :> RRBVector<_>

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

    let groupBy f (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            for resultItem in vec |> Seq.groupBy f do
                result.Push resultItem |> ignore
            result :> RRBVector<_>
        else
            vec |> Seq.groupBy f |> ofSeq

    let head (vec : RRBVector<'T>) =
        if vec.Length = 0 then invalidArg "vec" "Can't get head of empty vector"
        vec.[0]

    let indexed (vec : RRBVector<'T>) =
        let mutable transient =
            if vec |> isTransient
            then RRBTransientVector<int * 'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<int * 'T>.MkEmpty()
        let mutable i = 0
        for item in vec do
            transient.Push (i, item) |> ignore
            i <- i + 1
        if vec |> isTransient then
            transient :> RRBVector<_>
        else
            transient.Persistent() :> RRBVector<_>

    //  vec |> Seq.indexed |> RRBHelpers.buildTreeOfSeqWithKnownSize (ref null) vec.Length
    let inline init size f = Seq.init size f |> ofSeq
    let inline isEmpty (vec : RRBVector<'T>) = vec.IsEmpty()
    let inline item idx (vec : RRBVector<'T>) = vec.[idx]
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


    let map f (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            for item in vec do
                result.Push (f item) |> ignore
            result :> RRBVector<_>
        else
            vec |> Seq.map f |> ofSeq
            // TODO: Benchmark the Seq.map version (the else block here) and see if it would be faster to use the same "unrolled" version we use for transients

    let map2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && isSameObj (vec1 :?> RRBTransientVector<'T1>).Owner (vec2 :?> RRBTransientVector<'T2>).Owner
        if resultShouldBeTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            while iter1.MoveNext() && iter2.MoveNext() do
                result.Push (f iter1.Current iter2.Current) |> ignore
            result :> RRBVector<_>
        else
            Seq.map2 f vec1 vec2 |> ofSeq
            // TODO: Ditto re: benchmark for Seq.map

    let map3 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) (vec3 : RRBVector<'T3>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && vec3 |> isTransient
                                      && isSameObj (vec1 :?> RRBTransientVector<'T1>).Owner (vec2 :?> RRBTransientVector<'T2>).Owner
                                      && isSameObj (vec2 :?> RRBTransientVector<'T2>).Owner (vec3 :?> RRBTransientVector<'T3>).Owner
        if resultShouldBeTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            use iter3 = vec3.IterItems().GetEnumerator()
            while iter1.MoveNext() && iter2.MoveNext() && iter3.MoveNext() do
                result.Push (f iter1.Current iter2.Current iter3.Current) |> ignore
            result :> RRBVector<_>
        else
            Seq.map3 f vec1 vec2 vec3 |> ofSeq
            // TODO: Ditto re: benchmark for Seq.map

    let mapi f (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            let mutable i = 0
            for item in vec do
                result.Push (f i item) |> ignore
                i <- i + 1
            result :> RRBVector<_>
        else
            vec |> Seq.mapi f |> ofSeq
            // TODO: Ditto re: benchmark for Seq.map

    let mapi2 f (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && isSameObj (vec1 :?> RRBTransientVector<'T1>).Owner (vec2 :?> RRBTransientVector<'T2>).Owner
        if resultShouldBeTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
            let mutable i = 0
            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            while iter1.MoveNext() && iter2.MoveNext() do
                result.Push (f i iter1.Current iter2.Current) |> ignore
                i <- i + 1
            result :> RRBVector<_>
        else
            Seq.mapi2 f vec1 vec2 |> ofSeq
            // TODO: Ditto re: benchmark for Seq.map

    let mapFold folder initState (vec : RRBVector<'T>) =
        if isEmpty vec then vec else
        let resultShouldBeTransient = vec |> isTransient
        let mutable transient =
            if resultShouldBeTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        let mutable state = initState
        for item in vec do
            let item',state' = folder state item
            transient.Push item' |> ignore
            state <- state'
        if resultShouldBeTransient then transient :> RRBVector<_> else transient.Persistent() :> RRBVector<_>

    let mapFoldBack folder (vec : RRBVector<'T>) initState =
        if isEmpty vec then vec else
        let resultShouldBeTransient = vec |> isTransient
        let mutable transient =
            if resultShouldBeTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        let mutable state = initState
        for item in vec.RevIterItems() do
            let item',state' = folder item state
            transient <- transient.Push item' :?> RRBTransientVector<'T>
            state <- state'
        if resultShouldBeTransient then transient :> RRBVector<_> else transient.Persistent() :> RRBVector<_>

    let inline max (vec : RRBVector<'T>) = vec |> Seq.max
    let inline maxBy f (vec : RRBVector<'T>) = vec |> Seq.maxBy f
    let inline min (vec : RRBVector<'T>) = vec |> Seq.min
    let inline minBy f (vec : RRBVector<'T>) = vec |> Seq.minBy f

    let pairwise (vec : RRBVector<'T>) =
        if vec |> isTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            use iter1 = vec.IterItems().GetEnumerator()
            use iter2 = vec.IterItems().GetEnumerator()
            if not (iter2.MoveNext()) then
                // Input vector was empty
                result :> RRBVector<_>
            else
                while iter1.MoveNext() && iter2.MoveNext() do
                    result.Push (iter1.Current, iter2.Current) |> ignore
                result :> RRBVector<_>
        else
            vec |> Seq.pairwise |> ofSeq
            // TODO: Ditto re: benchmark for Seq.map

    let partition pred (vec : RRBVector<'T>) =
        let resultShouldBeTransient = vec |> isTransient
        let mutable trueItems =
            if resultShouldBeTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        let mutable falseItems =
            if resultShouldBeTransient
            then RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
            else RRBTransientVector<'T>.MkEmpty()
        for item in vec do
            if pred item then trueItems <- trueItems.Push item :?> RRBTransientVector<'T> else falseItems <- falseItems.Push item :?> RRBTransientVector<'T>
        if resultShouldBeTransient then
            trueItems :> RRBVector<'T>, falseItems :> RRBVector<'T>
        else
            trueItems.Persistent() :> RRBVector<'T>, falseItems.Persistent() :> RRBVector<'T>

    let permute f (vec : RRBVector<'T>) = // TODO: Implement a better version once we have transient RRBVectors, so we don't have to build an intermediate array
        let arr = Array.zeroCreate vec.Length
        let seen = Array.zeroCreate vec.Length
        let items = (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()
        let mutable i = 0
        while items.MoveNext() do
            let newIdx = f i
            arr.[newIdx] <- items.Current
            seen.[newIdx] <- 1uy
            i <- i + 1
        for i = 0 to seen.Length - 1 do
            if seen.[i] <> 1uy then invalidArg "f" "The function did not compute a permutation."
        if vec |> isTransient then
            // Now mutate the transient in-place
            for i = 0 to arr.Length - 1 do
                vec.Update i arr.[i] |> ignore
            vec :> RRBVector<_>
        else
            arr |> ofArray

    let inline pick f (vec : RRBVector<'T>) = vec |> Seq.pick f
    let inline reduce f (vec : RRBVector<'T>) = vec |> Seq.reduce f
    let reduceBack f (vec : RRBVector<'T>) = let f' = flip f in vec.RevIterItems() |> Seq.reduce f'

    let replicate count value =
        if count = 0 then empty<'T> else
        let mutable transient = RRBTransientVector<'T>.MkEmpty()
        for i = 1 to count do
            transient <- transient.Push value :?> RRBTransientVector<'T>
        transient.Persistent() :> RRBVector<'T>

    let rev (vec : RRBVector<'T>) =
        if isEmpty vec then vec else
        if vec |> isTransient then
            // Reverse by swapping in-place. TODO: Benchmark and see if this is faster than creating a new copy
            let lastIdx = vec.Length - 1
            let mid = vec.Length >>> 1
            for i = 0 to mid - 1 do
                let j = lastIdx - i
                let oldL = vec.[i]
                let oldR = vec.[j]
                vec.Update i oldR |> ignore
                vec.Update j oldL |> ignore
            vec
        else
            let mutable transient = RRBTransientVector<'T>.MkEmpty()
            for item in vec.RevIterItems() do
                transient <- transient.Push item :?> RRBTransientVector<'T>
            transient.Persistent() :> RRBVector<'T>

    // TODO: Make a variant for when f is of type 'a -> 'a, which does a scan in-place for transients
    let inline scan f initState (vec : RRBVector<'T>) = vec |> Seq.scan f initState |> ofSeq
    let scanBack initState f (vec : RRBVector<'T>) =
        let f' = flip f
        vec.RevIterItems() |> Seq.scan f' initState |> ofSeq

    let singleton (item : 'T) = RRBPersistentVector<'T>(1, Literals.blockSizeShift, emptyNode, [|item|], 0) :> RRBVector<'T>

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
    let splitInto splitCount (vec : RRBVector<'T>) =
        let extra = if vec.Length % splitCount = 0 then 0 else 1
        let mutable result = chunkBySize (vec.Length / splitCount + extra) vec  // TODO: Test that splits have the expected size
        if result.Length < splitCount then
            if result |> isTransient then
                let token = (result :?> RRBTransientVector<_>).Owner
                for i = 1 to splitCount - result.Length do
                    result <- result.Push(RRBTransientVector<'T>.MkEmptyWithToken(token))
            else
                for i = 1 to splitCount - result.Length do
                    result <- result.Push(RRBPersistentVector<'T>.MkEmpty())
            result
        else
            result

    let inline sum (vec : RRBVector<'T>) = vec |> Seq.sum
    let inline sumBy f (vec : RRBVector<'T>) = vec |> Seq.sumBy f
    let inline tail (vec : RRBVector<'T>) = vec.Remove 0
    let inline take n (vec : RRBVector<'T>) =
        // TODO: Document that RRBVector.take never throws, it just returns the whole vector if you take too much
        // if n > vec.Length then invalidArg "n" <| sprintf "Cannot take more items than a vector's length. Tried to take %d items from a vector of length %d" n vec.Length
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
        let resultShouldBeTransient = vec |> isTransient
        let mutable vec1 =
            if resultShouldBeTransient
            then RRBTransientVector<'T1>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
            else RRBTransientVector<'T1>.MkEmpty()
        let mutable vec2 =
            if resultShouldBeTransient
            then RRBTransientVector<'T2>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
            else RRBTransientVector<'T2>.MkEmpty()
        for a, b in vec do
            vec1 <- vec1.Push a :?> RRBTransientVector<_>
            vec2 <- vec2.Push b :?> RRBTransientVector<_>
        if resultShouldBeTransient then
            vec1 :> RRBVector<_>, vec2 :> RRBVector<_>
        else
            vec1.Persistent() :> RRBVector<_>, vec2.Persistent() :> RRBVector<_>

    let unzip3 (vec : RRBVector<'T1 * 'T2 * 'T3>) =
        let resultShouldBeTransient = vec |> isTransient
        let mutable vec1 =
            if resultShouldBeTransient
            then RRBTransientVector<'T1>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
            else RRBTransientVector<'T1>.MkEmpty()
        let mutable vec2 =
            if resultShouldBeTransient
            then RRBTransientVector<'T2>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
            else RRBTransientVector<'T2>.MkEmpty()
        let mutable vec3 =
            if resultShouldBeTransient
            then RRBTransientVector<'T3>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
            else RRBTransientVector<'T3>.MkEmpty()
        for a, b, c in vec do
            vec1 <- vec1.Push a :?> RRBTransientVector<_>
            vec2 <- vec2.Push b :?> RRBTransientVector<_>
            vec3 <- vec3.Push c :?> RRBTransientVector<_>
        if resultShouldBeTransient then
            vec1 :> RRBVector<_>, vec2 :> RRBVector<_>, vec3 :> RRBVector<_>
        else
            vec1.Persistent() :> RRBVector<_>, vec2.Persistent() :> RRBVector<_>, vec3.Persistent() :> RRBVector<_>

    let inline where pred (vec : RRBVector<'T>) = filter pred vec

    let windowedSeq windowSize (vec : RRBVector<'T>) =
        // Sequence of vectors that share as much structure as possible with the original vector and with each other
        if vec.Length < windowSize then Seq.empty else
        if windowSize <= Literals.blockSize then
            // Sequence of tail-only vectors
            seq {
                let mutable tail = Array.zeroCreate windowSize
                let mutable count = 0
                let itemEnumerator = (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()
                while count < windowSize && itemEnumerator.MoveNext() do
                    tail.[count] <- itemEnumerator.Current
                    count <- count + 1
                yield RRBPersistentVector<'T>(windowSize, Literals.blockSizeShift, emptyNode, tail, 0) :> RRBVector<'T>
                while itemEnumerator.MoveNext() do
                    tail <- tail |> Array.popFirstAndPush itemEnumerator.Current
                    yield RRBPersistentVector<'T>(windowSize, Literals.blockSizeShift, emptyNode, tail, 0) :> RRBVector<'T>
            }
        else
            seq {
                let mutable slidingVec = vec.Take windowSize
                let rest = vec.Skip windowSize
                yield slidingVec
                for item in rest do
                    slidingVec <- (slidingVec :?> RRBPersistentVector<'T>).RemoveWithoutRebalance 0
                    slidingVec <- slidingVec.Push item
                    yield slidingVec
            }

    let windowed windowSize (vec : RRBVector<'T>) =
        // Vector of vectors that share as much structure as possible with the original vector and with each other
        if vec.Length < windowSize
        then RRBPersistentVector<RRBVector<'T>>.MkEmpty() :> RRBVector<RRBVector<'T>>
        else windowedSeq windowSize vec |> ofSeq

    let zip (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && isSameObj (vec1 :?> RRBTransientVector<'T1>).Owner (vec2 :?> RRBTransientVector<'T2>).Owner
        if resultShouldBeTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            while iter1.MoveNext() && iter2.MoveNext() do
                result.Push (iter1.Current, iter2.Current) |> ignore
            result :> RRBVector<_>
        else
            Seq.zip vec1 vec2 |> ofSeq
            // TODO: Benchmark just like with Seq.map

    let zip3 (vec1 : RRBVector<'T1>) (vec2 : RRBVector<'T2>) (vec3 : RRBVector<'T3>) =
        let resultShouldBeTransient = vec1 |> isTransient && vec2 |> isTransient && vec3 |> isTransient
                                      && isSameObj (vec1 :?> RRBTransientVector<'T1>).Owner (vec2 :?> RRBTransientVector<'T2>).Owner
                                      && isSameObj (vec2 :?> RRBTransientVector<'T2>).Owner (vec3 :?> RRBTransientVector<'T3>).Owner
        if resultShouldBeTransient then
            let result = RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            use iter3 = vec3.IterItems().GetEnumerator()
            while iter1.MoveNext() && iter2.MoveNext() && iter3.MoveNext() do
                result.Push (iter1.Current, iter2.Current, iter3.Current) |> ignore
            result :> RRBVector<_>
        else
            Seq.zip3 vec1 vec2 vec3 |> ofSeq
            // TODO: Benchmark just like with Seq.map
