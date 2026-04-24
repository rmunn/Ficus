/// Relaxed Radix Balanced Vector
///
/// Original concept: Phil Bagwell and Tiark Rompf
/// https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf
///
/// Partly based on work by Jean Niklas L'orange: http://hypirion.com/thesis

module Ficus.RRBVectorTrees

open System.Threading
open RRBArrayExtensions
open RRBVectorNodes

#nowarn "FS3264"

[<AbstractClass>]
[<StructuredFormatDisplay("{StringRepr}")>]
type RRBVector<'T>() =
    /// Creates an empty vector
    abstract member Empty: unit -> RRBVector<'T>
    /// Tests whether vector is empty. <b>O(1)</b>
    abstract member IsEmpty: unit -> bool
    abstract member StringRepr: string
    /// Number of items in vector. <b>O(1)</b>
    abstract member Length: int
    abstract member IterLeaves: unit -> seq<'T[]>
    abstract member RevIterLeaves: unit -> seq<'T[]>
    abstract member IterItems: unit -> seq<'T>
    abstract member RevIterItems: unit -> seq<'T>
    // abstract member GetEnumerator : unit -> IEnumerator<'T>
    abstract member Push: 'T -> RRBVector<'T>
    abstract member Peek: unit -> 'T
    abstract member Pop: unit -> RRBVector<'T>
    abstract member Take: int -> RRBVector<'T>
    abstract member Skip: int -> RRBVector<'T>
    /// Split vector into two at index <code>i</code>. The left vector will be length <code>i</code> and
    /// contain items 0 through i-1, while the right vector will be length <code>x.Length - i</code> and
    /// contain items i through x.Length-1. <b>Effectively O(1)</b> (really O(log<sub>32</sub> N))
    abstract member Split: int -> RRBVector<'T> * RRBVector<'T>
    abstract member Slice: int * int -> RRBVector<'T>
    abstract member GetSlice: int option * int option -> RRBVector<'T>
    /// Concatenate two vectors to create a new one. <b>Effectively O(1)</b> (really O(log<sub>32</sub> N))
    abstract member Append: RRBVector<'T> -> RRBVector<'T>
    abstract member Insert: int -> 'T -> RRBVector<'T>
    abstract member Remove: int -> RRBVector<'T>
    abstract member Update: int -> 'T -> RRBVector<'T>
    abstract member GetItem: int -> 'T

    abstract member Transient: unit -> RRBVector<'T>
    abstract member Persistent: unit -> RRBVector<'T>

    interface System.Collections.Generic.IEnumerable<'T> with
        member this.GetEnumerator() = this.IterItems().GetEnumerator()

    interface System.Collections.IEnumerable with
        member this.GetEnumerator() =
            (this.IterItems().GetEnumerator()) :> System.Collections.IEnumerator

    member this.Item
        with get idx = this.GetItem idx

type internal IRRBInternal<'T> =
    abstract member InsertIntoTail: int -> 'T -> RRBVector<'T>
    abstract member RemoveFromTailAtTailIdx: int -> RRBVector<'T>
    abstract member RemoveImpl: bool -> int -> RRBVector<'T>
    abstract member RemoveWithoutRebalance: int -> RRBVector<'T>

type internal RRBPersistentVector<'T>
    internal (count, shift: int, root: RRBNode<'T>, tail: 'T[], tailOffset: int) =
    // TODO: Consider specifying that the root must always be an RRBFullNode<'T>, so we don't have to do nearly as many casts
    inherit RRBVector<'T>()

    member this.Count = count
    member this.Shift = shift
    member this.Root = root
    member this.Tail = tail
    member this.TailOffset = tailOffset

    override this.ToString() =
        sprintf
            "RRBPersistentVector<length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>"
            count
            shift
            tailOffset
            root
            tail

    internal new(token: OwnerToken) =
        let root = RRBFullNode<'T>(token, Array.empty)
        RRBPersistentVector<'T>(0, Literals.shiftSize, root, Array.empty, 0)

    new() = RRBPersistentVector<'T>(0, Literals.shiftSize, emptyNode, Array.empty, 0)

    static member MkEmpty() = RRBPersistentVector<'T>()
    static member internal MkEmptyWithToken token = RRBPersistentVector<'T>(token)

    override this.Transient() =
        let newToken = mkOwnerToken ()
        let newRoot = (this.Root :?> RRBFullNode<'T>).ExpandRightSpine newToken this.Shift
        let newTail = Array.zeroCreate Literals.blockSize
        this.Tail.CopyTo(newTail, 0)

        RRBTransientVector<'T>(this.Count, this.Shift, newRoot, newTail, this.TailOffset)
        :> RRBVector<'T>

    override this.Persistent() = this :> RRBVector<'T>

    member internal this.AdjustTree() =
        let v: RRBVector<'T> = this.ShiftNodesFromTailIfNeeded()
        (v :?> RRBPersistentVector<'T>).ShortenTree()

    member internal this.ShortenTree() =
        if
            this.Shift
            <= Literals.shiftSize
        then
            this :> RRBVector<'T>
        else if this.Root.NodeSize > 1 then
            this :> RRBVector<'T>
        elif this.Root.NodeSize = 1 then
            RRBPersistentVector<'T>(
                this.Count,
                RRBMath.down this.Shift,
                (this.Root :?> RRBFullNode<'T>).FirstChild,
                this.Tail,
                this.TailOffset
            )
                .ShortenTree()
        else // Empty root
            RRBPersistentVector<'T>(
                this.Count,
                Literals.shiftSize,
                emptyNode,
                this.Tail,
                this.TailOffset
            )
            :> RRBVector<'T>

    member internal this.ShiftNodesFromTailIfNeeded() =
        if
            this.TailOffset
            <= 0
            || this.Root.NodeSize = 0
        then
            // Empty root, so no need to shift any nodes
            this :> RRBVector<'T>
        else
            let lastTwig = (this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift
            // If parent of last leaf is a relaxed node, this automatically satisfies the invariant
            if lastTwig :? RRBRelaxedNode<'T> then
                this :> RRBVector<'T>
            else
                let lastLeaf = lastTwig.LastChild :?> RRBLeafNode<'T>

                let shiftCount =
                    Literals.blockSize
                    - lastLeaf.NodeSize

                let tailLen =
                    this.Count
                    - this.TailOffset

                if
                    shiftCount
                    <= 0
                then
                    this :> RRBVector<'T>
                elif
                    shiftCount
                    >= tailLen
                then
                    // Would shift everything out of the tail, so instead we'll promote a new tail
                    let removedLeaf, newRoot =
                        (this.Root :?> RRBFullNode<'T>).RemoveLastLeaf nullOwner this.Shift

                    let newTail =
                        this.Tail
                        |> Array.append removedLeaf.Items
                    // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
                    RRBPersistentVector<'T>(
                        this.Count,
                        this.Shift,
                        newRoot,
                        newTail,
                        this.TailOffset
                        - removedLeaf.NodeSize
                    )
                        .ShiftNodesFromTailIfNeeded()
                else
                    let itemsToShift, newTail =
                        this.Tail
                        |> Array.splitAt shiftCount

                    let newLeaf =
                        RRBLeafNode<'T>(nullOwner, Array.append lastLeaf.Items itemsToShift)

                    let newRoot =
                        (this.Root :?> RRBFullNode<'T>).ReplaceLastLeaf
                            nullOwner
                            this.Shift
                            newLeaf
                            shiftCount
                    // No need to recurse here
                    RRBPersistentVector<'T>(
                        this.Count,
                        this.Shift,
                        newRoot,
                        newTail,
                        this.TailOffset
                        + shiftCount
                    )
                    :> RRBVector<'T>

    // abstract member Empty : RRBVector<'T>  // Or maybe it should be unit -> RRBVector<'T>
    override this.Empty() =
        RRBPersistentVector<'T>(0, Literals.shiftSize, emptyNode, Array.empty, 0) :> RRBVector<'T>

    // abstract member IsEmpty : unit -> bool
    override this.IsEmpty() = this.Count = 0

    // abstract member StringRepr : string
    override this.StringRepr = this.ToString()

    // abstract member Length : int
    override this.Length = this.Count

    // abstract member IterLeaves : unit -> seq<'T []>
    override this.IterLeaves() =
        seq {
            yield!
                (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift
                |> Seq.map (fun leaf -> leaf.Items)

            yield this.Tail
        }

    // abstract member RevIterLeaves : unit -> seq<'T []>
    override this.RevIterLeaves() =
        seq {
            yield this.Tail

            yield!
                (this.Root :?> RRBFullNode<'T>).RevLeavesSeq this.Shift
                |> Seq.map (fun leaf -> leaf.Items)
        }

    // abstract member IterItems : unit -> seq<'T>
    override this.IterItems() =
        this.IterLeaves()
        |> Seq.collect id

    // abstract member RevIterItems : unit -> seq<'T>
    override this.RevIterItems() =
        seq {
            for arr in this.RevIterLeaves() do
                for i = arr.Length
                        - 1 downto 0 do
                    yield arr.[i]
        }

    // // abstract member GetEnumerator : unit -> IEnumerator<'T>

    // abstract member Push : 'T -> RRBVector<'T>
    override this.Push newItem =
        let tailLen =
            this.Count
            - this.TailOffset

        if tailLen < Literals.blockSize then
            let newTail =
                this.Tail
                |> Array.copyAndAppend newItem

            RRBPersistentVector<'T>(
                this.Count
                + 1,
                this.Shift,
                this.Root,
                newTail,
                this.TailOffset
            )
            :> RRBVector<'T>
        else
            let tailNode = RRBNode<'T>.MkLeaf nullOwner this.Tail :?> RRBLeafNode<'T>

            let newRoot, newShift =
                (this.Root :?> RRBFullNode<'T>).AppendLeaf nullOwner this.Shift tailNode

            RRBPersistentVector<'T>(
                this.Count
                + 1,
                newShift,
                newRoot,
                [| newItem |],
                this.Count
            )
            :> RRBVector<'T>

    // abstract member Peek : unit -> 'T
    override this.Peek() =
        if
            this.Count
            <= 0
        then
            failwith "Can't get last item from an empty vector"
        else
            let tailLen =
                this.Count
                - this.TailOffset
#if DEBUG
            if tailLen = 0 then
                failwith "Tail should never be empty"
#endif
            this.Tail.[tailLen
                       - 1]

    // abstract member Pop : unit -> RRBVector<'T>
    override this.Pop() =
        if
            this.Count
            <= 0
        then
            failwith "Can't pop from an empty vector"
        elif this.Count = 1 then
            this.Empty()
        else if this.Tail.Length > 1 then
            let newTail =
                this.Tail
                |> Array.copyAndPop

            RRBPersistentVector<'T>(
                this.Count
                - 1,
                this.Shift,
                this.Root,
                newTail,
                this.TailOffset
            )
            :> RRBVector<'T>
        else
            let newTailNode, newRoot =
                (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift

            RRBPersistentVector<'T>(
                this.Count
                - 1,
                this.Shift,
                newRoot,
                newTailNode.Items,
                this.TailOffset
                - newTailNode.NodeSize
            )
                .AdjustTree()

    // abstract member Take : int -> RRBVector<'T>
    override this.Take idx =
        if
            idx
            >= this.Count
        then
            this :> RRBVector<'T>
        elif idx <= 0 then // TODO: Allow taking negative items, which is like Skip (negative count + length), i.e. Take -5 will return the last five items of the list
            this.Empty()
        elif idx = this.TailOffset then
            // Dropping the tail and nothing else, so we promote a new tail
            let newTailNode, newRoot =
                (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift

            RRBPersistentVector<'T>(
                idx,
                this.Shift,
                newRoot,
                newTailNode.Items,
                idx
                - newTailNode.NodeSize
            )
                .AdjustTree()
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let newTail =
                this.Tail
                |> Array.truncate (
                    idx
                    - this.TailOffset
                )

            RRBPersistentVector<'T>(idx, this.Shift, this.Root, newTail, this.TailOffset)
            :> RRBVector<'T>
        else
            let tmpRoot = this.Root.KeepNTreeItems nullOwner this.Shift idx

            let newTailNode, newRoot =
                (tmpRoot :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift

            RRBPersistentVector<'T>(
                idx,
                this.Shift,
                newRoot,
                newTailNode.Items,
                idx
                - newTailNode.NodeSize
            )
                .AdjustTree()

    // abstract member Skip : int -> RRBVector<'T>
    override this.Skip idx =
        if idx <= 0 then
            this :> RRBVector<'T>
        elif
            idx
            >= this.Count
        then
            this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we'll have an empty root
            RRBPersistentVector<'T>(
                this.Tail.Length,
                Literals.shiftSize,
                RRBNode<'T>.MkFullNode nullOwner Array.empty,
                this.Tail,
                0
            )
            :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let newTail =
                this.Tail
                |> Array.skip (
                    idx
                    - this.TailOffset
                )

            RRBPersistentVector<'T>(
                newTail.Length,
                Literals.shiftSize,
                RRBNode<'T>.MkFullNode nullOwner Array.empty,
                newTail,
                0
            )
            :> RRBVector<'T>
        else
            let newRoot = this.Root.SkipNTreeItems nullOwner this.Shift idx

            RRBPersistentVector<'T>(
                this.Count
                - idx,
                this.Shift,
                newRoot,
                this.Tail,
                this.TailOffset
                - idx
            )
                .AdjustTree()

    // abstract member Split : int -> RRBVector<'T> * RRBVector<'T>
    override this.Split idx =
        this.EnsureValidIndexLengthAllowed idx

        if idx = this.Count then
            this :> RRBVector<'T>, this.Empty()
        elif idx = 0 then
            this.Empty(), this :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail
            let newTailNode, newRoot =
                (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift

            let newLeft =
                RRBPersistentVector<'T>(
                    idx,
                    this.Shift,
                    newRoot,
                    newTailNode.Items,
                    idx
                    - newTailNode.NodeSize
                )
                    .AdjustTree()

            let newRight =
                RRBPersistentVector<'T>(
                    this.Tail.Length,
                    Literals.shiftSize,
                    RRBNode<'T>.MkFullNode nullOwner Array.empty,
                    this.Tail,
                    0
                )
                :> RRBVector<'T>

            newLeft, newRight
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailL, tailR =
                this.Tail
                |> Array.splitAt (
                    idx
                    - this.TailOffset
                )

            let newLeft =
                RRBPersistentVector<'T>(idx, this.Shift, this.Root, tailL, this.TailOffset)
                :> RRBVector<'T>

            let newRight =
                RRBPersistentVector<'T>(
                    tailR.Length,
                    Literals.shiftSize,
                    RRBNode<'T>.MkFullNode nullOwner Array.empty,
                    tailR,
                    0
                )
                :> RRBVector<'T>

            newLeft, newRight
        else
            let rootL, rootR = this.Root.SplitTree nullOwner this.Shift idx

            let newTailNodeL, newRootL =
                (rootL :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift
            // Have to adjust the tree for both newLeft AND newRight in this one, since either one could have become a tall, thin tree during the split
            let newLeft =
                RRBPersistentVector<'T>(
                    idx,
                    this.Shift,
                    newRootL,
                    newTailNodeL.Items,
                    idx
                    - newTailNodeL.NodeSize
                )
                    .AdjustTree()

            let newRight =
                RRBPersistentVector<'T>(
                    this.Count
                    - idx,
                    this.Shift,
                    rootR,
                    this.Tail,
                    this.Count
                    - idx
                    - this.Tail.Length
                )
                    .AdjustTree()

            newLeft, newRight

    // abstract member Slice : int * int -> RRBVector<'T>
    override this.Slice(start, stop) =
        (this.Skip start)
            .Take(
                stop
                - start
                + 1
            )

    // abstract member GetSlice : int option * int option -> RRBVector<'T>
    override this.GetSlice(start, stop) =
        match start, stop with
        | None, None -> this :> RRBVector<'T>
        | None, Some stop -> this.Take(stop + 1) // vec.[..5] should return all indices from 0 to 5, i.e. 6 in total
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice(start, stop)

    // abstract member Append : RRBVector<'T> -> RRBVector<'T>
    override this.Append other =
        match other with
        | :? RRBPersistentVector<'T> as right ->
            if this.Count = 0 then
                right :> RRBVector<'T>
            elif right.Count = 0 then
                this :> RRBVector<'T>
            else
                let newLen =
                    this.Count
                    + right.Count

                if
                    right.TailOffset
                    <= 0
                then
                    // Right is a tail-only vector
                    let tailLenL =
                        this.Count
                        - this.TailOffset

                    let tailLenR =
                        right.Count
                        - right.TailOffset

                    if
                        tailLenL
                        + tailLenR
                        <= Literals.blockSize
                    then
                        let newTail = Array.append this.Tail right.Tail

                        RRBPersistentVector<'T>(
                            newLen,
                            this.Shift,
                            this.Root,
                            newTail,
                            this.TailOffset
                        )
                        :> RRBVector<'T>
                    else
                        let newLeafItems, newTail =
                            Array.appendAndSplitAt Literals.blockSize this.Tail right.Tail

                        let newLeaf = RRBNode<'T>.MkLeaf nullOwner newLeafItems :?> RRBLeafNode<'T>

                        if
                            this.TailOffset
                            <= 0
                        then
                            // Can't use AppendLeaf in an empty root, so we create the first twig by hand
                            // FIXME: Yes, we can actually. So this if-else isn't actually necessary! I think. TODO: Test this.
                            let newRoot = RRBNode<'T>.MkFullNode nullOwner [| newLeaf |]

                            RRBPersistentVector<'T>(
                                newLen,
                                Literals.shiftSize,
                                newRoot,
                                newTail,
                                Literals.blockSize
                            )
                            :> RRBVector<'T>
                        else
                            let newRoot, newShift =
                                (this.Root :?> RRBFullNode<'T>).AppendLeaf
                                    nullOwner
                                    this.Shift
                                    newLeaf
                            // The new leaf that was pushed down cannot have been short, so here we don't need to adjust the tree to maintain the invariant.
                            RRBPersistentVector<'T>(
                                newLen,
                                newShift,
                                newRoot,
                                newTail,
                                this.TailOffset
                                + Literals.blockSize
                            )
                            :> RRBVector<'T>
                elif
                    this.TailOffset
                    <= 0
                then
                    // Right has a root and a tail, but we're a tail-only node
                    let tailNode = RRBNode<'T>.MkLeaf nullOwner this.Tail :?> RRBLeafNode<'T>

                    let newRoot, newShift =
                        (right.Root :?> RRBFullNode<'T>).PrependLeaf nullOwner right.Shift tailNode

                    RRBPersistentVector<'T>(
                        newLen,
                        newShift,
                        newRoot,
                        right.Tail,
                        right.TailOffset
                        + tailNode.NodeSize
                    )
                    :> RRBVector<'T>
                else
                    // Right has a root and a tail, and so do we
                    // TODO FIXME: Determine if there are any merge scenarios which break the invariant, by running lots of merges that hit this code branch (Original code had .AdjustTree everywhere, but is it needed?)
                    // One scenario I can think of is a left tree of [M*M-1] TM, while the right tree is [M] T5 or something. TODO: Work that out more precisely.
                    let tailNode = RRBNode<'T>.MkLeaf nullOwner this.Tail :?> RRBLeafNode<'T>
                    // Can the tail be merged into the two twig nodes? Now's the time to find out, while we can still push it down to form a new root
                    let tailCanFit =
                        ((this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift)
                            .HasRoomToMergeTheTail
                            Literals.shiftSize
                            tailNode
                            ((right.Root :?> RRBFullNode<'T>).LeftmostTwig right.Shift)

                    let mergedShift, mergedTree =
                        if tailCanFit then
                            max this.Shift right.Shift,
                            (this.Root :?> RRBFullNode<'T>).MergeTree
                                nullOwner
                                this.Shift
                                (Some tailNode)
                                right.Shift
                                (right.Root :?> RRBFullNode<'T>)
                                false
                        else
                            let tmpRoot, tmpShift =
                                (this.Root :?> RRBFullNode<'T>).AppendLeaf
                                    nullOwner
                                    this.Shift
                                    tailNode

                            max tmpShift right.Shift,
                            (tmpRoot :?> RRBFullNode<'T>).MergeTree
                                nullOwner
                                tmpShift
                                None
                                right.Shift
                                (right.Root :?> RRBFullNode<'T>)
                                false

                    match mergedTree with
                    | newRoot, None ->
                        RRBPersistentVector<'T>(
                            newLen,
                            mergedShift,
                            newRoot,
                            right.Tail,
                            this.Count
                            + right.TailOffset
                        )
                            .AdjustTree()
                    | newLeft, Some newRight ->
                        let newRoot =
                            (newLeft :?> RRBFullNode<'T>).NewParent nullOwner mergedShift [|
                                newLeft
                                newRight
                            |]

                        RRBPersistentVector<'T>(
                            newLen,
                            (RRBMath.up mergedShift),
                            newRoot,
                            right.Tail,
                            this.Count
                            + right.TailOffset
                        )
                            .AdjustTree()
        // Transient vectors may only stay transient if appended to a transient of the same owner; here, we're a persistent
        | :? RRBTransientVector<'T> as right -> this.Append(right.Persistent())
        | _ -> this.Append(other :?> RRBPersistentVector<'T>) // WILL throw if we create a new subclass. TODO: Decide whether shutting up the compiler like this is really a good idea.

    // abstract member Insert : int -> 'T -> RRBVector<'T>
    override this.Insert idx newItem =
        this.EnsureValidIndexLengthAllowed idx

        if
            idx
            >= this.TailOffset
        then
            if
                this.Count
                - this.TailOffset < Literals.blockSize
            then
                let newTail =
                    this.Tail
                    |> Array.copyAndInsertAt
                        (idx
                         - this.TailOffset)
                        newItem

                RRBPersistentVector<'T>(
                    this.Count
                    + 1,
                    this.Shift,
                    this.Root,
                    newTail,
                    this.TailOffset
                )
                :> RRBVector<'T>
            else
                let newLeafItems, newTailItem =
                    this.Tail
                    |> Array.copyAndInsertIntoFullArray
                        (idx
                         - this.TailOffset)
                        newItem

                let newTail = Array.singleton newTailItem
                let newLeafNode = RRBNode<'T>.MkLeaf nullOwner newLeafItems :?> RRBLeafNode<'T>

                let newRoot, newShift =
                    (this.Root :?> RRBFullNode<'T>).AppendLeaf nullOwner this.Shift newLeafNode
                // Pushing a full tail down into a leaf can't break the invariant, so no need to adjust the tree here
                RRBPersistentVector<'T>(
                    this.Count
                    + 1,
                    newShift,
                    newRoot,
                    newTail,
                    this.Count
                )
                :> RRBVector<'T>
        else
            let newRoot, newShift =
                match this.Root.InsertedTree nullOwner this.Shift idx newItem None 0 with
                | SimpleInsertion(newCurrent) -> newCurrent, this.Shift
                | SplitNode(newCurrent, newRight) ->
                    (newCurrent :?> RRBFullNode<'T>).NewParent nullOwner this.Shift [|
                        newCurrent
                        newRight
                    |],
                    (RRBMath.up this.Shift)
                | SlidItemsLeft(newLeft, newCurrent) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
                | SlidItemsRight(newCurrent, newRight) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen

            RRBPersistentVector<'T>(
                this.Count
                + 1,
                newShift,
                newRoot,
                this.Tail,
                this.TailOffset
                + 1
            )
                .AdjustTree() // TODO: Remove this AdjustTree() call and prove that tests fail when we do that (inserting into last leaf could cause invariant failure)

    // abstract member Remove : int -> RRBVector<'T>
    override this.Remove idx = this.RemoveImpl idx true

    member internal this.RemoveWithoutRebalance idx = this.RemoveImpl idx false

    member internal this.RemoveImpl idx shouldCheckForRebalancing =
        this.EnsureValidIndex idx

        if
            idx
            >= this.TailOffset
        then
            if
                this.Count
                - this.TailOffset > 1
            then
                let newTail =
                    this.Tail
                    |> Array.copyAndRemoveAt (
                        idx
                        - this.TailOffset
                    )

                RRBPersistentVector<'T>(
                    this.Count
                    - 1,
                    this.Shift,
                    this.Root,
                    newTail,
                    this.TailOffset
                )
                :> RRBVector<'T>
            elif this.Count = 1 then
                this.Empty()
            else
                // Tail is now empty, so promote a new tail
                let newTailNode, newRoot =
                    (this.Root :?> RRBFullNode<'T>).PopLastLeaf nullOwner this.Shift

                RRBPersistentVector<'T>(
                    this.Count
                    - 1,
                    this.Shift,
                    newRoot,
                    newTailNode.Items,
                    this.TailOffset
                    - newTailNode.NodeSize
                )
                    .AdjustTree()
        else
            let newRoot =
                this.Root.RemovedItem nullOwner this.Shift shouldCheckForRebalancing idx

            RRBPersistentVector<'T>(
                this.Count
                - 1,
                this.Shift,
                newRoot,
                this.Tail,
                this.TailOffset
                - 1
            )
                .AdjustTree()

    // abstract member Update : int -> 'T -> RRBVector<'T>
    override this.Update idx newItem =
        this.EnsureValidIndex idx

        if
            idx
            >= this.TailOffset
        then
            let newTail =
                this.Tail
                |> Array.copyAndSet
                    (idx
                     - this.TailOffset)
                    newItem

            RRBPersistentVector<'T>(this.Count, this.Shift, this.Root, newTail, this.TailOffset)
            :> RRBVector<'T>
        else
            let newRoot = this.Root.UpdatedTree nullOwner this.Shift idx newItem

            RRBPersistentVector<'T>(this.Count, this.Shift, newRoot, this.Tail, this.TailOffset)
            :> RRBVector<'T>

    // abstract member GetItem : int -> 'T
    override this.GetItem idx =
        this.EnsureValidIndex idx

        if
            idx
            >= this.TailOffset
        then
            tail.[idx
                  - this.TailOffset]
        else
            this.Root.GetTreeItem this.Shift idx

    member this.EnsureValidIndex idx =
        if idx < 0 then
            failwith "Index must not be negative"
        elif
            idx
            >= this.Count
        then
            failwith "Index must not be past the end of the vector"
        else
            ()

    member this.EnsureValidIndexLengthAllowed idx =
        if idx < 0 then
            failwith "Index must not be negative"
        elif idx > this.Count then
            failwith "Index must not be more than one past the end of the vector"
        else
            ()


and internal RRBTransientVector<'T>
    internal (count, shift: int, root: RRBNode<'T>, tail: 'T[], tailOffset: int) =
    inherit RRBVector<'T>()

    member val Count = count with get, set
    member val Shift = shift with get, set
    member val Root = root with get, set
    member val Tail = tail with get, set
    member val TailOffset = tailOffset with get, set
    member val internal Owner = root.Owner with get, set

    override this.ToString() =
        sprintf
            "RRBTransientVector<owner=%s,length=%d,shift=%d,tailOffset=%d,root=%A,tail=%A>"
            (match !this.Owner with
             | null -> "<null>"
             | s -> s)
            this.Count
            this.Shift
            this.TailOffset
            this.Root
            this.Tail

    // new() = RRBTransientVector(0, 5, emptyNode, Array.empty, 0)
    internal new(token: OwnerToken) =
        let root = RRBExpandedFullNode<'T>(token, Array.zeroCreate Literals.blockSize, 0)
        RRBTransientVector<'T>(0, Literals.shiftSize, root, Array.zeroCreate Literals.blockSize, 0)

    new() = RRBTransientVector<'T>(mkOwnerToken ())

    static member MkEmpty() = RRBTransientVector<'T>()
    static member internal MkEmptyWithToken token = RRBTransientVector<'T>(token)

    member this.Invalidate() = this.Owner <- nullOwner

    // TODO: Create a property called (TODO: name? ValidTransient will do for now) that we'll expose to allow users to check whether transient
    // can be used or has been invalidated. Right now it's a bit black-box and people might get confused when an append invalidates a transient.
    member this.ThrowIfNotValid(?msg: string) =
        if isNull (!this.Owner) then
            let msg = defaultArg msg "any operations"

            invalidOp
            <| sprintf "This vector is no longer valid for %s" msg

    override this.Persistent() =
        let newRoot = (this.Root :?> RRBFullNode<'T>).ShrinkRightSpine nullOwner this.Shift

        let tailLen =
            this.Count
            - this.TailOffset

        this.Invalidate()

        RRBPersistentVector<'T>(
            this.Count,
            this.Shift,
            newRoot,
            this.Tail
            |> Array.truncate tailLen,
            this.TailOffset
        )
        :> RRBVector<'T>

    override this.Transient() =
        this.ThrowIfNotValid()
        this :> RRBVector<'T>

    member internal this.AdjustTree() =
        let v: RRBVector<'T> = this.ShiftNodesFromTailIfNeeded()
        (v :?> RRBTransientVector<'T>).ShortenTree()

    member internal this.ShortenTree() =
        if
            this.Shift
            <= Literals.shiftSize
        then
            this :> RRBVector<'T>
        else if this.Root.NodeSize > 1 then
            this :> RRBVector<'T>
        elif this.Root.NodeSize = 1 then
            this.Shift <- RRBMath.down this.Shift
            this.Root <- (this.Root :?> RRBFullNode<'T>).FirstChild
            this.ShortenTree()
        else // Empty root but shift was too large
            this.Shift <- Literals.shiftSize
            this :> RRBVector<'T>

    member internal this.ShiftNodesFromTailIfNeeded() =
        if
            this.TailOffset
            <= 0
            || this.Root.NodeSize = 0
        then
            // Empty root, so no need to shift any nodes
            this :> RRBVector<'T>
        else
            let lastTwig = (this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift
            // If parent of last leaf is a relaxed node, this automatically satisfies the invariant
            if lastTwig :? RRBRelaxedNode<'T> then
                this :> RRBVector<'T>
            else
                let lastLeaf = lastTwig.LastChild :?> RRBLeafNode<'T>

                let shiftCount =
                    Literals.blockSize
                    - lastLeaf.NodeSize

                let tailLen =
                    this.Count
                    - this.TailOffset

                if
                    shiftCount
                    <= 0
                then
                    this :> RRBVector<'T>
                elif
                    shiftCount
                    >= tailLen
                then
                    // Would shift everything out of the tail, so instead we'll promote a new tail
                    let removedLeaf, newRoot =
                        (this.Root :?> RRBFullNode<'T>).RemoveLastLeaf this.Owner this.Shift

                    let removedSize = removedLeaf.NodeSize

                    for i = tailLen
                            - 1 downto 0 do
                        this.Tail.[i
                                   + removedSize] <- this.Tail.[i]

                    removedLeaf.Items.CopyTo(this.Tail, 0)

                    this.TailOffset <-
                        this.TailOffset
                        - removedSize

                    if
                        not
                        <| isSameObj newRoot this.Root
                    then
                        this.Root <- newRoot
                    // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
                    this.ShiftNodesFromTailIfNeeded()
                else
                    let itemsToShift = Array.sub this.Tail 0 shiftCount

                    for i = 0 to tailLen
                                 - shiftCount
                                 - 1 do
                        this.Tail.[i] <-
                            this.Tail.[i
                                       + shiftCount]

                    Array.fill
                        this.Tail
                        (tailLen
                         - shiftCount)
                        shiftCount
                        Unchecked.defaultof<'T>

                    let newLeaf =
                        RRBLeafNode<'T>(this.Owner, Array.append lastLeaf.Items itemsToShift)

                    let newRoot =
                        (this.Root :?> RRBFullNode<'T>).ReplaceLastLeaf
                            this.Owner
                            this.Shift
                            newLeaf
                            shiftCount

                    if
                        not
                        <| isSameObj newRoot this.Root
                    then
                        this.Root <- newRoot

                    this.TailOffset <-
                        this.TailOffset
                        + shiftCount
                    // No need to recurse here
                    this :> RRBVector<'T>

    // abstract member Empty : unit -> RRBVector<'T>
    override this.Empty() =
        this.ThrowIfNotValid()
        this.Count <- 0
        this.Shift <- Literals.shiftSize
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
            yield!
                (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift
                |> Seq.map (fun leaf -> leaf.Items)

            yield
                this.Tail
                |> Array.truncate (
                    this.Count
                    - this.TailOffset
                )
        }

    member this.IterEditableLeavesWithoutTail() =
        this.ThrowIfNotValid()
        let owner = this.Owner

        (this.Root :?> RRBFullNode<'T>).LeavesSeq this.Shift
        |> Seq.map (fun leaf -> (leaf.GetEditableNode owner :?> RRBLeafNode<'T>).Items)

    // abstract member RevIterLeaves : unit -> seq<'T []>
    override this.RevIterLeaves() =
        this.ThrowIfNotValid()

        seq {
            yield
                this.Tail
                |> Array.truncate (
                    this.Count
                    - this.TailOffset
                )

            yield!
                (this.Root :?> RRBFullNode<'T>).RevLeavesSeq this.Shift
                |> Seq.map (fun leaf -> leaf.Items)
        }

    member this.RevIterEditableLeavesWithoutTail() =
        this.ThrowIfNotValid()
        let owner = this.Owner

        (this.Root :?> RRBFullNode<'T>).RevLeavesSeq this.Shift
        |> Seq.map (fun leaf -> (leaf.GetEditableNode owner :?> RRBLeafNode<'T>).Items)

    // abstract member IterItems : unit -> seq<'T>
    override this.IterItems() =
        this.ThrowIfNotValid()

        this.IterLeaves()
        |> Seq.collect id

    // abstract member RevIterItems : unit -> seq<'T>
    override this.RevIterItems() =
        this.ThrowIfNotValid()

        seq {
            for arr in this.RevIterLeaves() do
                for i = arr.Length
                        - 1 downto 0 do
                    yield arr.[i]
        }

    // // abstract member GetEnumerator : unit -> IEnumerator<'T>

    // abstract member Push : 'T -> RRBVector<'T>
    override this.Push newItem =
        this.ThrowIfNotValid()

        let tailLen =
            this.Count
            - this.TailOffset

        if tailLen < Literals.blockSize then
            this.Count <-
                this.Count
                + 1

            this.Tail.[tailLen] <- newItem
            this :> RRBVector<'T>
        else
            let tailNode = RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>

            let newRoot, newShift =
                (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift tailNode

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

            this.TailOffset <- this.Count

            this.Count <-
                this.Count
                + 1

            this.Shift <- newShift
            this.Tail <- Array.zeroCreate Literals.blockSize
            this.Tail.[0] <- newItem
            this :> RRBVector<'T>

    // abstract member Peek : unit -> 'T
    override this.Peek() =
        this.ThrowIfNotValid()

        if
            this.Count
            <= 0
        then
            failwith "Can't get last item from an empty vector"
        else
            let tailLen =
                this.Count
                - this.TailOffset
#if DEBUG
            if tailLen = 0 then
                failwith "Tail should never be empty"
#endif
            this.Tail.[tailLen
                       - 1]

    // abstract member Pop : unit -> RRBVector<'T>
    override this.Pop() =
        this.ThrowIfNotValid()

        if
            this.Count
            <= 0
        then
            failwith "Can't pop from an empty vector"
        elif this.Count = 1 then
            this.Empty()
        else
            let tailLen =
                this.Count
                - this.TailOffset

            if tailLen > 1 then
                this.Count <-
                    this.Count
                    - 1

                this.Tail.[tailLen
                           - 1] <- Unchecked.defaultof<'T>

                this :> RRBVector<'T>
            else
                this.Count <-
                    this.Count
                    - 1

                let newTailNode, newRoot =
                    (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift

                if
                    not
                    <| isSameObj newRoot this.Root
                then
                    this.Root <- newRoot

                this.TailOffset <-
                    this.TailOffset
                    - newTailNode.NodeSize

                this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
                this.AdjustTree()

    // abstract member Take : int -> RRBVector<'T>
    override this.Take idx =
        this.ThrowIfNotValid()

        if
            idx
            >= this.Count
        then
            this :> RRBVector<'T>
        elif idx <= 0 then // TODO: Allow taking negative items, which is like Skip (negative count + length), i.e. Take -5 will return the last five items of the list
            this.Empty()
        elif idx = this.TailOffset then
            // Dropping the tail and nothing else, so we promote a new tail
            let newTailNode, newRoot =
                (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift

            this.Count <- idx

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

            this.TailOffset <-
                idx
                - newTailNode.NodeSize

            this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
            this.AdjustTree()
        elif idx > this.TailOffset then
            // Splitting the tail in two
            this.Count <- idx

            let newTailLen =
                idx
                - this.TailOffset

            Array.fill
                this.Tail
                newTailLen
                (Literals.blockSize
                 - newTailLen)
                Unchecked.defaultof<'T>

            this :> RRBVector<'T>
        else
            let tmpRoot = this.Root.KeepNTreeItems this.Owner this.Shift idx

            let newTailNode, newRoot =
                (tmpRoot :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift

            let newRoot' = (newRoot :?> RRBFullNode<'T>).MaybeExpand this.Owner this.Shift
            this.Count <- idx

            if
                not
                <| isSameObj newRoot' this.Root
            then
                this.Root <- newRoot'

            this.TailOffset <-
                idx
                - newTailNode.NodeSize

            this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner
            this.AdjustTree()

    // abstract member Skip : int -> RRBVector<'T>
    override this.Skip idx =
        this.ThrowIfNotValid()

        if idx <= 0 then
            this :> RRBVector<'T>
        elif
            idx
            >= this.Count
        then
            this.Empty()
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we'll have an empty root
            this.Root.SetNodeSize 0
            Array.fill (this.Root :?> RRBFullNode<'T>).Children 0 Literals.blockSize null

            this.Count <-
                this.Count
                - idx

            this.Shift <- Literals.shiftSize
            this.TailOffset <- 0
            this :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            this.Root.SetNodeSize 0
            Array.fill (this.Root :?> RRBFullNode<'T>).Children 0 Literals.blockSize null

            let tailLen =
                this.Count
                - this.TailOffset

            let tailIdx =
                idx
                - this.TailOffset

            this.Count <-
                this.Count
                - idx

            this.Shift <- Literals.shiftSize
            // Shift remaining items down, then zero out the slots that have become empty
            for i = tailIdx to tailLen
                               - 1 do
                this.Tail.[i
                           - tailIdx] <- this.Tail.[i]

            Array.fill
                this.Tail
                (tailLen
                 - tailIdx)
                tailIdx
                Unchecked.defaultof<'T>

            this.TailOffset <- 0
            this :> RRBVector<'T>
        else
            let newRoot = this.Root.SkipNTreeItems this.Owner this.Shift idx

            this.Count <-
                this.Count
                - idx

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

            this.TailOffset <-
                this.TailOffset
                - idx

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
            let right =
                RRBTransientVector<'T>(
                    this.Count,
                    this.Shift,
                    this.Root,
                    this.Tail,
                    this.TailOffset
                )

            this.Count <- 0
            this.Shift <- Literals.shiftSize
            this.Root <- RRBExpandedFullNode<'T>(this.Owner, Array.zeroCreate Literals.blockSize, 0)
            this.Tail <- Array.zeroCreate Literals.blockSize
            this.TailOffset <- 0
            this :> RRBVector<'T>, right :> RRBVector<'T>
        elif idx = this.TailOffset then
            // Splitting exactly at the tail means we have to promote a new tail for the left (this)
            let right = RRBTransientVector<'T>(this.Owner)
            right.Tail <- this.Tail

            right.Count <-
                this.Count
                - this.TailOffset

            let newTailNode, newRoot =
                (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift

            this.Count <- idx

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

            this.Tail <- newTailNode.GetEditableArrayOfBlockSizeLength this.Owner

            this.TailOffset <-
                idx
                - newTailNode.NodeSize

            this.AdjustTree(), right :> RRBVector<'T>
        elif idx > this.TailOffset then
            // Splitting the tail in two
            let tailIdx =
                idx
                - this.TailOffset

            let tailLen =
                this.Count
                - this.TailOffset

            let right = RRBTransientVector<'T>(this.Owner)

            Array.blit
                this.Tail
                tailIdx
                right.Tail
                0
                (tailLen
                 - tailIdx)

            Array.fill
                this.Tail
                tailIdx
                (tailLen
                 - tailIdx)
                Unchecked.defaultof<'T>

            this.Count <- idx

            right.Count <-
                tailLen
                - tailIdx

            this :> RRBVector<'T>, right :> RRBVector<'T>
        else
            let rootL, rootR = this.Root.SplitTree this.Owner this.Shift idx

            let right =
                RRBTransientVector<'T>(
                    this.Count
                    - idx,
                    this.Shift,
                    rootR,
                    this.Tail,
                    this.TailOffset
                    - idx
                )

            let newTailNodeL, newRootL =
                (rootL :?> RRBFullNode<'T>).PopLastLeaf rootL.Owner this.Shift

            this.Count <- idx

            if
                not
                <| isSameObj newRootL this.Root
            then
                this.Root <- newRootL

            this.Tail <- newTailNodeL.GetEditableArrayOfBlockSizeLength this.Owner

            this.TailOffset <-
                idx
                - newTailNodeL.NodeSize
            // Have to adjust the tree for both "this" AND "right" in this one, since either one could have become a tall, thin tree
            this.AdjustTree(), right.AdjustTree()

    // abstract member Slice : int * int -> RRBVector<'T>
    override this.Slice(start, stop) =
        this.ThrowIfNotValid()

        (this.Skip start)
            .Take(
                stop
                - start
                + 1
            )

    // abstract member GetSlice : int option * int option -> RRBVector<'T>
    override this.GetSlice(start, stop) =
        this.ThrowIfNotValid()

        match start, stop with
        | None, None -> this :> RRBVector<'T>
        | None, Some stop -> this.Take(stop + 1) // vec.[..5] should return all indices from 0 to 5, i.e. 6 in total
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice(start, stop)

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
            elif right.Count = 0 then
                right.Invalidate()
                this :> RRBVector<'T>
            else
                let newLen =
                    this.Count
                    + right.Count

                if
                    right.TailOffset
                    <= 0
                then
                    // Right is a tail-only vector
                    let tailLenL =
                        this.Count
                        - this.TailOffset

                    let tailLenR =
                        right.Count
                        - right.TailOffset

                    if
                        tailLenL
                        + tailLenR
                        <= Literals.blockSize
                    then
                        Array.blit right.Tail 0 this.Tail tailLenL tailLenR
                        this.Count <- newLen
                        right.Invalidate()
                        this :> RRBVector<'T>
                    else
                        let newTail = Array.zeroCreate Literals.blockSize

                        let splitIdx =
                            Literals.blockSize
                            - tailLenL

                        Array.blit right.Tail 0 this.Tail tailLenL splitIdx

                        Array.blit
                            right.Tail
                            splitIdx
                            newTail
                            0
                            (tailLenR
                             - splitIdx)

                        let newLeaf = RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>
                        this.Tail <- newTail

                        if
                            this.TailOffset
                            <= 0
                        then
                            // We CAN use AppendLeaf in an empty root!
                            let newRoot, newShift =
                                (this.Root :?> RRBFullNode<'T>).AppendLeaf
                                    this.Owner
                                    this.Shift
                                    newLeaf

                            this.Count <- newLen

                            if
                                not
                                <| isSameObj newRoot this.Root
                            then
                                this.Root <- newRoot

                            this.Shift <- newShift

                            this.TailOffset <-
                                this.TailOffset
                                + newLeaf.NodeSize

                            right.Invalidate()
                            this :> RRBVector<'T>
                        else
                            let newRoot, newShift =
                                (this.Root :?> RRBFullNode<'T>).AppendLeaf
                                    this.Owner
                                    this.Shift
                                    newLeaf

                            this.Count <- newLen

                            if
                                not
                                <| isSameObj newRoot this.Root
                            then
                                this.Root <- newRoot

                            this.Shift <- newShift

                            this.TailOffset <-
                                this.TailOffset
                                + newLeaf.NodeSize

                            right.Invalidate()
                            this :> RRBVector<'T>
                // TODO: The "then" and "else" blocks here are identical. Write test to make sure we can append into a tail-only left transient, then collapse this "if" into a single copy of this block.
                elif
                    this.TailOffset
                    <= 0
                then
                    // Right has a root and a tail, but we're a tail-only node
                    let tailLen =
                        this.Count
                        - this.TailOffset

                    let tailNode =
                        if tailLen < Literals.blockSize then
                            RRBNode<'T>.MkLeaf
                                this.Owner
                                (this.Tail
                                 |> Array.truncate tailLen)
                            :?> RRBLeafNode<'T>
                        else
                            RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>

                    let newRoot, newShift =
                        (right.Root :?> RRBFullNode<'T>).PrependLeaf
                            right.Owner
                            right.Shift
                            tailNode

                    if
                        not
                        <| isSameObj newRoot right.Root
                    then
                        right.Root <- newRoot
                    // Calling code expects "this" to contain the results, so we steal everything from the right-hand tree
                    this.Root <- right.Root
                    this.Shift <- newShift
                    this.Count <- newLen

                    this.TailOffset <-
                        right.TailOffset
                        + tailNode.NodeSize

                    this.Tail <- right.Tail
                    right.Invalidate()
                    this :> RRBVector<'T>
                else
                    // Right has a root and a tail, and so do we
                    let tailLen =
                        this.Count
                        - this.TailOffset

                    let tailNode =
                        if tailLen < Literals.blockSize then
                            RRBNode<'T>.MkLeaf
                                this.Owner
                                (this.Tail
                                 |> Array.truncate tailLen)
                            :?> RRBLeafNode<'T>
                        else
                            RRBNode<'T>.MkLeaf this.Owner this.Tail :?> RRBLeafNode<'T>
                    // Can the tail be merged into the two twig nodes? Now's the time to find out, while we can still push it down to form a new root
                    let tailCanFit =
                        ((this.Root :?> RRBFullNode<'T>).RightmostTwig this.Shift)
                            .HasRoomToMergeTheTail
                            Literals.shiftSize
                            tailNode
                            ((right.Root :?> RRBFullNode<'T>).LeftmostTwig right.Shift)

                    let newRoot, newShift =
                        let mergedShift, mergedTree =
                            if tailCanFit then
                                max this.Shift right.Shift,
                                (this.Root :?> RRBFullNode<'T>).MergeTree
                                    this.Owner
                                    this.Shift
                                    (Some tailNode)
                                    right.Shift
                                    (right.Root :?> RRBFullNode<'T>)
                                    true
                            else
                                let tmpRoot, tmpShift =
                                    (this.Root :?> RRBFullNode<'T>).AppendLeaf
                                        this.Owner
                                        this.Shift
                                        tailNode

                                max tmpShift right.Shift,
                                (tmpRoot :?> RRBFullNode<'T>).MergeTree
                                    this.Owner
                                    tmpShift
                                    None
                                    right.Shift
                                    (right.Root :?> RRBFullNode<'T>)
                                    true

                        match mergedTree with
                        | newRoot, None -> newRoot, mergedShift
                        | newLeft, Some newRight ->
                            // NOTE: Here we really do want newRight to be the instance on which we call NewParent.
                            // We've already shrunk newLeft's inside MergeTree, so newLeft is no longer an expanded node by now.
                            let newRoot =
                                (newRight :?> RRBFullNode<'T>).NewParent this.Owner mergedShift [|
                                    newLeft
                                    newRight
                                |]

                            newRoot, (RRBMath.up mergedShift)

                    this.TailOffset <-
                        this.Count
                        + right.TailOffset

                    this.Count <- newLen
                    this.Shift <- newShift
                    this.Root <- newRoot
                    this.Tail <- right.Tail
                    right.Invalidate()
                    this.AdjustTree()
        // Transient vectors may only stay transient if appended to a transient of the same owner
        | :? RRBTransientVector<'T> as right -> this.Persistent().Append(right.Persistent())
        | :? RRBPersistentVector<'T> as right -> this.Persistent().Append right
        | _ -> this.Persistent().Append other

    // abstract member Insert : int -> 'T -> RRBVector<'T>
    override this.Insert idx newItem =
        this.ThrowIfNotValid()
        this.EnsureValidIndexLengthAllowed idx

        if
            idx
            >= this.TailOffset
        then
            let tailLen =
                this.Count
                - this.TailOffset

            if tailLen < Literals.blockSize then
                let tailIdx =
                    idx
                    - this.TailOffset

                for i = tailLen
                        - 1 downto tailIdx do
                    this.Tail.[i + 1] <- this.Tail.[i]

                this.Tail.[tailIdx] <- newItem

                this.Count <-
                    this.Count
                    + 1

                this :> RRBVector<'T>
            else
                let newLeafItems, newTailItem =
                    this.Tail
                    |> Array.copyAndInsertIntoFullArray
                        (idx
                         - this.TailOffset)
                        newItem

                let newTail = Array.zeroCreate Literals.blockSize
                newTail.[0] <- newTailItem
                let newLeafNode = RRBNode<'T>.MkLeaf this.Owner newLeafItems :?> RRBLeafNode<'T>

                let newRoot, newShift =
                    (this.Root :?> RRBFullNode<'T>).AppendLeaf this.Owner this.Shift newLeafNode

                if
                    not
                    <| isSameObj newRoot this.Root
                then
                    this.Root <- newRoot

                this.Shift <- newShift
                this.TailOffset <- this.Count

                this.Count <-
                    this.Count
                    + 1

                this.Tail <- newTail
                // Pushing a full tail down into a leaf can't break the invariant, so no need to adjust the tree here
                this :> RRBVector<'T>
        else
            let newRoot, newShift =
                match this.Root.InsertedTree this.Owner this.Shift idx newItem None 0 with
                | SimpleInsertion(newCurrent) -> newCurrent, this.Shift
                | SplitNode(newCurrent, newRight) ->
                    (newRight :?> RRBFullNode<'T>).NewParent this.Owner this.Shift [|
                        newCurrent
                        newRight
                    |],
                    (RRBMath.up this.Shift)
                // Was: | SplitNode(newCurrent, newRight) -> (newCurrent :?> RRBFullNode<'T>).NewParent this.Owner this.Shift [|newCurrent; newRight|], (RRBMath.up this.Shift)
                // (Note "newCurrent", not newRight, as instance on which NewParent is called)
                | SlidItemsLeft(newLeft, newCurrent) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen
                | SlidItemsRight(newCurrent, newRight) -> failwith "Impossible" // TODO: Write full error message in case this ever manages to happen

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

            this.Shift <- newShift

            this.Count <-
                this.Count
                + 1

            this.TailOffset <-
                this.TailOffset
                + 1

            this.AdjustTree() // TODO: Remove this AdjustTree() call and prove that tests fail when we do that (inserting into last leaf could cause invariant failure)

    // abstract member Remove : int -> RRBVector<'T>
    override this.Remove idx = this.RemoveImpl idx true

    member internal this.RemoveWithoutRebalance idx = this.RemoveImpl idx false

    member internal this.RemoveImpl idx shouldCheckForRebalancing =
        this.ThrowIfNotValid()
        this.EnsureValidIndex idx

        if
            idx
            >= this.TailOffset
        then
            let tailLen =
                this.Count
                - this.TailOffset

            if tailLen > 1 then
                let tailIdx =
                    idx
                    - this.TailOffset

                for i = tailIdx to tailLen
                                   - 2 do
                    this.Tail.[i] <- this.Tail.[i + 1]

                this.Tail.[tailLen
                           - 1] <- Unchecked.defaultof<'T>

                this.Count <-
                    this.Count
                    - 1

                this :> RRBVector<'T>
            elif this.Count = 1 then
                this.Empty()
            else
                // Tail is now empty, so promote a new tail
                let newTailNode, newRoot =
                    (this.Root :?> RRBFullNode<'T>).PopLastLeaf this.Owner this.Shift

                newTailNode.Items.CopyTo(this.Tail, 0)

                Array.fill
                    this.Tail
                    (newTailNode.NodeSize)
                    (tailLen
                     - newTailNode.NodeSize
                     |> max 0)
                    Unchecked.defaultof<'T>

                if
                    not
                    <| isSameObj newRoot this.Root
                then
                    this.Root <- newRoot

                this.Count <-
                    this.Count
                    - 1

                this.TailOffset <-
                    this.TailOffset
                    - newTailNode.NodeSize

                this.AdjustTree()
        else
            let newRoot =
                this.Root.RemovedItem this.Owner this.Shift shouldCheckForRebalancing idx

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

            this.Count <-
                this.Count
                - 1

            this.TailOffset <-
                this.TailOffset
                - 1

            this.AdjustTree()

    // abstract member Update : int -> 'T -> RRBVector<'T>
    override this.Update idx newItem =
        this.ThrowIfNotValid()
        this.EnsureValidIndex idx

        if
            idx
            >= this.TailOffset
        then
            this.Tail.[idx
                       - this.TailOffset] <- newItem
        else
            let newRoot = this.Root.UpdatedTree this.Owner this.Shift idx newItem

            if
                not
                <| isSameObj newRoot this.Root
            then
                this.Root <- newRoot

        this :> RRBVector<'T>

    // abstract member GetItem : int -> 'T
    override this.GetItem idx =
        this.ThrowIfNotValid()
        this.EnsureValidIndex idx

        if
            idx
            >= this.TailOffset
        then
            this.Tail.[idx
                       - this.TailOffset]
        else
            this.Root.GetTreeItem this.Shift idx

    member this.EnsureValidIndex idx =
        if idx < 0 then
            failwith "Index must not be negative"
        elif
            idx
            >= this.Count
        then
            failwith "Index must not be past the end of the vector"
        else
            ()

    member this.EnsureValidIndexLengthAllowed idx =
        if idx < 0 then
            failwith "Index must not be negative"
        elif idx > this.Count then
            failwith "Index must not be more than one past the end of the vector"
        else
            ()

