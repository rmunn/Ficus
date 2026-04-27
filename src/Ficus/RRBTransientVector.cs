using System;
using System.Collections.Generic;
using System.Linq;
using Ficus.RRBArrayExtensions;

namespace Ficus;

internal sealed class RRBTransientVector<T> : RRBVector<T>
{
    // private int _count;
    // private int _shift;
    // private RRBNode<T> _root;
    // private T[] _tail;
    // private int _tailOffset;

    public OwnerToken Owner { get; internal set; }

    public int Count { get; internal set; }
    public int Shift { get; internal set; }
    // TODO: Consider specifying that the root must always be an RRBFullNode<'T>; see RRBPersistentVector for reasons why.
    public RRBNode<T> Root { get; internal set; }
    public T[] Tail { get; internal set; }
    public int TailOffset { get; internal set; }

    public RRBTransientVector(int count, int shift, RRBNode<T> root, T[] tail, int tailOffset)
    {
        Owner = root.Owner;
        Count = count;
        Shift = shift;
        Root = root;
        Tail = tail;
        TailOffset = tailOffset;
    }

    internal RRBTransientVector(OwnerToken token)
    {
        Root = new RRBExpandedFullNode<T>(token, [], 0);
        Tail = new T[Literals.blockSize];
        Count = 0;
        Shift = Literals.shiftSize;
        TailOffset = 0;
        Owner = token;
    }

    public static RRBTransientVector<T> MkEmpty() => new RRBTransientVector<T>(OwnerTokens.MkOwnerToken());
    internal static RRBTransientVector<T> MkEmptyWithToken(OwnerToken token) => new RRBTransientVector<T>(token);

    // TODO: #if DEBUG here
    public override string StringRepr => this.ToString();

    public override int Length => this.Count;


    public void Invalidate()
    {
        Owner = OwnerTokens.NullOwner;
    }

    public void ThrowIfNotValid(string msg = "any operations")
    {
        if (ReferenceEquals(Owner, OwnerTokens.NullOwner)) throw new InvalidOperationException($"This vector is no longer valid for {msg}");
        // TODO: if (Owner is null) instead?
    }

    public override string ToString()
    {
        var ownerStr = ReferenceEquals(Owner, OwnerTokens.NullOwner) ? "<null>" : Owner.ToString(); // TODO: Get some sort of numeric ID or something
        return $"RRBTransientVector<owner={ownerStr},length={Count},shift={Shift},tailOffset={TailOffset},root={Root},tail={Tail}>";
    }

    public override RRBVector<T> Transient() => this;

    public override RRBVector<T> Persistent()
    {
        ThrowIfNotValid(); // TODO: Or perhaps this should just say "Hey, yeah, this one is already persisent so here you go"?
        // TODO: Consider that, then see what regression tests start passing when you do that

        var newRoot = ((RRBFullNode<T>)Root).ShrinkRightSpine(OwnerTokens.NullOwner, Shift);

        int tailLen = Count - TailOffset;
        T[] newTail = tailLen == Literals.blockSize ? Tail : Tail.Truncate(tailLen);
        // TODO: Verify whether we need to copy the tail at all times, or whether it's safe to reuse it if it happened to be blockSize in length
        Invalidate();
        return new RRBPersistentVector<T>(Count, Shift, newRoot, newTail, TailOffset);
    }

    private RRBVector<T> AdjustTree()
    {
        var v = ShiftNodesFromTailIfNeeded();
        return ((RRBTransientVector<T>)v).ShortenTree();
    }

    private RRBVector<T> ShortenTree()
    {
        // Twig-level trees are short enough already
        if (Shift <= Literals.shiftSize) return this;

        // Non-singleton roots don't need to be shortened
        if (Root.NodeSize > 1) return this;

        if (Root.NodeSize == 1)
        {
            // We're a transient, so shorten destructively with mutations
            Shift = RRBMath.Down(Shift);
            Root = ((RRBFullNode<T>)Root).FirstChild;
            return ShortenTree(); // Might need to recurse
        }

        // If we got here, root was empty but shift was too large
        // TODO: Delete this case and see what tests fail, then create regression test to trigger this scenario
        Shift = Literals.shiftSize;
        return this;
    }

    private RRBVector<T> ShiftNodesFromTailIfNeeded()
    {
        // Empty roots don't need to shift
        if (TailOffset <= 0 || Root.NodeSize == 0) return this;

        // If parent of last leaf is a relaxed node, this automatically satisfies the invariant
        var lastTwig = ((RRBFullNode<T>)Root).RightmostTwig(Shift);
        if (lastTwig is RRBRelaxedNode<T>) return this;

        var lastLeaf = (RRBLeafNode<T>)lastTwig.LastChild;

        int shiftCount = Literals.blockSize - lastLeaf.NodeSize;
        int tailLen = Count - TailOffset;

        // Last leaf is full, so no shifting needed
        if (shiftCount <= 0) return this;

        if (shiftCount >= tailLen)
        {
            // Would shift everything out of the tail, so instead we'll promote a new tail
            var (removedLeaf, newRoot) = ((RRBFullNode<T>)Root).RemoveLastLeaf(Owner, Shift);

            int removedSize = removedLeaf.NodeSize;

            // Shift current tail items out of the way of incoming ones
            for (int i = tailLen - 1; i >= 0; i--)
            {
                Tail[i + removedSize] = Tail[i];
            }
            // TODO: Check if overlapping copies are allowed, might be more efficient if Array.Copy can use memmove or the equivalent
            // Array.Copy(Tail, 0, Tail, removedSize, tailLen);

            // Now put the leaf's items into the start of the tail
            Array.Copy(removedLeaf.Items, 0, Tail, 0, removedSize);

            TailOffset -= removedSize;

            // It's possible we didn't need to change the root
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

            // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6, which then needs to become M T11.
            return ShiftNodesFromTailIfNeeded();
        }
        else
        {
            // Tail will end up being split, last leaf will become full
            var itemsToShift = Tail[..shiftCount];

            for (int i = 0; i < tailLen - shiftCount; i++)
            {
                Tail[i] = Tail[i + shiftCount];
            }
            // TODO: Check if overlapping copies are allowed, might be more efficient if Array.Copy can use memmove or the equivalent
            // Array.Copy(Tail, shiftCount, Tail, 0, tailLen - shiftCount);
            Array.Clear(Tail, tailLen - shiftCount, shiftCount); // TODO: Check that this is the right method to fill with null/default T values

            // TODO: Write Array.Append or find the correct C# call
            var newLeaf = new RRBLeafNode<T>(Owner, lastLeaf.Items.Append(itemsToShift));
            var newRoot = ((RRBFullNode<T>)Root).ReplaceLastLeaf(Owner, Shift, newLeaf, shiftCount);

            // Skip assignment if the root didn't change
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

            TailOffset += shiftCount;
            return this;
        }
    }

    public override RRBVector<T> Empty()
    {
        ThrowIfNotValid();

        Count = 0;
        Shift = Literals.shiftSize;
        var root = (RRBFullNode<T>)Root;
        root.SetNodeSize(0);
        Array.Clear(root.Children, 0, Literals.blockSize);

        Root = root; // Shouldn't be necessary
        // TODO: Remove that assignment and prove that no tests fail

        Array.Clear(Tail, 0, Literals.blockSize);
        TailOffset = 0;

        return this as RRBVector<T>;
    }

    public override bool IsEmpty()
    {
        return Count == 0;
    }

    public override IEnumerable<T[]> IterLeaves()
    {
        ThrowIfNotValid();

        var root = (RRBFullNode<T>)Root;

        foreach (var leaf in root.LeavesSeq(Shift))
        {
            yield return leaf.Items;
        }

        yield return Tail.Truncate(Count - TailOffset);
    }

    public IEnumerable<T[]> IterEditableLeavesWithoutTail()
    {
        ThrowIfNotValid();

        var owner = Owner;
        var root = (RRBFullNode<T>)Root;

        return root
            .LeavesSeq(Shift)
            .Select(leaf => ((RRBLeafNode<T>)leaf.GetEditableNode(owner)).Items);
    }

    public override IEnumerable<T[]> RevIterLeaves()
    {
        ThrowIfNotValid();

        yield return Tail.Truncate(Count - TailOffset);
        var root = (RRBFullNode<T>)Root;
        foreach (var leaf in root.RevLeavesSeq(Shift))
        {
            yield return leaf.Items;
        }
    }

    public IEnumerable<T[]> RevIterEditableLeavesWithoutTail()
    {
        ThrowIfNotValid();

        var owner = Owner;
        var root = (RRBFullNode<T>)Root;

        return root
            .RevLeavesSeq(Shift)
            .Select(leaf => ((RRBLeafNode<T>)leaf.GetEditableNode(owner)).Items);
    }

    public override IEnumerable<T> IterItems()
    {
        ThrowIfNotValid();

        foreach (var arr in IterLeaves())
            foreach (var x in arr)
                yield return x;
        // TODO: Do IterLeaves without tail, then enumerate tail without doing the new array thing
    }

    public override IEnumerable<T> RevIterItems()
    {
        ThrowIfNotValid();

        foreach (var arr in RevIterLeaves())
            for (int i = arr.Length - 1; i >= 0; i--)
                yield return arr[i];
    }

    public override RRBVector<T> Push(T item)
    {
        ThrowIfNotValid();

        int tailLen = Count - TailOffset;

        if (tailLen < Literals.blockSize)
        {
            // Room to grow, easy and efficient case
            Tail[tailLen] = item;
            Count++;
            return this;
        }

        // No room to grow tail, push it down and create new one-item tail (but full-sized for future push efficiency)
        var tailNode = RRBNode<T>.MkLeaf(Owner, Tail);

        var (newRoot, newShift) = ((RRBFullNode<T>)Root).AppendLeaf(Owner, Shift, (RRBLeafNode<T>)tailNode);
        if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

        TailOffset = Count;
        Count++;
        Shift = newShift;
        Tail = new T[Literals.blockSize];
        Tail[0] = item;

        return this;
    }

    public override T Peek()
    {
        ThrowIfNotValid();

        if (Count <= 0) throw new InvalidOperationException("Can't get last item from an empty vector");

        int tailLen = Count - TailOffset;
        return Tail[tailLen - 1];
    }

    public override RRBVector<T> Pop()
    {
        ThrowIfNotValid();

        if (Count <= 0) throw new InvalidOperationException("Can't pop from an empty vector");

        if (Count == 1) return Empty();

        int tailLen = Count - TailOffset;
        if (tailLen > 1)
        {
            Tail[tailLen - 1] = default!;
            Count--;
            return this;
        }

        var (newTailNode, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(Owner, Shift);

        if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

        TailOffset -= newTailNode.NodeSize;
        Tail = newTailNode.Items.ExpandToBlockSize(Literals.blockSize);

        Count--;
        return AdjustTree();
    }

    public override RRBVector<T> Take(int idx)
    {
        ThrowIfNotValid();

        // Taking everything = unchanged tree and tail
        if (idx >= Count) return this;

        // Taking nothing = empty vector
        if (idx <= 0) return Empty();

        if (idx == TailOffset)
        {
            // Dropping the tail and nothing else, so we promote a new tail
            var (newTailNode, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(Owner, Shift);

            Count = idx;
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

            TailOffset = idx - newTailNode.NodeSize;
            Tail = newTailNode.GetEditableArrayOfBlockSizeLength(Owner);

            return this.AdjustTree();
        }
        else if (idx > TailOffset)
        {
            // Splitting the tail in two
            Count = idx;
            int newTailLen = idx - TailOffset;

            Array.Clear(Tail, newTailLen, Literals.blockSize - newTailLen);

            return this;
        }
        else
        {
            var tmpRoot = Root.KeepNTreeItems(Owner, Shift, idx);

            var (newTailNode, newRoot) = ((RRBFullNode<T>)tmpRoot).PopLastLeaf(Owner, Shift);
            var newRootExpanded = ((RRBFullNode<T>)newRoot).MaybeExpand(Owner, Shift);

            Count = idx;

            if (!ReferenceEquals(newRootExpanded, Root)) Root = newRootExpanded;

            TailOffset = idx - newTailNode.NodeSize;
            Tail = newTailNode.GetEditableArrayOfBlockSizeLength(Owner);

            return this.AdjustTree();
        }
    }

    public override RRBVector<T> Skip(int idx)
    {
        ThrowIfNotValid();

        // Simple cases: skipping nothing, skipping everything
        if (idx <= 0) return this;
        if (idx >= Count) return Empty();

        else if (idx == TailOffset)
        {
            // Splitting exactly at the tail means we'll have an empty root
            var root = (RRBFullNode<T>)Root;

            root.SetNodeSize(0);
            Array.Clear(root.Children, 0, Literals.blockSize);

            Count -= idx;
            Shift = Literals.shiftSize;
            TailOffset = 0;

            return this as RRBVector<T>;
        }
        else if (idx > TailOffset)
        {
            // Splitting the tail in two
            var root = (RRBFullNode<T>)Root;

            root.SetNodeSize(0);
            Array.Clear(root.Children, 0, Literals.blockSize);

            int tailLen = Count - TailOffset;
            int tailIdx = idx - TailOffset;

            Count -= idx;
            Shift = Literals.shiftSize;

            // Shift remaining items down, then zero out the slots that have become empty
            for (int i = tailIdx; i < tailLen; i++)
            {
                Tail[i - tailIdx] = Tail[i];
            }
            Array.Clear(Tail, tailLen - tailIdx, tailIdx);

            TailOffset = 0;

            return this;
        }
        else
        {
            // Splitting root somewhere, tail remains same
            var newRoot = Root.SkipNTreeItems(Owner, Shift, idx);
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

            Count -= idx;
            TailOffset -= idx;

            return this.AdjustTree();
        }
    }

    public override (RRBVector<T>, RRBVector<T>) Split(int idx)
    {
        ThrowIfNotValid();
        // NOTE: Remember that in transients, we need to create the right vector with the same owner token
        // And "this" must remain the left vector. So when idx = 0, we can't just do "this.Empty()" as that
        // would erase the root. Instead, we must first copy the node and hand the copy to right to be its
        // root, or else hand our original node to right and make a brand-new empty node for "this". Don't
        // know yet which is better. If anyone had pointers to our original root... well, they shouldn't.
        // And if anyone had pointers to nodes further down in the tree, copying the root won't do any harm.
        EnsureValidIndexLengthAllowed(idx);

        if (idx == Count)
        {
            // Left (this) unchanged, right is new empty vector
            var right = new RRBTransientVector<T>(Owner);
            return (this, right);
        }
        else if (idx == 0)
        {
            // Left (this) empty, right gets whole thing
            var right = new RRBTransientVector<T>(Count, Shift, Root, Tail, TailOffset);

            // Now erase this (but don't just do this.Empty() as that would erase the root that right will inherit)
            Count = 0;
            Shift = Literals.shiftSize;
            Root = new RRBExpandedFullNode<T>(Owner, new RRBNode<T>[Literals.blockSize], 0);
            Tail = new T[Literals.blockSize];
            TailOffset = 0;

            return (this, right);
        }
        else if (idx == TailOffset)
        {
            // Splitting exactly at the tail means we have to promote a new tail for the left (this)
            var right = new RRBTransientVector<T>(Owner);
            right.Tail = Tail;

            right.Count = Count - TailOffset;

            var (newTailNode, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(Owner, Shift);
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;
            Tail = newTailNode.GetEditableArrayOfBlockSizeLength(Owner);

            Count = idx;
            TailOffset = idx - newTailNode.NodeSize;

            return (this.AdjustTree(), right);
        }
        else if (idx > TailOffset)
        {
            // Splitting the tail in two, so right becomes tail-only while left gets rest of tail (aund unchanged root)
            int tailIdx = idx - TailOffset;
            // int tailLen = Count - TailOffset;
            // int rightTailLen = tailLen - tailIdx;
            int rightTailLen = Count - idx;


            var right = new RRBTransientVector<T>(Owner);

            Array.Copy(Tail, tailIdx, right.Tail, 0, rightTailLen);
            Array.Clear(Tail, tailIdx, rightTailLen);

            Count = idx;
            right.Count = rightTailLen;

            return (this, right);
        }
        else
        {
            // Split inside tree; right gets tail while left gets a promoted tail
            var (rootL, rootR) = Root.SplitTree(Owner, Shift, idx);

            var right = new RRBTransientVector<T>(Count - idx, Shift, rootR, Tail, TailOffset - idx);

            var (newTailNodeL, newRootL) = ((RRBFullNode<T>)rootL).PopLastLeaf(Owner, Shift);
            if (!ReferenceEquals(newRootL, Root)) Root = newRootL;

            Count = idx;
            Tail = newTailNodeL.GetEditableArrayOfBlockSizeLength(Owner);
            TailOffset = idx - newTailNodeL.NodeSize;

            // Have to adjust the tree for both "this" AND "right" in this one, since either one could have become a tall, thin tree
            return (this.AdjustTree(), right.AdjustTree());
        }
    }

    public override RRBVector<T> Slice(int start, int length)
    {
        ThrowIfNotValid();

        return Skip(start).Take(length);
    }

    public override RRBVector<T> Append(RRBVector<T> other)
    {
        ThrowIfNotValid();

        if (other is RRBTransientVector<T> right && Root.IsEditableBy(right.Owner))
        {
            if (Count == 0)
            {
                // Calling code expects "this" to contain the results, so we steal everything from the right-hand tree
                Root = right.Root;
                Shift = right.Shift;
                Count = right.Count;
                TailOffset = right.TailOffset;
                Tail = right.Tail;

                // TODO: Whoops, this Invalidate() call is probably the cause of some of the test failures. Because now that *we* own the right root, the right getting invalidated means that *we* get invalidated too, which we shouldn't.
                // TODO: ... Or maybe not? It will reset the owner token of the right-hand side, but NOT the left side: Invalidate() doesn't touch the root node.
                right.Invalidate();
                return this;
            }

            if (right.Count == 0)
            {
                right.Invalidate();
                return this;
            }

            int newLen = Count + right.Count;

            if (right.TailOffset <= 0)
            {
                // Right is a tail-only vector
                int tailLenL = Count - TailOffset;
                int tailLenR = right.Count - right.TailOffset;

                if (tailLenL + tailLenR <= Literals.blockSize)
                {
                    // And it fits into left without needing to touch the tree
                    Array.Copy(right.Tail, 0, Tail, tailLenL, tailLenR);
                    Count = newLen;

                    right.Invalidate();
                    return this;
                }
                else
                {
                    // Both tails combined are more than the total tail, so we need to push down a new leaf
                    var newTail = new T[Literals.blockSize];

                    int splitIdxR = Literals.blockSize - tailLenL;

                    // Fill current tail up to maximum size, preparing to make it a leaf
                    Array.Copy(right.Tail, 0, Tail, tailLenL, splitIdxR);
                    // Rest of right.Tail goes into the new tail array
                    Array.Copy(right.Tail, splitIdxR, newTail, 0, tailLenR - splitIdxR);

                    var newLeaf = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(Owner, Tail);
                    Tail = newTail;

                    var (newRoot, newShift) = ((RRBFullNode<T>)Root).AppendLeaf(Owner, Shift, newLeaf);

                    Count = newLen;

                    if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

                    Shift = newShift;
                    TailOffset += newLeaf.NodeSize;

                    right.Invalidate();
                    return this;
                }
            }
            else if (TailOffset <= 0)
            {
                // Right has a root and a tail, but we're a tail-only node: create new leaf from our tail and put it in right-most tree
                int tailLen = Count - TailOffset;

                // Transient tails are expanded, but leaves are not supposed to be expanded, so we shrink the leaf before inserting it
                var tailArray = tailLen < Literals.blockSize ? Tail.Truncate(tailLen) : Tail;
                var tailNode = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(Owner, tailArray);
                var (newRoot, newShift) = ((RRBFullNode<T>)right.Root).PrependLeaf(right.Owner, right.Shift, tailNode);
                if (!ReferenceEquals(newRoot, right.Root)) right.Root = newRoot;

                // Calling code expects "this" to contain the results, so we steal everything from the right-hand tree
                Root = right.Root;
                Shift = newShift;
                Count = newLen;
                TailOffset = right.TailOffset + tailNode.NodeSize;
                Tail = right.Tail;

                right.Invalidate();
                return this;
            }
            else
            {
                // Right has a root and a tail, and so do we
                int tailLen = Count - TailOffset;

                var tailArray = tailLen < Literals.blockSize
                    ? Tail.Truncate(tailLen)
                    : Tail;

                var tailNode = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(Owner, tailArray);

                var leftFull = (RRBFullNode<T>)Root;
                var rightFull = (RRBFullNode<T>)right.Root;

                // Can the tail be merged into the two twig nodes? Now's the time to find out, while we can still push it down to form a new root
                bool tailCanFit = leftFull.RightmostTwig(Shift).HasRoomToMergeTheTail(
                    Literals.shiftSize,
                    tailNode,
                    rightFull.LeftmostTwig(right.Shift)
                );

                RRBNode<T> newRoot;
                int newShift;

                if (tailCanFit)
                {
                    int mergedShift = Math.Max(Shift, right.Shift);

                    var (l, r) = leftFull.MergeTree(Owner, Shift, tailNode, right.Shift, rightFull, true);

                    if (r == null)
                    {
                        newRoot = l;
                        newShift = mergedShift;
                    }
                    else
                    {
                        newRoot = ((RRBFullNode<T>)r).NewParent(Owner, mergedShift, new[] { l, r });
                        newShift = RRBMath.Up(mergedShift);
                    }
                }
                else
                {
                    var (tmpRoot, tmpShift) = leftFull.AppendLeaf(Owner, Shift, tailNode);
                    int mergedShift = Math.Max(tmpShift, right.Shift);

                    var (l, r) = ((RRBFullNode<T>)tmpRoot).MergeTree(Owner, tmpShift, null, right.Shift, rightFull, true);

                    if (r == null)
                    {
                        newRoot = l;
                        newShift = mergedShift;
                    }
                    else
                    {
                        newRoot = ((RRBFullNode<T>)r).NewParent(Owner, mergedShift, new[] { l, r });
                        newShift = RRBMath.Up(mergedShift);
                    }
                }

                TailOffset = Count + right.TailOffset;
                Count = newLen;
                Shift = newShift;
                Root = newRoot;
                Tail = right.Tail;

                right.Invalidate();
                return this.AdjustTree();
            }
        }
        else if (other is RRBTransientVector<T> t)
        {
            // Transient vectors may only stay transient if appended to a transient of the same owner.
            // Two transients from *different owners* must be considered unsafe to merge as transients,
            // so we must convert both of them to persistent, invalidating them, *before* the merge.
            return Persistent().Append(t.Persistent());
            // TODO: Reexamine this decision. Since we're invalidating the right tree anyway, is it possible
            // to just keep the left one transient? Think about scenarios where that could cause problems.
        }
        else if (other is RRBPersistentVector<T> p)
        {
            return Persistent().Append(p);
        }
        else
        {
#if DEBUG
            throw new InvalidOperationException("How did we get here? Do we have a new RRBVector subclass?");
#else
            return Persistent().Append(other);
#endif
        }
    }

    public override RRBVector<T> Insert(int idx, T newItem)
    {
        ThrowIfNotValid();
        EnsureValidIndexLengthAllowed(idx);

        if (idx >= TailOffset)
        {
            // Inserting into tail
            if (idx == Count)
            {
                // Inserting at the end is just pushing, which can be done more efficiently. Bail out.
                return Push(newItem);
            }

            int tailLen = Count - TailOffset;
            if (tailLen < Literals.blockSize)
            {
                // Tail has room for new item
                int tailIdx = idx - TailOffset;

                // Shift elements in the tail to the right to make space for new item
                for (int i = tailLen - 1; i >= tailIdx; i--)
                {
                    Tail[i + 1] = Tail[i];
                }

                Tail[tailIdx] = newItem;

                Count = Count + 1;

                return this;
            }
            else
            {
                // Tail is full. Insert the item first, catching the overflow
                var (newLeafItems, newTailItem) = Tail.CopyAndInsertIntoFullArray(idx - TailOffset, newItem);

                // Overflow becomes a new single-item tail
                var newTail = new T[Literals.blockSize];
                newTail[0] = newTailItem;

                // Old tail array (after insert) becomes new rightmost leaf
                var newLeafNode = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(Owner, newLeafItems);
                var (newRoot, newShift) = ((RRBFullNode<T>)Root).AppendLeaf(Owner, Shift, newLeafNode);

                if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

                Shift = newShift;
                TailOffset = Count;
                Count = Count + 1;
                Tail = newTail;

                // Pushing a full tail down into a leaf can't break the invariant, so no need to adjust the tree here
                return this;
            }
        }
        else
        {
            // Inserting into middle of tree. Might result in splitting the tree
            var result = Root.InsertedTree(Owner, Shift, idx, newItem, null, 0);
            RRBNode<T> newRoot; // TODO: RRBFullNode<T> once we switch the root to be a FullNode
            int newShift;

            switch (result)
            {
                case SimpleInsertion<RRBNode<T>> simpleInsertion:
                    newRoot = simpleInsertion.NewCurrent;
                    newShift = Shift;
                    break;

                case SplitNode<RRBNode<T>> splitNode:
                    newRoot = ((RRBFullNode<T>)splitNode.NewRight).NewParent(Owner, Shift, new[] { splitNode.NewCurrent, splitNode.NewRight });
                    newShift = RRBMath.Up(Shift);
                    break;
#if DEBUG
                case SlidItemsLeft<RRBNode<T>> slidItemsLeft:
                    throw new InvalidOperationException("Impossible case: SlidItemsLeft in Insert() of transient vector");

                case SlidItemsRight<RRBNode<T>> slidItemsRight:
                    throw new InvalidOperationException("Impossible case: SlidItemsRight in Insert() of transient vector");

                default:
                    throw new InvalidOperationException("Impossible case: unknown insertion result in Insert() of transient vector");
#endif
            }

            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;

            Shift = newShift;
            Count = Count + 1;
            TailOffset = TailOffset + 1;

            // TODO: Remove this AdjustTree() call and prove that tests fail when we do that (inserting into last leaf could cause invariant failure)
            return this.AdjustTree();
        }
    }

    public override RRBVector<T> Remove(int idx)
    {
        return RemoveImpl(idx, true);
    }

    internal RRBVector<T> RemoveWithoutRebalance(int idx)
    {
        return RemoveImpl(idx, false);
    }

    internal RRBVector<T> RemoveImpl(int idx, bool shouldCheckForRebalancing)
    {
        ThrowIfNotValid();
        EnsureValidIndex(idx);

        if (idx >= TailOffset)
        {
            // Removing from tail
            int tailLen = Count - TailOffset;

            if (tailLen > 1)
            {
                // Tail will still be non-empty after remove
                int tailIdx = idx - TailOffset;

                // Shift all items after the removed one down to fill the gap
                for (int i = tailIdx; i <= tailLen - 2; i++) Tail[i] = Tail[i + 1];

                // Clear final slot
                Tail[tailLen - 1] = default!;
                Count = Count - 1;
                // No need to call AdjustTree(), tree hasn't changed
                return this;
            }
            else if (Count == 1)
            {
                // Removing only item
                return Empty();
            }
            else
            {
                // Tail is now empty, so promote a new tail
                var (newTailNode, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(Owner, Shift);
                int newTailLen = newTailNode.NodeSize;
                newTailNode.Items.CopyTo(Tail, 0);

                int remainingSpace = Literals.blockSize - newTailLen;
                if (remainingSpace > 0) Array.Clear(Tail, newTailLen, remainingSpace);

                if (!ReferenceEquals(newRoot, Root)) Root = newRoot;
                Count = Count - 1;
                TailOffset = TailOffset - newTailLen;
                // Popping tail might have broken invariant, so check and adjust
                return this.AdjustTree();
                // TODO: Delete this AdjustTree() and make tests fail, then create targeted regression test aimed here
            }
        }
        else
        {
            // Removing from middle of tree, tail remains unchanged
            var newRoot = Root.RemovedItem(Owner, Shift, shouldCheckForRebalancing, idx);
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;
            Count = Count - 1;
            TailOffset = TailOffset - 1;
            // Removing items can break invariant (tree becomes too skinny), so AdjustTree() is needed here
            return this.AdjustTree();
            // TODO: Delete this AdjustTree() and make tests fail, then create targeted regression test for that scenario
        }
    }

    // abstract member Update : int -> 'T -> RRBVector<'T>
    public override RRBVector<T> Update(int idx, T newItem)
    {
        ThrowIfNotValid();
        EnsureValidIndex(idx);

        if (idx >= TailOffset)
        {
            // Updating item in tail
            Tail[idx - TailOffset] = newItem;
        }
        else
        {
            // Updating item in tree
            var newRoot = Root.UpdatedTree(Owner, Shift, idx, newItem);
            if (!ReferenceEquals(newRoot, Root)) Root = newRoot;
        }
        return this;
    }

    // abstract member GetItem : int -> 'T
    public override T GetItem(int idx)
    {
        ThrowIfNotValid();
        EnsureValidIndex(idx);

        if (idx >= TailOffset)
        {
            return Tail[idx - TailOffset];
        }
        else
        {
            return Root.GetTreeItem(Shift, idx);
        }
    }

    private void EnsureValidIndex(int idx)
    {
        if (idx < 0) throw new IndexOutOfRangeException("Index must not be negative");
        if (idx >= Count) throw new IndexOutOfRangeException("Index must not be past the end of the vector");
    }

    private void EnsureValidIndexLengthAllowed(int idx)
    {
        if (idx < 0) throw new IndexOutOfRangeException("Index must not be negative");
        if (idx > Count) throw new IndexOutOfRangeException("Index must not be more than one past the end of the vector");
    }
}
