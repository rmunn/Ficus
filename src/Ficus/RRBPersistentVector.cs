using System;
using System.Collections.Generic;
using Ficus.RRBArrayExtensions;

namespace Ficus;

internal sealed class RRBPersistentVector<T> : RRBVector<T>
{
    internal readonly int Count;
    internal readonly int Shift;
    // TODO: Consider specifying that the root must always be an RRBFullNode<'T>, so we don't have to do nearly as many casts of Root
    // We have already decided that Shift must always be at least 5 (no leaf nodes in the root), so no need for this to be an RRBNode<T>
    // since it cannot be a leaf.
    // Alternately, we could allow leaf nodes at the root and reconsider ALL of our logic. It would be more efficient for vectors of size 33-64.
    // But then, it would not gain anything for vectors of size 0-32, and those are common. And it would make the logic more complex in many
    // places, creating lots and lots of room for bugs. At the moment I'm thinking no.
    internal readonly RRBNode<T> Root;
    internal readonly T[] Tail;
    internal readonly int TailOffset;

    // TODO: Shouldn't this one be internal?
    public RRBPersistentVector(int count, int shift, RRBNode<T> root, T[] tail, int tailOffset)
    {
        Count = count;
        Shift = shift;
        Root = root;
        Tail = tail;
        TailOffset = tailOffset;
    }

    internal RRBPersistentVector(OwnerToken token)
    {
        var root = new RRBFullNode<T>(token, []);
        Count = 0;
        Shift = Literals.shiftSize;
        Root = root;
        Tail = [];
        TailOffset = 0;
    }

    public RRBPersistentVector() : this(OwnerTokens.NullOwner) { }

    public static RRBPersistentVector<T> MkEmpty() => new RRBPersistentVector<T>();

    internal static RRBPersistentVector<T> MkEmptyWithToken(OwnerToken token)
        => new RRBPersistentVector<T>(token);

    public override string ToString()
        => $"RRBPersistentVector<length={Count},shift={Shift},tailOffset={TailOffset},root={Root},tail={Tail}>";

    public override RRBVector<T> Persistent() => this;

    public override RRBVector<T> Transient()
    {
        var newToken = OwnerTokens.MkOwnerToken();

        var newRoot = ((RRBFullNode<T>)Root).ExpandRightSpine(newToken, Shift);
        var newTail = new T[Literals.blockSize];
        Tail.CopyTo(newTail, 0);

        return new RRBTransientVector<T>(Count, Shift, newRoot, newTail, TailOffset);
    }

    internal RRBVector<T> AdjustTree()
    {
        var v = ShiftNodesFromTailIfNeeded();
        return ((RRBPersistentVector<T>)v).ShortenTree();
    }

    internal RRBVector<T> ShortenTree()
    {
        // Twig roots shouldn't be shortened, we don't allow leaf roots
        if (Shift <= Literals.shiftSize) return this;

        // Non-singleton roots also don't need to be shortened
        if (Root.NodeSize > 1) return this;

        if (Root.NodeSize == 1)
        {
            var newRoot = ((RRBFullNode<T>)Root).FirstChild;
            return new RRBPersistentVector<T>(Count, RRBMath.Down(Shift), newRoot, Tail, TailOffset)
                .ShortenTree();
        }

        // If we get here the root was (or has just become) empty
        return new RRBPersistentVector<T>(Count, Literals.shiftSize, RRBFullNode<T>.EmptyNode, Tail, TailOffset);
        // TODO: Decide where emptyNode lives.
    }

    internal RRBVector<T> ShiftNodesFromTailIfNeeded()
    {
        // Empty roots don't need to shift nodes from tail
        if (TailOffset <= 0 || Root.NodeSize == 0) return this;

        var lastTwig = ((RRBFullNode<T>)Root).RightmostTwig(Shift);

        // If parent of last leaf is a relaxed node, this automatically satisfies the invariant
        if (lastTwig is RRBRelaxedNode<T>) return this;

        var lastLeaf = (RRBLeafNode<T>)lastTwig.LastChild;

        int shiftCount = Literals.blockSize - lastLeaf.NodeSize;
        int tailLen = Count - TailOffset;

        if (shiftCount <= 0) return this; // Nothing to do, last leaf was full

        if (shiftCount >= tailLen)
        {
            // Would shift everything out of the tail, so instead we'll promote a new tail
            var (removedLeaf, newRoot) = ((RRBFullNode<T>)Root).RemoveLastLeaf(OwnerTokens.NullOwner, Shift);

            var newTail = Tail.Append(removedLeaf.Items);

            // In certain rare cases, we might need to recurse. For example, the vector [M 5] [5] T1 will become [M 5] T6,
            // which then needs to become M T11. So we have to call .ShiftNodesFromTailIfNeeded() again.
            return new RRBPersistentVector<T>(
                    Count,
                    Shift,
                    newRoot,
                    newTail,
                    TailOffset - removedLeaf.NodeSize)
                .ShiftNodesFromTailIfNeeded();
        }

        // Not enough room to shift everything out of the tail, so we'll still have a tail
        var (itemsToShift, newTailArr) = Tail.SplitAt(shiftCount);

        var newLeaf = new RRBLeafNode<T>(OwnerTokens.NullOwner, lastLeaf.Items.Append(itemsToShift));
        var newRoot2 = ((RRBFullNode<T>)Root).ReplaceLastLeaf(OwnerTokens.NullOwner, Shift, newLeaf, shiftCount);

        // No need to recurse here, though.
        return new RRBPersistentVector<T>(
            Count,
            Shift,
            newRoot2,
            newTailArr,
            TailOffset + shiftCount);
    }

    public override RRBVector<T> Empty()
        => new RRBPersistentVector<T>(0, Literals.shiftSize, RRBFullNode<T>.EmptyNode, [], 0);

    public override bool IsEmpty() => Count == 0;

    // TODO: This is wrong for C#, come back and fix it later
    public override string StringRepr => ToString();

    public override int Length => Count;

    public override IEnumerable<T[]> IterLeaves()
    {
        foreach (var leaf in ((RRBFullNode<T>)Root).LeavesSeq(Shift))
            yield return leaf.Items;

        yield return Tail;
    }

    public override IEnumerable<T[]> RevIterLeaves()
    {
        yield return Tail;

        foreach (var leaf in ((RRBFullNode<T>)Root).RevLeavesSeq(Shift))
            yield return leaf.Items;
    }

    public override IEnumerable<T> IterItems()
    {
        foreach (var arr in IterLeaves())
            foreach (var x in arr)
                yield return x;
    }

    public override IEnumerable<T> RevIterItems()
    {
        foreach (var arr in RevIterLeaves())
            for (int i = arr.Length - 1; i >= 0; i--)
                yield return arr[i];
    }

    public override RRBVector<T> Push(T item)
    {
        int tailLen = Count - TailOffset;

        if (tailLen < Literals.blockSize)
        {
            var newTail = Tail.CopyAndPush(item);
            return new RRBPersistentVector<T>(Count + 1, Shift, Root, newTail, TailOffset);
        }

        // Tail was full, promote it to a leaf and make new one-item tail
        var tailNode = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(OwnerTokens.NullOwner, Tail);

        var (newRoot, newShift) = ((RRBFullNode<T>)Root).AppendLeaf(OwnerTokens.NullOwner, Shift, tailNode);

        return new RRBPersistentVector<T>(
            Count + 1,
            newShift,
            newRoot,
            new[] { item },
            Count);
    }

    public override T Peek()
    {
        if (Count <= 0) throw new InvalidOperationException("Can't get last item from an empty vector");

        int tailLen = Count - TailOffset;
#if DEBUG
        if (tailLen == 0) throw new InvalidOperationException("Invariant broken: tail should never be empty");
#endif
        return Tail[tailLen - 1];
    }

    public override RRBVector<T> Pop()
    {
        if (Count <= 0)
            throw new InvalidOperationException("Can't pop from an empty vector");

        if (Count == 1) return Empty();

        if (Tail.Length > 1)
        {
            // Simple
            var newTail = Tail.CopyAndPop();
            return new RRBPersistentVector<T>(Count - 1, Shift, Root, newTail, TailOffset);
        }

        // Single-item tail means we need to promote last leaf to tail, which might require adjusting the new tree
        var (newTailNode, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(OwnerTokens.NullOwner, Shift);

        return new RRBPersistentVector<T>(
            Count - 1,
            Shift,
            newRoot,
            newTailNode.Items,
            TailOffset - newTailNode.NodeSize)
            .AdjustTree();
    }

    public override RRBVector<T> Take(int idx)
    {
        if (idx >= Count) return this;
        if (idx <= 0) return Empty();

        if (idx == TailOffset)
        {
            // Dropping the tail and nothing else, so we promote a new tail, and might need to adjust the new tree
            var (leaf, root) = ((RRBFullNode<T>)Root).PopLastLeaf(OwnerTokens.NullOwner, Shift);
            return new RRBPersistentVector<T>(idx, Shift, root, leaf.Items, idx - leaf.NodeSize).AdjustTree();
        }

        if (idx > TailOffset)
        {
            // Splitting the tail in two, not touching the main body of the tree
            var newTail = Tail.Truncate(idx - TailOffset);
            return new RRBPersistentVector<T>(idx, Shift, Root, newTail, TailOffset);
        }

        // Have to adjust the tree. First produce a tree with the right number of items, but which doesn't have a tail...
        var tmpRoot = Root.KeepNTreeItems(OwnerTokens.NullOwner, Shift, idx);

        // And then promote the last leaf to a new tail
        var (lastLeaf, newRoot) = ((RRBFullNode<T>)tmpRoot).PopLastLeaf(OwnerTokens.NullOwner, Shift);

        // Tree is likely to need adjusting since there's a good chance we ended up with a shallow path to the root now
        return new RRBPersistentVector<T>(idx, Shift, newRoot, lastLeaf.Items, idx - lastLeaf.NodeSize).AdjustTree();
    }

    public override RRBVector<T> Skip(int idx)
    {
        if (idx <= 0) return this;
        if (idx >= Count) return Empty();

        if (idx == TailOffset)
        {
            // Keeping the tail and nothing else, so we return a new tree with the same tail but an empty root node
            var emptyRoot = RRBNode<T>.MkFullNode(OwnerTokens.NullOwner, Array.Empty<RRBNode<T>>());
            return new RRBPersistentVector<T>(Tail.Length, Literals.shiftSize, emptyRoot, Tail, 0);
            // TODO: Use emptyNode here, it's safe to share because these are persistent. Reduces GC pressure.
        }

        if (idx > TailOffset)
        {
            // Keeping only part of the tail and no part of the tree, so we return a new tree with truncated tail and an empty root node
            var newTail = RRBArrayExtensions.RRBArrayExtensions.Skip(Tail, idx - TailOffset);
            var emptyRoot = RRBNode<T>.MkFullNode(OwnerTokens.NullOwner, Array.Empty<RRBNode<T>>());
            return new RRBPersistentVector<T>(newTail.Length, Literals.shiftSize, emptyRoot, newTail, 0);
            // TODO: Use emptyNode here, it's safe to share because these are persistent. Reduces GC pressure.
        }

        // Keeping the tail, but modifying the root. May require tree adjutsment.
        var newRoot = Root.SkipNTreeItems(OwnerTokens.NullOwner, Shift, idx);
        return new RRBPersistentVector<T>(Count - idx, Shift, newRoot, Tail, TailOffset - idx).AdjustTree();
    }

    public override (RRBVector<T> Left, RRBVector<T> Right) Split(int idx)
    {
        EnsureValidIndexLengthAllowed(idx);

        // Special cases that need zero work
        if (idx == Count) return (this, Empty());
        if (idx == 0) return (Empty(), this);

        if (idx == TailOffset)
        {
            // Splitting exactly at the tail means we have to promote a new tail for the left tree, while the right tree becomes tail-only
            var (newTailNode, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(OwnerTokens.NullOwner, Shift);

            var newLeft = new RRBPersistentVector<T>(idx, Shift, newRoot, newTailNode.Items, idx - newTailNode.NodeSize).AdjustTree();

            var emptyRoot = RRBNode<T>.MkFullNode(OwnerTokens.NullOwner, Array.Empty<RRBNode<T>>());
            var newRight = new RRBPersistentVector<T>(Tail.Length, Literals.shiftSize, emptyRoot, Tail, 0);
            // TODO: Use emptyNode here, it's safe to share because these are persistent. Reduces GC pressure.

            return (newLeft, newRight);
        }
        else if (idx > TailOffset)
        {
            // Splitting the tail in two: left tree has minimal changes (like dropping last few items), right tree ends up tail-only
            var (tailL, tailR) = Tail.SplitAt(idx - TailOffset);

            var newLeft = new RRBPersistentVector<T>(idx, Shift, Root, tailL, TailOffset);

            var emptyRoot = RRBNode<T>.MkFullNode(OwnerTokens.NullOwner, Array.Empty<RRBNode<T>>());
            var newRight = new RRBPersistentVector<T>(tailR.Length, Literals.shiftSize, emptyRoot, tailR, 0);
            // TODO: Use emptyNode here, it's safe to share because these are persistent. Reduces GC pressure.

            return (newLeft, newRight);
        }
        else
        {
            // Splitting inside the tree, need to ask the root node to split itself
            var (rootL, rootR) = Root.SplitTree(OwnerTokens.NullOwner, Shift, idx);

            // Right tree will retain current tail, left tree must promote a new tail
            var (newTailNodeL, newRootL) = ((RRBFullNode<T>)rootL).PopLastLeaf(OwnerTokens.NullOwner, Shift);

            // Have to adjust the tree for both newLeft AND newRight in this one, since either one could have become a tall, thin tree during the split
            var newLeft = new RRBPersistentVector<T>(idx, Shift, newRootL, newTailNodeL.Items, idx - newTailNodeL.NodeSize).AdjustTree();

            var newRight = new RRBPersistentVector<T>(Count - idx, Shift, rootR, Tail, Count - idx - Tail.Length).AdjustTree();

            return (newLeft, newRight);
        }
    }

    public override RRBVector<T> Slice(int start, int end)
    {
        int len = end - start + 1;
        return this.Skip(start).Take(len);
    }

    public override RRBVector<T> GetSlice(int? start, int? end)
    {
        RRBVector<T> result = this;
        if (start is int s) result = result.Skip(s);
        if (end is int e)
        {
            int len = e - (start ?? 0) + 1;
            result = result.Take(len);
        }
        return result;
    }

    public override RRBVector<T> Append(RRBVector<T> other)
    {
        return other switch
        {
            RRBPersistentVector<T> right => AppendPersistent(right),
            RRBTransientVector<T> right => Append(right.Persistent()),
            _ => Append((RRBPersistentVector<T>)other) // Will throw an invalid cast exception if we ever create a new subclass
            // TODO: Get rid of the catch-all case and just call .Persistent(), it's a no-op if we're already persistent
        };
    }

    private RRBPersistentVector<T> AppendPersistent(RRBPersistentVector<T> right)
    {
        if (this.Count == 0) return right;
        if (right.Count == 0) return this;

        int newLen = this.Count + right.Count;

        if (right.TailOffset <= 0)
        {
            // Right is a tail-only vector
            int tailLenL = this.Count - this.TailOffset;
            int tailLenR = right.Count - right.TailOffset;

            if (tailLenL + tailLenR <= Literals.blockSize)
            {
                // Easy case, tail grows without affecting root
                var newTail = this.Tail.Append(right.Tail);
                return new RRBPersistentVector<T>(newLen, Shift, Root, newTail, TailOffset);
            }
            else
            {
                // Combined tails will become a new leaf node and a new tail
                var (newLeafItems, newTail) = RRBArrayExtensions.RRBArrayExtensions.AppendAndSplitAt(Literals.blockSize, this.Tail, right.Tail);

                var newLeaf = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(OwnerTokens.NullOwner, newLeafItems);

                if (this.TailOffset <= 0)
                {
                    // Can't use AppendLeaf in an empty root, so we create the first twig by hand
                    // TODO: Test whether we can actually do that, because that would simplify this bit of the code
                    var newRoot = RRBNode<T>.MkFullNode(OwnerTokens.NullOwner, [newLeaf]);
                    return new RRBPersistentVector<T>(newLen, Literals.shiftSize, newRoot, newTail, Literals.blockSize);
                }
                else
                {
                    var (newRoot, newShift) = ((RRBFullNode<T>)this.Root).AppendLeaf(OwnerTokens.NullOwner, this.Shift, newLeaf);
                    return new RRBPersistentVector<T>(newLen, newShift, newRoot, newTail, this.TailOffset + Literals.blockSize);
                }
            }
        }
        else if (this.TailOffset <= 0)
        {
            // Right has a root and a tail, but we're a tail-only node. Our tail will be promoted to a new leftmost leaf in the right tree
            var newFirstLeaf = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(OwnerTokens.NullOwner, this.Tail); // TODO: Is this cast needed? Should we make MkLeaf return RRBLeafNode<T>?

            var (newRoot, newShift) = ((RRBFullNode<T>)right.Root).PrependLeaf(OwnerTokens.NullOwner, right.Shift, newFirstLeaf);
            return new RRBPersistentVector<T>(newLen, newShift, newRoot, right.Tail, right.TailOffset + newFirstLeaf.NodeSize);
        }
        else
        {
            // Right has a root and a tail, and so do we
            var tailNode = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(OwnerTokens.NullOwner, this.Tail);

            // Can the tail be merged into the two twig nodes that will surround it?
            // Now's the time to find out, while we can still push it down to form a new root of the left node
            var tailCanFit = ((RRBFullNode<T>)this.Root)
                .RightmostTwig(this.Shift)
                .HasRoomToMergeTheTail(
                    Literals.shiftSize,
                    tailNode,
                    ((RRBFullNode<T>)right.Root).LeftmostTwig(right.Shift)
                );

            int mergedShift;
            RRBNode<T> mergedRoot;
            RRBNode<T>? maybeNewRight;

            if (tailCanFit)
            {
                // No need to push tail down before merging, so skip that extra step
                mergedShift = Math.Max(this.Shift, right.Shift);
                (mergedRoot, maybeNewRight) = ((RRBFullNode<T>)this.Root).MergeTree(OwnerTokens.NullOwner, this.Shift, tailNode, right.Shift, (RRBFullNode<T>)right.Root, false);
            }
            else
            {
                var (newLeftRoot, newLeftShift) = ((RRBFullNode<T>)this.Root).AppendLeaf(OwnerTokens.NullOwner, this.Shift, tailNode);
                mergedShift = Math.Max(newLeftShift, right.Shift);
                (mergedRoot, maybeNewRight) = ((RRBFullNode<T>)this.Root).MergeTree(OwnerTokens.NullOwner, newLeftShift, null, right.Shift, (RRBFullNode<T>)right.Root, false);
            }

            if (maybeNewRight is RRBFullNode<T> rightRoot)
            {
                // No room at current level, have to grow the tree with new two-child root node
                var newRoot = ((RRBFullNode<T>)mergedRoot).NewParent(OwnerTokens.NullOwner, mergedShift, new[] { mergedRoot, rightRoot });

                return (RRBPersistentVector<T>)new RRBPersistentVector<T>(newLen, RRBMath.Up(mergedShift), newRoot, right.Tail, this.Count + right.TailOffset).AdjustTree();
            }
            else
            {
                // Everything fit into the merged root, no need to grow the tree upwards
                return (RRBPersistentVector<T>)new RRBPersistentVector<T>(newLen, mergedShift, mergedRoot, right.Tail, this.Count + right.TailOffset).AdjustTree();
            }
        }
    }

    // TODO: Remove, Update all need porting as well
    // And check the uses for RemoveWithoutRebalance, because we'll definitely want to use that when needed
    // Also GetItem and so on

    public override RRBVector<T> Insert(int idx, T newItem)
    {
        EnsureValidIndexLengthAllowed(idx);

        if (idx >= TailOffset)
        {
            // Inserting into the tail
            if (Count - TailOffset < Literals.blockSize)
            {
                // Inserting into tail, which has enough room to grow
                var newTail = Tail.CopyAndInsertAt(idx - TailOffset, newItem);

                return new RRBPersistentVector<T>(Count + 1, Shift, Root, newTail, TailOffset);
            }
            else
            {
                // Tail is full. Insert into middle of tail, split new item off to make new tail, resulting full array becomes leaf
                var (newLeafItems, newTailItem) = Tail.CopyAndInsertIntoFullArray(idx - TailOffset, newItem);

                var newTail = new[] { newTailItem };
                var newLeafNode = (RRBLeafNode<T>)RRBNode<T>.MkLeaf(OwnerTokens.NullOwner, newLeafItems);

                var (newRoot, newShift) = ((RRBFullNode<T>)Root).AppendLeaf(OwnerTokens.NullOwner, Shift, newLeafNode);

                // Pushing a full tail down into a leaf can't break the invariant, so no need to adjust the tree here
                return new RRBPersistentVector<T>(Count + 1, newShift, newRoot, newTail, Count);
            }
        }
        else
        {
            // Inserting into the root, tail will remain unchanged
            var insertionResult = ((RRBFullNode<T>)Root).InsertedTree(OwnerTokens.NullOwner, Shift, idx, newItem, null, 0);
            if (insertionResult is SimpleInsertion<RRBNode<T>> simple)
            {
                var newRoot = (RRBFullNode<T>)simple.NewCurrent;
                // Tree did not grow... but invariant might be violated now depending on where the insertion happened, so we must adjust tree
                return new RRBPersistentVector<T>(Count + 1, Shift, newRoot, Tail, TailOffset + 1).AdjustTree();
                // TODO: Remove this AdjustTree call and make a test fail, then create a targeted regression test for that scenario
            }
            if (insertionResult is SplitNode<RRBNode<T>> pair)
            {
                var newRoot = ((RRBFullNode<T>)pair.NewCurrent).NewParent(OwnerTokens.NullOwner, Shift, new[] { pair.NewCurrent, pair.NewRight });
                var newShift = RRBMath.Up(Shift);
                return new RRBPersistentVector<T>(Count + 1, newShift, newRoot, Tail, TailOffset + 1).AdjustTree();
                // TODO: Remove this AdjustTree call and make a test fail, then create a targeted regression test for that scenario
            }
#if DEBUG
            if (insertionResult is SlidItemsLeft<T>)
            {
                throw new InvalidOperationException("Impossible case: SlidItemsLeft in Insert() of persistent vector");
            }
            if (insertionResult is SlidItemsRight<T>)
            {
                throw new InvalidOperationException("Impossible case: SlidItemsRight in Insert() of persistent vector");
            }
            if (insertionResult is null)
            {
                throw new InvalidOperationException("Impossible case: null insertion result in Insert() of persistent vector");
            }
            else
            {
                throw new InvalidOperationException("Impossible case: unknown insertion result in Insert() of persistent vector");
            }
#endif
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
        EnsureValidIndex(idx);

        if (idx >= TailOffset)
        {
            // Removing from tail, tree remains unchanged
            if (Count == 1) return Empty(); // Fast exit for simplest case

            if (Count - TailOffset > 1)
            {
                // Tail will still have at least one item, so root will remain untouched
                var newTail = Tail.CopyAndRemoveAt(idx - TailOffset);

                return new RRBPersistentVector<T>(Count - 1, Shift, Root, newTail, TailOffset);
            }
            else
            {
                // Tail becomes empty: promote last leaf into new tail
                var (lastLeaf, newRoot) = ((RRBFullNode<T>)Root).PopLastLeaf(OwnerTokens.NullOwner, Shift);

                // Tree might need adjusting now, might have shrunk
                return new RRBPersistentVector<T>(Count - 1, Shift, newRoot, lastLeaf.Items, TailOffset - lastLeaf.NodeSize).AdjustTree();
            }
        }
        else
        {
            // Removing from tree, tail remains unchanged
            var newRoot = Root.RemovedItem(OwnerTokens.NullOwner, Shift, shouldCheckForRebalancing, idx);

            // Tree might need adjusting now, might have shrunk
            return new RRBPersistentVector<T>(Count - 1, Shift, newRoot, Tail, TailOffset - 1).AdjustTree();
        }
        // TODO: For each AdjustTree call there, remove it and see what tests fail, then create new regression test for that scenario
    }

    public override RRBVector<T> Update(int idx, T newItem)
    {
        EnsureValidIndex(idx);

        if (idx >= TailOffset)
        {
            // Root remains unchanged
            var newTail = Tail.CopyAndSet(idx - TailOffset, newItem);
            return new RRBPersistentVector<T>(Count, Shift, Root, newTail, TailOffset);
        }
        else
        {
            // Tail remains unchanged
            var newRoot = Root.UpdatedTree(OwnerTokens.NullOwner, Shift, idx, newItem);
            return new RRBPersistentVector<T>(Count, Shift, newRoot, Tail, TailOffset);
        }
    }

    public override T GetItem(int idx)
    {
        EnsureValidIndex(idx);

        if (idx >= TailOffset) return Tail[idx - TailOffset];
        else return Root.GetTreeItem(Shift, idx);
    }

    internal void EnsureValidIndex(int idx)
    {
        if (idx < 0 || idx >= Count) throw new IndexOutOfRangeException();
    }

    internal void EnsureValidIndexLengthAllowed(int idx)
    {
        if (idx < 0 || idx > Count) throw new IndexOutOfRangeException();
    }
}
