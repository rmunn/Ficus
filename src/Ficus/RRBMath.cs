using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Ficus.RRBArrayExtensions;

namespace Ficus;

// Concepts:
//
// "Shift" - The height of any given node, multiplied by Literals.shiftSize.
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
// Twig - The tree level just above the leaf level; shift = Literals.shiftSize
// (Successively higher levels of the tree could called, in order after twig: branch, limb, trunk...
// But we don't actually use those terms in the code, just "twig" and "leaf".)

public static class RRBMath
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int RadixIndex(int shift, int treeIdx)
    {
        return (treeIdx >> shift) & Literals.blockIndexMask;
    }

    // Syntactic sugar for operations we'll use *all the time*: moving up and down the tree levels
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Down(int shift)
    {
        return shift - Literals.shiftSize;
    }

    // Syntactic sugar for operations we'll use *all the time*: moving up and down the tree levels
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Up(int shift)
    {
        return shift + Literals.shiftSize;
    }

    // This takes a `len` parameter that should be the size of the size table, so that it can handle expanded nodes
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsSizeTableFullAtShift(int shift, int[] sizeTbl, int len)
    {
        if (len <= 1) return true;
        int checkIdx = len - 2;
        return sizeTbl[checkIdx] == ((checkIdx + 1) << shift);
    }

    // TODO: These should probably just take ints instead of bytes, honestly
    public static (int Index, int Length) FindMergeCandidates(IEnumerable<int> sizeSeq, int len)
    {
        using var e = sizeSeq.GetEnumerator();

        var openSlots = new int[len];

        for (int i = 0; i < len; i++)
        {
            if (e.MoveNext())
                openSlots[i] = Literals.blockSize - e.Current;
            else
                openSlots[i] = 0;
        }

        return openSlots.SmallestRunOfAtLeast(Literals.blockSize);
    }

    // TODO: At some point, test whether this version is more efficient than the single-pass version above
    public static (int Index, int Length, int SlotsDropped)
        FindMergeCandidatesTwoPasses(IEnumerable<int> sizeSeq, int len)
    {
        using var e = sizeSeq.GetEnumerator();

        var openSlots = new int[len];

        for (int i = 0; i < len; i++)
        {
            if (e.MoveNext())
                openSlots[i] = Literals.blockSize - e.Current;
            else
                openSlots[i] = 0;
        }

        var (idx1, len1) = openSlots.SmallestRunOfAtLeast(Literals.blockSize);
        var (idx2, len2) = openSlots.SmallestRunOfAtLeast(Literals.blockSize * 2);

        // Heuristic: prefer collapsing 2 slots if it's not too expensive
        if (len2 < (len1 * 2))
            return (idx2, len2, 2);
        else
            return (idx1, len1, 1);
    }

    // Note that this will not always find a *theoretically optimum* solution. For example, consider these sizes where blockSize = 32:
    // [20; 32; 31; 32; 23; 32; 17; 32; 26; 32; 20; 32; 29; 24; 32; 18; 32; 27; 32; 21; 32; 30; 32; 24; 32; 16; 32; 17; 32; 19; 32; 20]
    // A solution optimized for length would find (15, 17, 3), basically collapsing the right half of the node and saving three slots.
    // Our solution finds (23, 9, 2), so it only saves two slots. OTOH, it does half the work, so in practice going from 2 to 3 is probably
    // not worth doubling the work, whereas going from 1 to 2 probably is worth doubling the work. (Though that does need to be benchmarked
    // and measured to see if it *really* works out in practice.)
}
