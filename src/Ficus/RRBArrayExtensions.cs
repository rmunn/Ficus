using System;
using System.Collections.Generic;

public static class RRBArrayExtensions
{
    public static T[] CopyAndAppend<T>(this T[] oldArr, T newItem)
    {
        int len = oldArr.Length;
        var newArr = new T[len + 1];
        Array.Copy(oldArr, 0, newArr, 0, len);
        newArr[len] = newItem;
        return newArr;
    }

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    public static T[] CopyAndInsertAt<T>(this T[] oldArr, int idx, T newItem)
    {
        int oldLen = oldArr.Length;
        var newArr = new T[oldLen + 1];

        Array.Copy(oldArr, 0, newArr, 0, idx);
        newArr[idx] = newItem;
        Array.Copy(oldArr, idx, newArr, idx + 1, oldLen - idx);

        return newArr;
    }

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    public static (T[] Array, T Overflow) CopyAndInsertIntoFullArray<T>(this T[] oldArr, int idx, T newItem)
    {
        int oldLen = oldArr.Length;

        if (idx == oldLen)
            return (oldArr, newItem);

        var newArr = new T[oldLen];

        Array.Copy(oldArr, 0, newArr, 0, idx);
        newArr[idx] = newItem;
        Array.Copy(oldArr, idx, newArr, idx + 1, oldLen - idx - 1);

        return (newArr, oldArr[oldLen - 1]);
    }

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    public static T[] CopyAndSet<T>(this T[] oldArr, int idx, T newItem)
    {
        var newArr = (T[])oldArr.Clone();
        newArr[idx] = newItem;
        return newArr;
    }

    public static T[] ExpandToBlockSize<T>(this T[] arr, int blockSize)
    {
        int len = arr.Length;

        if (len == blockSize)
            return arr;

        if (len > blockSize)
        {
            var result = new T[blockSize];
            Array.Copy(arr, 0, result, 0, blockSize);
            return result;
        }
        else
        {
            var result = new T[blockSize];
            Array.Copy(arr, 0, result, 0);
            return result;
        }
    }

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    public static T[] CopyAndRemoveAt<T>(this T[] oldArr, int idx)
    {
        int newLen = oldArr.Length - 1;
        var newArr = new T[newLen];

        Array.Copy(oldArr, 0, newArr, 0, idx);
        Array.Copy(oldArr, idx + 1, newArr, idx, newLen - idx);

        return newArr;
    }

    // Special case of the above for removing the last item
    public static T[] CopyAndPop<T>(this T[] oldArr)
    {
        int len = oldArr.Length - 1;
        var result = new T[len];
        Array.Copy(oldArr, 0, result, 0, len);
        return result;
    }

    // Remove first item and push new item onto the end of the array, in one operation with no intermediate arrays
    public static T[] PopFirstAndPush<T>(this T[] oldArr, T item)
    {
        int len = oldArr.Length;
        var newArr = new T[len];

        Array.Copy(oldArr, 1, newArr, 0, len - 1);
        newArr[len - 1] = item;

        return newArr;
    }

    public static void FillFromEnumerator<T>(T[] arr, IEnumerator<T> e, int idx, int len)
    {
        int i = idx;
        int j = 0;
        int lenArr = arr.Length;

        while (i < lenArr && j < len && e.MoveNext())
        {
            arr[i++] = e.Current;
            j++;
        }
    }

    public static void Fill2FromEnumerator<T>(T[] arrL, T[] arrR, IEnumerator<T> e, int idx, int len)
    {
        int lenL = arrL.Length;
        // TODO: If we want better error-checking, uncomment next three lines and use lenToFill instead of len in the "while j < len" part
        // int lenR = arrR.Length
        // int totalLen = lenL + lenR
        // int lenToFill = Math.min(len, totalLen)
        int i = idx;
        int j = 0;
        bool fillingLeft = true;

        while (j < len && e.MoveNext())
        {
            if (fillingLeft && i >= lenL)
            {
                fillingLeft = false;
                i -= lenL;
            }

            if (fillingLeft)
                arrL[i] = e.Current;
            else
                arrR[i] = e.Current;

            i++;
            j++;
        }
    }

    public static void FillFromSeq<T>(T[] arr, IEnumerable<T> s, int idx, int len)
    {
        using var e = s.GetEnumerator();
        FillFromEnumerator(arr, e, idx, len);
    }

    public static void Fill2FromSeq<T>(T[] arrL, T[] arrR, IEnumerable<T> s, int idx, int len)
    {
        using var e = s.GetEnumerator();
        Fill2FromEnumerator(arrL, arrR, e, idx, len);
    }

    public static IEnumerable<T[]> CreateManyFromEnumerator<T>(IEnumerator<T> e, int totalLen, int lenPerArray)
    {
        int arrayCount = totalLen / lenPerArray;
        int remainder = totalLen % lenPerArray;

        if (arrayCount <= 0)
        {
            var arr = new T[remainder];
            FillFromEnumerator(arr, e, 0, remainder);
            yield return arr;
        }
        else
        {
            for (int i = 0; i < arrayCount; i++)
            {
                var arr = new T[lenPerArray];
                FillFromEnumerator(arr, e, 0, lenPerArray);
                yield return arr;
            }

            if (remainder > 0)
            {
                var arr = new T[remainder];
                FillFromEnumerator(arr, e, 0, remainder);
                yield return arr;
            }
        }
    }

    public static IEnumerable<T[]> CreateManyFromSeq<T>(IEnumerable<T> s, int totalLen, int lenPerArray)
    {
        using var e = s.GetEnumerator();
        foreach (var arr in CreateManyFromEnumerator(e, totalLen, lenPerArray))
            yield return arr;
    }

    // Appends two arrays, splitting at given index if total length too large, without creating intermediate arrays
    public static (T[] Left, T[] Right) AppendAndSplitAt<T>(int splitIdx, T[] left, T[] right)
    {
        // TODO: Consider using Span<T>.Slice(...).CopyTo(...) to avoid the constant bounds-checking in Array.Copy
        int lenL = left.Length;
        int lenR = right.Length;
        int totalLen = lenL + lenR;

        if (lenL == splitIdx)
            return (left, right);

        if (lenL < splitIdx)
        {
            var resultL = new T[splitIdx];
            Array.Copy(left, 0, resultL, 0, lenL);
            Array.Copy(right, 0, resultL, lenL, splitIdx - lenL);

            var resultR = new T[totalLen - splitIdx];
            Array.Copy(right, splitIdx - lenL, resultR, 0, totalLen - splitIdx);

            return (resultL, resultR);
        }
        else
        {
            var resultL = new T[splitIdx];
            Array.Copy(left, 0, resultL, 0, splitIdx);

            var resultR = new T[totalLen - splitIdx];
            Array.Copy(left, splitIdx, resultR, 0, lenL - splitIdx);
            Array.Copy(right, 0, resultR, lenL - splitIdx, lenR);

            return (resultL, resultR);
        }
    }

    // Insert an item into an array and then split the array into two arrays of equal size;
    // if the total # of items is odd, the left array in the result will be 1 larger than the right array.
    // Does not use an intermediate array in producing its results. Used in the insertion algorithm.
    public static (T[] Left, T[] Right) InsertAndSplitEvenly<T>(T[] arr, int idx, T item)
    {
        // TODO: Consider using Span<T>.Slice(...).CopyTo(...) to avoid the constant bounds-checking in Array.Copy
        int len = arr.Length;

        int lenResultL = (len >> 1) + 1;
        int lenResultR = len + 1 - lenResultL;

        var resultL = new T[lenResultL];
        var resultR = new T[lenResultR];

        if (idx < lenResultL)
        {
            Array.Copy(arr, 0, resultL, 0, idx);
            resultL[idx] = item;

            Array.Copy(arr, idx, resultL, idx + 1, lenResultL - idx - 1);
            Array.Copy(arr, lenResultL - 1, resultR, 0, lenResultR);
        }
        else
        {
            int idxInR = idx - lenResultL;

            Array.Copy(arr, 0, resultL, 0, lenResultL);
            Array.Copy(arr, lenResultL, resultR, 0, idxInR);

            resultR[idxInR] = item;

            Array.Copy(arr, idx, resultR, idxInR + 1, lenResultR - idxInR - 1);
        }

        return (resultL, resultR);
    }

    // Append two arrays, insert an item into the resulting array merged array, and then split the
    // post-insertion array into two equal parts (with left array possibly being 1 larger than right).
    // Does not use any intermediate arrays. Used in the insertion algorithm.
    public static (T[] Left, T[] Right) AppendAndInsertAndSplitEvenly<T>(
        T[] a1, T[] a2, int idx, T item)
    {
        // TODO: Consider using Span<T>.Slice(...).CopyTo(...) to avoid the constant bounds-checking in Array.Copy
        int len1 = a1.Length;
        int len2 = a2.Length;

        int lenResultL = ((len1 + len2) >> 1) + 1;
        int lenResultR = (len1 + len2) + 1 - lenResultL;

        var resultL = new T[lenResultL];
        var resultR = new T[lenResultR];

        if (idx < len1 && len1 < lenResultL)
        {
            Array.Copy(a1, 0, resultL, 0, idx);
            resultL[idx] = item;
            Array.Copy(a1, idx, resultL, idx + 1, len1 - idx);

            int remainingInL = lenResultL - len1 - 1;

            Array.Copy(a2, 0, resultL, len1 + 1, remainingInL);
            Array.Copy(a2, remainingInL, resultR, 0, lenResultR);
        }
        else if (idx < len1 && idx < lenResultL)
        {
            // len1 >= lenResultL
            Array.Copy(a1, 0, resultL, 0, idx);
            resultL[idx] = item;

            int remainingInL = lenResultL - idx - 1;

            Array.Copy(a1, idx, resultL, idx + 1, remainingInL);

            Array.Copy(
                a1,
                idx + remainingInL,
                resultR,
                0,
                len1 - lenResultL + 1);

            Array.Copy(
                a2,
                0,
                resultR,
                len1 - lenResultL + 1,
                len2);
        }
        else if (idx < len1)
        {
            // len1 >= lenResultL && idx >= lenResultL
            int idxInR = idx - lenResultL;

            Array.Copy(a1, 0, resultL, 0, lenResultL);
            Array.Copy(a1, lenResultL, resultR, 0, idxInR);

            resultR[idxInR] = item;

            Array.Copy(a1, idx, resultR, idxInR + 1, len1 - idx);

            Array.Copy(
                a2,
                0,
                resultR,
                len1 - lenResultL + 1,
                len2);
        }
        else if (idx < lenResultL)
        {
            // idx >= len1, therefore len1 < lenResultL
            Array.Copy(a1, 0, resultL, 0, len1);
            Array.Copy(a2, 0, resultL, len1, idx - len1);

            resultL[idx] = item;

            int remainingInL = lenResultL - idx - 1;

            Array.Copy(a2, idx - len1, resultL, idx + 1, remainingInL);

            Array.Copy(
                a2,
                remainingInL + idx - len1,
                resultR,
                0,
                lenResultR);
        }
        else if (len1 < lenResultL)
        {
            // idx >= lenResultL and idx >= len1
            int idxInR = idx - lenResultL;

            Array.Copy(a1, 0, resultL, 0, len1);

            Array.Copy(
                a2,
                0,
                resultL,
                len1,
                lenResultL - len1);

            Array.Copy(
                a2,
                lenResultL - len1,
                resultR,
                0,
                idxInR);

            resultR[idxInR] = item;

            Array.Copy(
                a2,
                lenResultL - len1 + idxInR,
                resultR,
                idxInR + 1,
                lenResultR - idxInR - 1);
        }
        else
        {
            // idx >= len1 and len1 >= lenResultL
            int idxInR = idx - lenResultL;

            Array.Copy(a1, 0, resultL, 0, lenResultL);

            Array.Copy(
                a1,
                lenResultL,
                resultR,
                0,
                len1 - lenResultL);

            Array.Copy(
                a2,
                0,
                resultR,
                len1 - lenResultL,
                idx - len1);

            resultR[idxInR] = item;

            Array.Copy(
                a2,
                idx - len1,
                resultR,
                idxInR + 1,
                lenResultR - idxInR - 1);
        }

        return (resultL, resultR);
    }

    // Finds a run of numbers whose sum is >= N in a single pass through the array; usually finds a run of minimal length, though
    // in some cases it can find a run that's just a little bit longer than the minimal possible run.
    // Basic algorithm found at https://stackoverflow.com/questions/13023188/smallest-subset-of-array-whose-sum-is-no-less-than-key
    public static (int Index, int Length) SmallestRunOfAtLeast(this byte[] arr, byte n)
    {
        byte acc = 0;
        int p = 0, q = 0;
        int arrLen = arr.Length;
        int bestIdx = 0;
        int bestLen = arrLen;

        while (q < arrLen)
        {
            while (acc < n && q < arrLen)
                acc += arr[q++];

            while (p < arrLen && acc - arr[p] >= n)
                acc -= arr[p++];

            if (acc >= n)
            {
                int candidateLen = q - p;
                if (candidateLen < bestLen)
                {
                    bestLen = candidateLen;
                    bestIdx = p;
                }
            }

            acc -= arr[p++];
        }

        return (bestIdx, bestLen);
    }
}
