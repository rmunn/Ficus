module Ficus.RRBArrayExtensions

// A set of functions for manipulating arrays in a "functional" way, returning a
// new array as the result instead of mutating the existing array. Some of these
// functions also do multiple operations in a single step, without creating
// intermediate arrays, so as to be more efficient and reduce GC pressure.

// TODO: Once https://github.com/fsharp/fslang-suggestions/issues/102 gets into
// FSharp.Core, many of these functions will become obsolete. At that point, we
// should replace them with the "official" versions.

module Array =

    let copyAndAppend newItem oldArr =
        let len = Array.length oldArr
        let newArr = Array.zeroCreate (len + 1)
        Array.blit oldArr 0 newArr 0 len
        newArr.[len] <- newItem
        newArr

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    let copyAndInsertAt idx newItem oldArr =
        let oldLen = Array.length oldArr
        let newArr = Array.zeroCreate (oldLen + 1)
        Array.blit oldArr 0 newArr 0 idx
        newArr.[idx] <- newItem
        Array.blit oldArr idx newArr (idx + 1) (oldLen - idx)
        newArr

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    let inline copyAndSet idx newItem oldArr =
        let newArr = Array.copy oldArr
        newArr.[idx] <- newItem
        newArr

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    let inline sliceAndSet idx newItem oldArr oldArrLen =
        // TODO: Remove if not used
        let newArr = oldArr |> Array.truncate oldArrLen
        newArr.[idx] <- newItem
        newArr

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    let copyAndRemoveAt idx oldArr =
        let newLen = Array.length oldArr - 1
        let newArr = Array.zeroCreate newLen
        Array.blit oldArr 0 newArr 0 idx
        Array.blit oldArr (idx + 1) newArr idx (newLen - idx)
        newArr

    // Special case of the above for removing the first item
    let inline copyAndRemoveFirst oldArr =
        Array.sub oldArr 1 (Array.length oldArr - 1)

    // Special case of the above for removing the last item
    let inline copyAndPop oldArr =
        Array.sub oldArr 0 (Array.length oldArr - 1)

    // Slice oldArr from 0 to sliceLen-1 inclusive, THEN also remove the item at idx, in one operation with no intermediate arrays
    let sliceAndRemoveAt idx oldArr sliceLen =
        // TODO: We could turn all copyAndRemoveAt instances into calls to sliceAndRemoveAt idx oldArr oldArr.Length ... or more likely, node.NodeSize
        // Consider whether that's worth it.
        let newLen = sliceLen - 1
        let newArr = Array.zeroCreate newLen
        Array.blit oldArr 0 newArr 0 idx
        Array.blit oldArr (idx + 1) newArr idx (newLen - idx)
        newArr

    // Remove first item and push new item onto the end of the array, in one operation with no intermediate arrays
    let inline popFirstAndPush item oldArr =
        let len = Array.length oldArr
        let newArr = Array.zeroCreate len
        Array.blit oldArr 1 newArr 0 (len - 1)
        newArr.[len - 1] <- item
        newArr

    // Special case of copyAndSet for setting the last item (makes for a slightly nicer calling syntax)
    let inline copyAndSetLast newItem oldArr =
        copyAndSet (Array.length oldArr - 1) newItem oldArr

    // Like Array.append left right |> Array.splitAt splitIdx, but without creating an intermediate array
    let appendAndSplitAt splitIdx left right =
        let lenL = Array.length left
        let lenR = Array.length right
        let totalLen = lenL + lenR
        if lenL = splitIdx then
            left, right // Efficiency: don't make copies if we don't have to
        elif lenL < splitIdx then
            let resultL = Array.zeroCreate splitIdx
            Array.blit left 0 resultL 0 lenL
            Array.blit right 0 resultL lenL (splitIdx - lenL)
            let resultR = Array.sub right (splitIdx - lenL) (totalLen - splitIdx)
            resultL, resultR
        else // lenL > splitIdx
            let resultL = Array.sub left 0 splitIdx
            let resultR = Array.zeroCreate (totalLen - splitIdx)
            Array.blit left splitIdx resultR 0 (lenL - splitIdx)
            Array.blit right 0 resultR (lenL - splitIdx) lenR
            resultL, resultR

    // Like Array.append, but for three arrays at once. Equivalent to Array.concat [|left;middle;right|] but more efficient.
    let append3 left middle right =
        let lenL = Array.length left
        let lenM = Array.length middle
        let lenR = Array.length right
        let newArr = Array.zeroCreate (lenL + lenM + lenR)
        Array.blit left   0 newArr 0    lenL
        Array.blit middle 0 newArr lenL lenM
        Array.blit right  0 newArr (lenL+lenM) lenR
        newArr

    // Like Array.append3, but the "middle" is an item rather than an array. Equivalent to Array.concat [|left;[|middle|];right|] but more efficient.
    let append3' left middleItem right =
        let lenL = Array.length left
        let lenR = Array.length right
        let newArr = Array.zeroCreate (lenL + 1 + lenR)
        Array.blit left   0 newArr 0    lenL
        newArr.[lenL] <- middleItem
        Array.blit right  0 newArr (lenL+1) lenR
        newArr

    // Like Array.append, but for four arrays at once. Equivalent to Array.concat [|a;b;c;d|] but more efficient.
    let append4 a b c d =
        let lenA = Array.length a
        let lenB = Array.length b
        let lenC = Array.length c
        let lenD = Array.length d
        let newArr = Array.zeroCreate (lenA + lenB + lenC + lenD)
        Array.blit a 0 newArr 0 lenA
        Array.blit b 0 newArr lenA lenB
        Array.blit c 0 newArr (lenA+lenB) lenC
        Array.blit d 0 newArr (lenA+lenB+lenC) lenD
        newArr

    // Append two arrays and insert an item at position `idx` in the resulting array, without creating an intermediate array
    let appendAndInsertAt idx item a1 a2 =
        let len1 = Array.length a1
        let len2 = Array.length a2
        let totalLen = len1 + len2 + 1
        let result = Array.zeroCreate totalLen
        if idx >= len1 then
            Array.blit a1 0 result 0 len1
            Array.blit a2 0 result len1 (idx - len1)
            result.[idx] <- item
            Array.blit a2 (idx - len1) result (idx + 1) (len2 - (idx - len1))
        else
            Array.blit a1 0 result 0 idx
            result.[idx] <- item
            Array.blit a1 idx result (idx + 1) (len1 - idx)
            Array.blit a2 0 result (len1 + 1) len2
        result

    // Insert an item into an array and then split the array into two arrays of equal size;
    // if the total # of items is odd, the left array in the result will be 1 larger than the right array.
    // Does not use an intermediate array in producing its results. Used in the insertion algorithm.
    let insertAndSplitEvenly idx item arr =
        let len = Array.length arr
        let lenResultL = (len >>> 1) + 1
        let lenResultR = len + 1 - lenResultL
        let resultL = Array.zeroCreate lenResultL
        let resultR = Array.zeroCreate lenResultR
        if idx < lenResultL then
            Array.blit arr 0 resultL 0 idx
            resultL.[idx] <- item
            Array.blit arr idx resultL (idx + 1) (lenResultL - idx - 1)
            Array.blit arr (lenResultL - 1) resultR 0 lenResultR
        else
            let idxInR = idx - lenResultL
            Array.blit arr 0 resultL 0 lenResultL
            Array.blit arr lenResultL resultR 0 idxInR
            resultR.[idxInR] <- item
            Array.blit arr idx resultR (idxInR + 1) (lenResultR - idxInR - 1)
        resultL, resultR

    // Append two arrays, insert an item into the resulting array merged array, and then split the
    // post-insertion array into two equal parts (with left array possibly being 1 larger than right).
    // Does not use any intermediate arrays. Used in the insertion algorithm.
    let appendAndInsertAndSplitEvenly idx item a1 a2 =
        let len1 = Array.length a1
        let len2 = Array.length a2
        let lenResultL = ((len1 + len2) >>> 1) + 1
        let lenResultR = (len1 + len2) + 1 - lenResultL
        let resultL = Array.zeroCreate lenResultL
        let resultR = Array.zeroCreate lenResultR
        if idx < len1 && len1 < lenResultL then
            Array.blit a1 0 resultL 0 idx
            resultL.[idx] <- item
            Array.blit a1 idx resultL (idx + 1) (len1 - idx)
            let remainingInL = lenResultL - len1 - 1
            Array.blit a2 0 resultL (len1 + 1) remainingInL
            Array.blit a2 remainingInL resultR 0 lenResultR
            resultL, resultR
        elif idx < len1 && idx < lenResultL then
            // len1 >= lenResultL
            Array.blit a1 0 resultL 0 idx
            resultL.[idx] <- item
            let remainingInL = lenResultL - idx - 1
            Array.blit a1 idx resultL (idx + 1) remainingInL
            Array.blit a1 (idx + remainingInL) resultR 0 (len1 - lenResultL + 1)
            Array.blit a2 0 resultR (len1 - lenResultL + 1) len2
            resultL, resultR
        elif idx < len1 then
            // len1 >= lenResultL && idx >= lenResultL
            let idxInR = idx - lenResultL
            Array.blit a1 0 resultL 0 lenResultL
            Array.blit a1 lenResultL resultR 0 idxInR
            resultR.[idxInR] <- item
            let remainingInL = lenResultL - idx - 1
            Array.blit a1 idx resultR (idxInR + 1) (len1 - idx)
            Array.blit a2 0 resultR (len1 - lenResultL + 1) len2
            resultL, resultR
        elif idx < lenResultL then
            // idx >= len1, therefore len1 < lenResultL
            Array.blit a1 0 resultL 0 len1
            Array.blit a2 0 resultL len1 (idx - len1)
            resultL.[idx] <- item
            let remainingInL = lenResultL - idx - 1
            Array.blit a2 (idx - len1) resultL (idx + 1) remainingInL
            Array.blit a2 (remainingInL + idx - len1) resultR 0 lenResultR
            resultL, resultR
        elif len1 < lenResultL then
            // idx >= lenResultL and idx >= len1
            let idxInR = idx - lenResultL
            Array.blit a1 0 resultL 0 len1
            Array.blit a2 0 resultL len1 (lenResultL - len1)
            Array.blit a2 (lenResultL - len1) resultR 0 idxInR
            resultR.[idxInR] <- item
            Array.blit a2 (lenResultL - len1 + idxInR) resultR (idxInR + 1) (lenResultR - idxInR - 1)
            resultL, resultR
        else
            // idx >= len1 and len1 >= lenResultL, therefore idx >= lenResultL
            let idxInR = idx - lenResultL
            Array.blit a1 0 resultL 0 lenResultL
            Array.blit a1 lenResultL resultR 0 (len1 - lenResultL)
            Array.blit a2 0 resultR (len1 - lenResultL) (idx - len1)
            resultR.[idxInR] <- item
            Array.blit a2 (idx - len1) resultR (idxInR + 1) (lenResultR - idxInR - 1)
            resultL, resultR
