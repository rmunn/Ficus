module Ficus.RRBArrayExtensions

// A set of functions for manipulating arrays in a "functional" way, returning a
// new array as the result instead of mutating the existing array. Some of these
// functions also do multiple operations in a single step, without creating
// intermediate arrays, so as to be more efficient and reduce GC pressure.

// TODO: Once https://github.com/fsharp/fslang-suggestions/issues/102 gets into
// FSharp.Core, many of these functions will become obsolete. At that point, we
// should replace them with the "official" versions.

module Array =

    let copyAndAppend newItem oldArr =   // FIXME: Rename to copyAndPush
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
    let copyAndInsertIntoFullArray idx newItem oldArr =
        let oldLen = Array.length oldArr
        if idx = oldLen then
            oldArr, newItem
        else
            let newArrL = Array.zeroCreate oldLen
            Array.blit oldArr 0 newArrL 0 idx
            newArrL.[idx] <- newItem
            Array.blit oldArr idx newArrL (idx + 1) (oldLen - idx - 1)
            newArrL, oldArr.[oldLen - 1]

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    let copyAndSet idx newItem oldArr =
        let newArr = Array.copy oldArr
        newArr.[idx] <- newItem
        newArr

    let expandToBlockSize (arr : 'T[]) =
        let len = Array.length arr
        if len = Literals.blockSize then arr
        elif len > Literals.blockSize then arr |> Array.truncate Literals.blockSize
        else
            let arr' = Array.zeroCreate Literals.blockSize
            arr.CopyTo(arr', 0)
            arr'

    // NOTE: No bounds-checking on idx. It's caller's responsibility to set it properly.
    let copyAndRemoveAt idx oldArr =
        let newLen = Array.length oldArr - 1
        let newArr = Array.zeroCreate newLen
        Array.blit oldArr 0 newArr 0 idx
        Array.blit oldArr (idx + 1) newArr idx (newLen - idx)
        newArr

    // Special case of the above for removing the last item
    let copyAndPop oldArr =
        Array.sub oldArr 0 (Array.length oldArr - 1)

    // Remove first item and push new item onto the end of the array, in one operation with no intermediate arrays
    let popFirstAndPush item oldArr =
        let len = Array.length oldArr
        let newArr = Array.zeroCreate len
        Array.blit oldArr 1 newArr 0 (len - 1)
        newArr.[len - 1] <- item
        newArr

    let fillFromEnumerator (e : System.Collections.Generic.IEnumerator<'T>) (idx : int) (len : int) (arr : 'T []) =
        let mutable i = idx
        let mutable j = 0
        let lenArr = arr.Length
        while i < lenArr && j < len && e.MoveNext() do
            arr.[i] <- e.Current
            i <- i + 1
            j <- j + 1

    let fill2FromEnumerator (e : System.Collections.Generic.IEnumerator<'T>) (idx : int) (len : int) (arrL : 'T []) (arrR : 'T []) =
        let lenL = arrL.Length
        // TODO: If we want better error-checking, uncomment next three lines and use lenToFill instead of len in the "while j < len" part
        // let lenR = arrR.Length
        // let totalLen = lenL + lenR
        // let lenToFill = min len totalLen
        let mutable i = idx
        let mutable j = 0
        let mutable fillingLeft = true
        while j < len && e.MoveNext() do
            if fillingLeft && i >= lenL then
                fillingLeft <- false
                i <- i - lenL
            if fillingLeft then
                arrL.[i] <- e.Current
            else
                arrR.[i] <- e.Current
            i <- i + 1
            j <- j + 1

    let fillFromSeq (s : 'T seq) (idx : int) (len : int) (arr : 'T []) =
        arr |> fillFromEnumerator (s.GetEnumerator()) idx len

    let fill2FromSeq (s : 'T seq) idx len arrL arrR =
        fill2FromEnumerator (s.GetEnumerator()) idx len arrL arrR
(* Symmetry functions for fillFrom* that aren't really used (createFromEnumerator was used in one #if DEBUG block, but that block is gone now)
    let createFromEnumerator (e : System.Collections.Generic.IEnumerator<'T>) (len : int) =
        let arr = Array.zeroCreate len
        arr |> fillFromEnumerator e 0 len
        arr

    let create2FromEnumerator (e : System.Collections.Generic.IEnumerator<'T>) (lenL : int) (lenR : int) =
        let arrL = Array.zeroCreate lenL
        let arrR = Array.zeroCreate lenR
        fill2FromEnumerator e 0 (lenL+lenR) arrL arrR
        arrL, arrR

    let createFromSeq (s : 'T seq) (len : int) =
        let arr = Array.zeroCreate len
        arr |> fillFromSeq s 0 len
        arr

    let create2FromSeq (s : 'T seq) (lenL : int) (lenR : int) =
        let arrL = Array.zeroCreate lenL
        let arrR = Array.zeroCreate lenR
        fill2FromSeq s 0 (lenL+lenR) arrL arrR
        arrL, arrR
*)
    let createManyFromEnumerator (e : System.Collections.Generic.IEnumerator<'T>) (totalLen : int) (lenPerArray : int) =
        let arrayCount = totalLen / lenPerArray
        let remainder = totalLen % lenPerArray
        if arrayCount <= 0 then
            seq {
                let arr = Array.zeroCreate remainder
                fillFromEnumerator e 0 remainder arr
                yield arr
            }
        else
            seq {
                for i = 1 to arrayCount do
                    let arr = Array.zeroCreate lenPerArray
                    fillFromEnumerator e 0 lenPerArray arr
                    yield arr
                if remainder > 0 then
                    let arr = Array.zeroCreate remainder
                    fillFromEnumerator e 0 remainder arr
                    yield arr
            }

    let createManyFromSeq (s : 'T seq) (totalLen : int) (lenPerArray : int) =
        createManyFromEnumerator (s.GetEnumerator()) totalLen lenPerArray

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

    // Finds a run of numbers whose sum is >= N in a single pass through the array; usually finds a run of minimal length, though
    // in some cases it can find a run that's just a little bit longer than the minimal possible run.
    // Basic algorithm found at https://stackoverflow.com/questions/13023188/smallest-subset-of-array-whose-sum-is-no-less-than-key
    let smallestRunOfAtLeast (n : byte) (arr : byte[]) =
        let mutable acc = 0uy
        let mutable p = 0
        let mutable q = 0
        let arrLen = Array.length arr
        let mutable bestIdx = 0
        let mutable bestLen = arrLen
        while q < arrLen do
            // Expand candidate run until its total is at least N
            while acc < n && q < arrLen do
                acc <- acc + arr.[q]
                q <- q + 1
            while acc - arr.[p] >= n && p < arrLen do
                acc <- acc - arr.[p]
                p <- p + 1
            if acc >= n then
                let candidateLen = q - p
                if candidateLen < bestLen then
                    bestLen <- candidateLen
                    bestIdx <- p
            acc <- acc - arr.[p]
            p <- p + 1
        bestIdx, bestLen
