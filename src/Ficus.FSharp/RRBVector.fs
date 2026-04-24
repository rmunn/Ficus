/// Relaxed Radix Balanced Vector
///
/// Original concept: Phil Bagwell and Tiark Rompf
/// https://infoscience.epfl.ch/record/169879/files/RMTrees.pdf
///
/// Partly based on work by Jean Niklas L'orange: http://hypirion.com/thesis

module Ficus.FSharp

open Ficus

let internal isTransient (vec: RRBVector<'T>) = vec :? RRBTransientVector<'T>
let internal emptyNode<'T> = Ficus.RRBFullNode<'T>.EmptyNode
let internal nullOwner = Ficus.OwnerTokens.NullOwner
let inline internal isSameObj a b = LanguagePrimitives.PhysicalEquality a b

// Extensions to C# API

// GetSlice needs F# options, not Nullable<int>, as parameters, so C# code won't want to consume it

type Ficus.RRBVector<'T> with
    member this.GetSlice(start, stop) =
        match start, stop with
        | None, None -> this
        | None, Some stop -> this.Take(stop + 1) // vec.[..5] should return all indices from 0 to 5, i.e. 6 in total
        | Some start, None -> this.Skip start
        | Some start, Some stop -> this.Slice(start, stop)

module RRBVector =
    let inline nth (idx: int) (vec: RRBVector<'T>) = vec.[idx]
    let inline peek (vec: RRBVector<'T>) = vec.Peek()
    let inline pop (vec: RRBVector<'T>) = vec.Pop()
    let inline push (item: 'T) (vec: RRBVector<'T>) = vec.Push item
    let inline append (vec1: RRBVector<'T>) (vec2: RRBVector<'T>) = vec1.Append(vec2)
    let inline split idx (vec: RRBVector<'T>) = vec.Split idx
    let inline remove idx (vec: RRBVector<'T>) = vec.Remove idx
    let inline insert idx (item: 'T) (vec: RRBVector<'T>) = vec.Insert(idx, item)

    let empty<'T> = RRBPersistentVector<'T>.MkEmpty() :> RRBVector<'T>

    let internal flip f a b = f b a
    let internal flip3 f a b c = f b c a

    let toArray (vec: RRBVector<'T>) =
        let result: 'T[] = Array.zeroCreate vec.Length
        let mutable i = 0

        for leaf in vec.IterLeaves() do
            leaf.CopyTo(result, i)

            i <-
                i
                + leaf.Length

        result

    let inline toSeq (vec: RRBVector<'T>) =
        vec :> System.Collections.Generic.IEnumerable<'T>

    let inline toList (vec: RRBVector<'T>) =
        vec
        |> List.ofSeq

    let ofSeq (s: seq<'T>) =
        let mutable transient = RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        for item in s do
            transient <- transient.Push item

        transient.Persistent()

    let ofArray (a: 'T[]) =
        if
            a.Length
            <= Literals.blockSize
        then
            let tail = Array.copy a

            RRBPersistentVector<'T>(a.Length, Literals.shiftSize, emptyNode, tail, 0)
            :> RRBVector<'T>
        elif
            a.Length
            <= Literals.blockSize
               * 2
        then
            let leaf, tail =
                a
                |> Array.splitAt Literals.blockSize

            RRBPersistentVector<'T>(
                a.Length,
                Literals.shiftSize,
                RRBNode<'T>.MkFullNode(nullOwner, [| RRBNode<'T>.MkLeaf(nullOwner, leaf) |]),
                tail,
                Literals.blockSize
            )
            :> RRBVector<'T>
        // TODO: Perhaps this should be a static member of RRBVector, called FromArray? Might be nice to have this in the C# API
        else
            a |> ofSeq

    let ofList (l: 'T list) =
        let mutable transient = RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        for item in l do
            transient <- transient.Push item

        transient.Persistent()

    let rev (vec: RRBVector<'T>) =
        if vec.IsEmpty() then
            vec
        else if
            vec
            |> isTransient
        then
            // Reverse by swapping in-place. TODO: Benchmark and see if this is faster than creating a new copy
            let lastIdx =
                vec.Length
                - 1

            let mid =
                vec.Length
                >>> 1

            for i = 0 to mid - 1 do
                let j =
                    lastIdx
                    - i

                let oldL = vec.[i]
                let oldR = vec.[j]

                vec.Update(i, oldR)
                |> ignore

                vec.Update(j, oldL)
                |> ignore

            vec
        else
            let mutable transient = RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

            for item in vec.RevIterItems() do
                transient <- transient.Push item

            transient.Persistent()

    // TODO: Try improving average and averageBy by using iterLeafArrays(), summing up each array, and then dividing by count at the end. MIGHT be faster than Seq.average.
    let inline average (vec: RRBVector<'T>) =
        vec
        |> Seq.average

    let inline averageBy f (vec: RRBVector<'T>) =
        vec
        |> Seq.averageBy f

    let choose (chooser: 'T -> 'U option) (vec: RRBVector<'T>) : RRBVector<'U> =
        // TODO: Might be able to consolidate most of this (as we did in RRBVector.except), as only the first and last lines really differ. Ditto for rest of "if vec |> isTransient" cases
        if
            vec
            |> isTransient
        then
            let mutable result =
                RRBTransientVector<'U>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<'U>

            for item in vec do
                match chooser item with
                | None -> ()
                | Some value -> result <- result.Push value

            result
        else
            let mutable transient = RRBTransientVector<'U>.MkEmpty() :> RRBVector<'U>

            for item in vec do
                match chooser item with
                | None -> ()
                | Some value -> transient <- transient.Push value

            transient.Persistent()
    // Alternate version (for persistent vectors only). TODO: Benchmark
    // let chooseAlt (chooser : 'T -> 'U option) (vec : RRBVector<'T>) : RRBVector<'U> =
    //     if vec |> isTransient then failwith "DEBUG: chooseAlt only implemented for persistent vectors"
    //     vec |> Seq.choose chooser |> ofSeq

    let chunkBySize chunkSize (vec: RRBVector<'T>) =
        if
            chunkSize
            <= 0
            && vec.Length > 0
        then
            failwith "Chunk size must be greater than zero"

        if
            vec
            |> isTransient
        then
            let mutable result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            let mutable remaining = vec

            while remaining.Length > 0 do
                let struct (chunk, rest) = remaining.Split(min chunkSize remaining.Length)
                // Now that it's a C# struct tuple, can't do it that way
                result <- result.Push chunk
                remaining <- rest

            result
        else
            let mutable transient = RRBTransientVector<_>.MkEmpty() :> RRBVector<_>
            let mutable remaining = vec

            while remaining.Length > 0 do
                let struct (chunk, rest) = remaining.Split(min chunkSize remaining.Length)
                transient <- transient.Push chunk
                remaining <- rest

            transient.Persistent()

    let concat (vecs: seq<RRBVector<'T>>) =
        if
            vecs
            |> Seq.isEmpty
        then
            empty<'T>
        else
            let mutable result = Seq.head vecs

            for vec in Seq.tail vecs do
                result <- result.Append vec

            result

    let inline collect (f: 'T -> RRBVector<'T>) (vec: RRBVector<'T>) =
        // TODO: Benchmark the following two options, because I have no idea which is slower.
        // Option 1, the merging version
        vec
        |> Seq.map f
        |> concat

    // Option 2, the one-at-a-time version
    // let mutable transient = RRBTransientVector.MkEmpty() :> RRBVector<_>
    // for src in vec do
    //     for item in f src do
    //         transient <- transient.Push item
    // transient.Persistent()

    let inline compareWith f (vec1: RRBVector<'T>) (vec2: RRBVector<'T>) =
        (vec1, vec2)
        ||> Seq.compareWith f

    let countBy f (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            for resultItem in
                vec
                |> Seq.countBy f do
                result.Push resultItem
                |> ignore

            result
        else
            vec
            |> Seq.countBy f
            |> ofSeq

    let inline contains item (vec: RRBVector<'T>) =
        vec
        |> Seq.contains item

    let distinct (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            for resultItem in
                vec
                |> Seq.distinct do
                result.Push resultItem
                |> ignore

            result
        else
            vec
            |> Seq.distinct
            |> ofSeq

    let distinctBy f (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            for resultItem in
                vec
                |> Seq.distinctBy f do
                result.Push resultItem
                |> ignore

            result
        else
            vec
            |> Seq.distinctBy f
            |> ofSeq

    let exactlyOne (vec: RRBVector<'T>) =
        if
            vec.Length
            <> 1
        then
            invalidArg "vec"
            <| sprintf
                "exactlyOne called on a vector of %d items (requires a vector of exactly 1 item)"
                vec.Length

        vec.Peek()

    let except (excludedVec: RRBVector<'T>) (vec: RRBVector<'T>) =
        let excludedSet = System.Collections.Generic.HashSet<'T>(excludedVec)

        let mutable transient =
            if
                vec
                |> isTransient
            then
                RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        for item in vec do
            if not (excludedSet.Contains item) then
                transient <- transient.Push item

        if
            vec
            |> isTransient
        then
            transient
        else
            transient.Persistent()

    let inline exists f (vec: RRBVector<'T>) =
        vec
        |> Seq.exists f

    let inline exists2 f (vec1: RRBVector<'T>) (vec2: RRBVector<'U>) =
        (vec1, vec2)
        ||> Seq.exists2 f

    let filter pred (vec: RRBVector<'T>) =
        let mutable transient =
            if
                vec
                |> isTransient
            then
                RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        for item in vec do
            if pred item then
                transient <- transient.Push item

        if
            vec
            |> isTransient
        then
            transient
        else
            transient.Persistent()

    let filteri pred (vec: RRBVector<'T>) =
        let mutable transient =
            if
                vec
                |> isTransient
            then
                RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        let mutable i = 0

        for item in vec do
            if pred i item then
                transient <- transient.Push item

            i <- i + 1

        if
            vec
            |> isTransient
        then
            transient
        else
            transient.Persistent()

    let filter2 pred (vec1: RRBVector<'T>) (vec2: RRBVector<'T>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T>).Owner
                (vec2 :?> RRBTransientVector<'T>).Owner

        let mutable transient =
            if resultShouldBeTransient then
                RRBTransientVector<'T * 'T>
                    .MkEmptyWithToken((vec1 :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T * 'T>.MkEmpty() :> RRBVector<_>

        for item1, item2 in Seq.zip vec1 vec2 do
            if pred item1 item2 then
                transient <- transient.Push(item1, item2)

        if resultShouldBeTransient then
            transient
        else
            transient.Persistent()

    let filteri2 pred (vec1: RRBVector<'T>) (vec2: RRBVector<'T>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T>).Owner
                (vec2 :?> RRBTransientVector<'T>).Owner

        let mutable transient =
            if resultShouldBeTransient then
                RRBTransientVector<'T * 'T>
                    .MkEmptyWithToken((vec1 :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T * 'T>.MkEmpty() :> RRBVector<_>

        let mutable i = 0

        for item1, item2 in Seq.zip vec1 vec2 do
            if pred i item1 item2 then
                transient <- transient.Push(item1, item2)

            i <- i + 1

        if resultShouldBeTransient then
            transient
        else
            transient.Persistent()

    let inline find f (vec: RRBVector<'T>) =
        vec
        |> Seq.find f

    let inline findBack f (vec: RRBVector<'T>) =
        vec.RevIterItems()
        |> Seq.find f

    let inline findIndex f (vec: RRBVector<'T>) =
        vec
        |> Seq.findIndex f

    let inline findIndexBack f (vec: RRBVector<'T>) =
        let idx =
            vec.RevIterItems()
            |> Seq.findIndex f in

        vec.Length
        - 1
        - idx

    let inline fold folder (initState: 'State) (vec: RRBVector<'T>) =
        vec
        |> Seq.fold folder initState

    let inline fold2 folder (initState: 'State) (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        (vec1, vec2)
        ||> Seq.fold2 folder initState

    let inline foldBack (initState: 'State) folder (vec: RRBVector<'T>) =
        vec.RevIterItems()
        |> Seq.fold (fun a b -> folder b a) initState

    let inline foldBack2 (initState: 'State) folder (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        (vec1.RevIterItems(), vec2.RevIterItems())
        ||> Seq.fold2 (fun a b c -> folder b c a) initState

    let inline forall f (vec: RRBVector<'T>) =
        vec
        |> Seq.forall f

    let inline forall2 f (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        (vec1, vec2)
        ||> Seq.forall2 f

    let groupBy f (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            for resultItem in
                vec
                |> Seq.groupBy f do
                result.Push resultItem
                |> ignore

            result
        else
            vec
            |> Seq.groupBy f
            |> ofSeq

    let head (vec: RRBVector<'T>) =
        if vec.Length = 0 then
            invalidArg "vec" "Can't get head of empty vector"

        vec.[0]

    let indexed (vec: RRBVector<'T>) =
        let mutable transient =
            if
                vec
                |> isTransient
            then
                RRBTransientVector<int * 'T>
                    .MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<int * 'T>.MkEmpty() :> RRBVector<_>

        let mutable i = 0

        for item in vec do
            transient.Push(i, item)
            |> ignore

            i <- i + 1

        if
            vec
            |> isTransient
        then
            transient
        else
            transient.Persistent()

    let inline init size f =
        Seq.init size f
        |> ofSeq

    let inline isEmpty (vec: RRBVector<'T>) = vec.IsEmpty()
    let inline item (idx: int) (vec: RRBVector<'T>) = vec.[idx]

    let inline iter f (vec: RRBVector<'T>) =
        vec
        |> Seq.iter f

    let inline iter2 f (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        (vec1, vec2)
        ||> Seq.iter2 f

    let inline iteri f (vec: RRBVector<'T>) =
        vec
        |> Seq.iteri f

    let inline iteri2 f (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        (vec1, vec2)
        ||> Seq.iteri2 f

    let inline last (vec: RRBVector<'T>) =
        if vec.Length = 0 then
            invalidArg "vec" "Can't get last item of empty vector"

        vec.[vec.Length
             - 1]

    let inline length (vec: RRBVector<'T>) = vec.Length
    // TODO: Make map family of functions use IterEditableLeaves when vector is transient (see FIXME below)
    // TODO: Go through the functions and find any that make the transient be the same size, or just a little larger (e.g. scan), and use IterEditableLeaves there too (with one Insert for scan)


    let map f (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            for item in vec do
                result.Push(f item)
                |> ignore

            result
        else
            vec
            |> Seq.map f
            |> ofSeq
    // TODO: Benchmark the Seq.map version (the else block here) and see if it would be faster to use the same "unrolled" version we use for transients

    let map2 f (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T1>).Owner
                (vec2 :?> RRBTransientVector<'T2>).Owner

        if resultShouldBeTransient then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
                :> RRBVector<_>

            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()

            while iter1.MoveNext()
                  && iter2.MoveNext() do
                result.Push(f iter1.Current iter2.Current)
                |> ignore

            result
        else
            Seq.map2 f vec1 vec2
            |> ofSeq
    // TODO: Ditto re: benchmark for Seq.map

    let map3 f (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) (vec3: RRBVector<'T3>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && vec3
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T1>).Owner
                (vec2 :?> RRBTransientVector<'T2>).Owner
            && isSameObj
                (vec2 :?> RRBTransientVector<'T2>).Owner
                (vec3 :?> RRBTransientVector<'T3>).Owner

        if resultShouldBeTransient then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
                :> RRBVector<_>

            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            use iter3 = vec3.IterItems().GetEnumerator()

            while iter1.MoveNext()
                  && iter2.MoveNext()
                  && iter3.MoveNext() do
                result.Push(f iter1.Current iter2.Current iter3.Current)
                |> ignore

            result
        else
            Seq.map3 f vec1 vec2 vec3
            |> ofSeq
    // TODO: Ditto re: benchmark for Seq.map

    let mapi f (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            let mutable i = 0

            for item in vec do
                result.Push(f i item)
                |> ignore

                i <- i + 1

            result
        else
            vec
            |> Seq.mapi f
            |> ofSeq
    // TODO: Ditto re: benchmark for Seq.map

    let mapi2 f (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T1>).Owner
                (vec2 :?> RRBTransientVector<'T2>).Owner

        if resultShouldBeTransient then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
                :> RRBVector<_>

            let mutable i = 0
            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()

            while iter1.MoveNext()
                  && iter2.MoveNext() do
                result.Push(f i iter1.Current iter2.Current)
                |> ignore

                i <- i + 1

            result
        else
            Seq.mapi2 f vec1 vec2
            |> ofSeq
    // TODO: Ditto re: benchmark for Seq.map

    let mapFold
        (folder: 'State -> 'T -> 'Result * 'State)
        (initState: 'State)
        (vec: RRBVector<'T>)
        =
        let resultShouldBeTransient =
            vec
            |> isTransient

        let mutable transient =
            if resultShouldBeTransient then
                RRBTransientVector<'Result>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'Result>.MkEmpty() :> RRBVector<_>

        let mutable state = initState

        for item in vec do
            let item', state' = folder state item

            transient.Push item'
            |> ignore

            state <- state'

        let result =
            if resultShouldBeTransient then
                transient
            else
                transient.Persistent()

        result, state

    let mapFoldBack
        (folder: 'T -> 'State -> 'Result * 'State)
        (vec: RRBVector<'T>)
        (initState: 'State)
        =
        let resultShouldBeTransient =
            vec
            |> isTransient

        let mutable transient =
            if resultShouldBeTransient then
                RRBTransientVector<'Result>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'Result>.MkEmpty() :> RRBVector<_>

        let mutable state = initState

        for item in vec.RevIterItems() do
            let item', state' = folder item state

            transient.Push item'
            |> ignore

            state <- state'

        let result =
            if resultShouldBeTransient then
                transient
            else
                transient.Persistent()

        result
        |> rev,
        state

    let inline max (vec: RRBVector<'T>) =
        vec
        |> Seq.max

    let inline maxBy f (vec: RRBVector<'T>) =
        vec
        |> Seq.maxBy f

    let inline min (vec: RRBVector<'T>) =
        vec
        |> Seq.min

    let inline minBy f (vec: RRBVector<'T>) =
        vec
        |> Seq.minBy f

    let pairwise (vec: RRBVector<'T>) =
        if
            vec
            |> isTransient
        then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>

            use iter1 = vec.IterItems().GetEnumerator()
            use iter2 = vec.IterItems().GetEnumerator()

            if not (iter2.MoveNext()) then
                // Input vector was empty
                result
            else
                while iter1.MoveNext()
                      && iter2.MoveNext() do
                    result.Push(iter1.Current, iter2.Current)
                    |> ignore

                result
        else
            vec
            |> Seq.pairwise
            |> ofSeq
    // TODO: Ditto re: benchmark for Seq.map

    let partition pred (vec: RRBVector<'T>) =
        let resultShouldBeTransient =
            vec
            |> isTransient

        let mutable trueItems =
            if resultShouldBeTransient then
                RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        let mutable falseItems =
            if resultShouldBeTransient then
                RRBTransientVector<'T>.MkEmptyWithToken((vec :?> RRBTransientVector<'T>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

        for item in vec do
            if pred item then
                trueItems <- trueItems.Push item
            else
                falseItems <- falseItems.Push item

        if resultShouldBeTransient then
            trueItems, falseItems
        else
            trueItems.Persistent(), falseItems.Persistent()

    let permute f (vec: RRBVector<'T>) = // TODO: Implement a better version once we have transient RRBVectors, so we don't have to build an intermediate array
        let arr = Array.zeroCreate vec.Length
        let seen = Array.zeroCreate vec.Length
        let items = (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()
        let mutable i = 0

        while items.MoveNext() do
            let newIdx = f i
            arr.[newIdx] <- items.Current
            seen.[newIdx] <- 1uy
            i <- i + 1

        for i = 0 to seen.Length
                     - 1 do
            if
                seen.[i]
                <> 1uy
            then
                invalidArg "f" "The function did not compute a permutation."

        if
            vec
            |> isTransient
        then
            // Now mutate the transient in-place
            for i = 0 to arr.Length
                         - 1 do
                vec.Update(i, arr.[i])
                |> ignore

            vec :> RRBVector<_>
        else
            arr
            |> ofArray

    let inline pick f (vec: RRBVector<'T>) =
        vec
        |> Seq.pick f

    let inline reduce f (vec: RRBVector<'T>) =
        vec
        |> Seq.reduce f

    let reduceBack f (vec: RRBVector<'T>) =
        let f' = flip f in

        vec.RevIterItems()
        |> Seq.reduce f'

    let replicate count value =
        if count = 0 then
            empty<'T>
        else
            let mutable transient = RRBTransientVector<'T>.MkEmpty() :> RRBVector<_>

            for i = 1 to count do
                transient <- transient.Push value

            transient.Persistent()

    // TODO: Make a variant for when f is of type 'a -> 'a, which does a scan in-place for transients
    let inline scan f initState (vec: RRBVector<'T>) =
        vec
        |> Seq.scan f initState
        |> ofSeq

    let scanBack f (vec: RRBVector<'T>) initState =
        let f' = flip f

        vec.RevIterItems()
        |> Seq.scan f' initState
        |> ofSeq
        |> rev
    // TODO: Test this to make sure we got the scanBack implementation exactly right (e.g., initial value first, not last, in result)

    let singleton (item: 'T) =
        RRBPersistentVector<'T>(1, Literals.shiftSize, emptyNode, [| item |], 0) :> RRBVector<'T>

    let inline skip count (vec: RRBVector<'T>) = vec.Skip count

    let skipWhile pred (vec: RRBVector<'T>) = // TODO: Test this
        let rec loop pred n =
            if
                n
                >= vec.Length
            then
                empty<'T>
            elif pred vec.[n] then
                loop pred (n + 1)
            else
                vec.Skip n

        loop pred 0

    // TODO: Implement a sort-in-place algorithm (perhaps TimSort or Block sort) on transients, then benchmark against this simple version of sorting
    // Also think about the Block sort algorithm and how we can exploit the fact that we already have natural blocks, albeit of varying sizes
    // E.g., can we do an insertion sort on each leaf and then a merge sort between leaves (or pairs, quads, etc of leaves)?
    let sort (vec: RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlace arr

        arr
        |> ofArray

    let sortBy f (vec: RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlaceBy f arr

        arr
        |> ofArray

    let sortWith f (vec: RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlaceWith f arr

        arr
        |> ofArray

    let sortDescending (vec: RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlace arr
        System.Array.Reverse arr // Reverse in-place, more efficient than Array.rev since it doesn't create an intermediate array

        arr
        |> ofArray

    let sortByDescending f (vec: RRBVector<'T>) =
        let arr = toArray vec
        Array.sortInPlaceBy f arr
        System.Array.Reverse arr // Reverse in-place, more efficient than Array.rev since it doesn't create an intermediate array

        arr
        |> ofArray

    let inline splitAt idx (vec: RRBVector<'T>) = vec.Split idx

    let splitInto splitCount (vec: RRBVector<'T>) =
        let extra = if vec.Length % splitCount = 0 then 0 else 1

        let mutable result =
            chunkBySize
                (vec.Length
                 / splitCount
                 + extra)
                vec // TODO: Test that splits have the expected size

        if result.Length < splitCount then
            if
                result
                |> isTransient
            then
                let token = (result :?> RRBTransientVector<_>).Owner

                for i = 1 to splitCount
                             - result.Length do
                    result <- result.Push(RRBTransientVector<'T>.MkEmptyWithToken(token))
            else
                for i = 1 to splitCount
                             - result.Length do
                    result <- result.Push(RRBPersistentVector<'T>.MkEmpty())

            result
        else
            result

    let inline sum (vec: RRBVector<'T>) =
        vec
        |> Seq.sum

    let inline sumBy f (vec: RRBVector<'T>) =
        vec
        |> Seq.sumBy f

    let inline tail (vec: RRBVector<'T>) = vec.Remove 0

    let inline take n (vec: RRBVector<'T>) =
        // TODO: Document that RRBVector.take never throws, it just returns the whole vector if you take too much
        // if n > vec.Length then invalidArg "n" <| sprintf "Cannot take more items than a vector's length. Tried to take %d items from a vector of length %d" n vec.Length
        vec.Take n

    let inline takeWhile pred (vec: RRBVector<'T>) = // TODO: Try a version with vec.IterItems() and a counter, and see if that runs faster. Also update skipWhile if it does.
        let rec loop pred n =
            if
                n
                >= vec.Length
            then
                vec
            elif pred vec.[n] then
                loop pred (n + 1)
            else
                vec.Take n

        loop pred 0

    let inline truncate n (vec: RRBVector<'T>) = vec.Take n

    let inline tryFind f (vec: RRBVector<'T>) =
        vec
        |> Seq.tryFind f

    let inline tryFindBack f (vec: RRBVector<'T>) =
        vec.RevIterItems()
        |> Seq.tryFindBack f

    let inline tryFindIndex f (vec: RRBVector<'T>) =
        vec
        |> Seq.tryFindIndex f

    let inline tryFindIndexBack f (vec: RRBVector<'T>) =
        vec.RevIterItems()
        |> Seq.tryFindIndexBack f
        |> Option.map (fun idx ->
            vec.Length
            - 1
            - idx
        )

    let tryHead (vec: RRBVector<'T>) =
        if vec.Length = 0 then None else Some vec.[0]

    let inline tryItem idx (vec: RRBVector<'T>) =
        let idx =
            if idx < 0 then
                idx
                + vec.Length
            else
                idx

        if
            idx < 0
            || idx
               >= vec.Length
        then
            None
        else
            Some vec.[idx]

    let inline tryLast (vec: RRBVector<'T>) =
        if vec.Length = 0 then None else Some(last vec)

    let inline tryPick f (vec: RRBVector<'T>) =
        vec
        |> Seq.tryPick f

    let inline unfold f initState =
        Seq.unfold f initState
        |> ofSeq

    let unzip (vec: RRBVector<'T1 * 'T2>) =
        let resultShouldBeTransient =
            vec
            |> isTransient

        let mutable vec1 =
            if resultShouldBeTransient then
                RRBTransientVector<'T1>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T1>.MkEmpty() :> RRBVector<_>

        let mutable vec2 =
            if resultShouldBeTransient then
                RRBTransientVector<'T2>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T2>.MkEmpty() :> RRBVector<_>

        for a, b in vec do
            vec1 <- vec1.Push a
            vec2 <- vec2.Push b

        if resultShouldBeTransient then
            vec1, vec2
        else
            vec1.Persistent(), vec2.Persistent()

    let unzip3 (vec: RRBVector<'T1 * 'T2 * 'T3>) =
        let resultShouldBeTransient =
            vec
            |> isTransient

        let mutable vec1 =
            if resultShouldBeTransient then
                RRBTransientVector<'T1>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T1>.MkEmpty() :> RRBVector<_>

        let mutable vec2 =
            if resultShouldBeTransient then
                RRBTransientVector<'T2>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T2>.MkEmpty() :> RRBVector<_>

        let mutable vec3 =
            if resultShouldBeTransient then
                RRBTransientVector<'T3>.MkEmptyWithToken((vec :?> RRBTransientVector<_>).Owner)
                :> RRBVector<_>
            else
                RRBTransientVector<'T3>.MkEmpty() :> RRBVector<_>

        for a, b, c in vec do
            vec1 <- vec1.Push a
            vec2 <- vec2.Push b
            vec3 <- vec3.Push c

        if resultShouldBeTransient then
            vec1, vec2, vec3
        else
            vec1.Persistent(), vec2.Persistent(), vec3.Persistent()

    let inline where pred (vec: RRBVector<'T>) = filter pred vec

    let windowedSeq windowSize (vec: RRBVector<'T>) =
        // Sequence of vectors that share as much structure as possible with the original vector and with each other
        if vec.Length < windowSize then
            Seq.empty
        else if
            windowSize
            <= Literals.blockSize
        then
            // Sequence of tail-only vectors
            seq {
                let mutable tail = Array.zeroCreate windowSize
                let mutable count = 0

                let itemEnumerator =
                    (vec :> System.Collections.Generic.IEnumerable<'T>).GetEnumerator()

                while count < windowSize
                      && itemEnumerator.MoveNext() do
                    tail.[count] <- itemEnumerator.Current
                    count <- count + 1

                yield
                    RRBPersistentVector<'T>(windowSize, Literals.shiftSize, emptyNode, tail, 0)
                    :> RRBVector<'T>

                while itemEnumerator.MoveNext() do
                    tail <- Ficus.RRBArrayExtensions.RRBArrayExtensions.PopFirstAndPush(tail, itemEnumerator.Current)

                    yield
                        RRBPersistentVector<'T>(windowSize, Literals.shiftSize, emptyNode, tail, 0)
                        :> RRBVector<'T>
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

    let windowed windowSize (vec: RRBVector<'T>) =
        // Vector of vectors that share as much structure as possible with the original vector and with each other
        if vec.Length < windowSize then
            RRBPersistentVector<RRBVector<'T>>.MkEmpty() :> RRBVector<RRBVector<'T>>
        else
            windowedSeq windowSize vec
            |> ofSeq

    let zip (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T1>).Owner
                (vec2 :?> RRBTransientVector<'T2>).Owner

        if resultShouldBeTransient then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
                :> RRBVector<_>

            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()

            while iter1.MoveNext()
                  && iter2.MoveNext() do
                result.Push(iter1.Current, iter2.Current)
                |> ignore

            result
        else
            Seq.zip vec1 vec2
            |> ofSeq
    // TODO: Benchmark just like with Seq.map

    let zip3 (vec1: RRBVector<'T1>) (vec2: RRBVector<'T2>) (vec3: RRBVector<'T3>) =
        let resultShouldBeTransient =
            vec1
            |> isTransient
            && vec2
               |> isTransient
            && vec3
               |> isTransient
            && isSameObj
                (vec1 :?> RRBTransientVector<'T1>).Owner
                (vec2 :?> RRBTransientVector<'T2>).Owner
            && isSameObj
                (vec2 :?> RRBTransientVector<'T2>).Owner
                (vec3 :?> RRBTransientVector<'T3>).Owner

        if resultShouldBeTransient then
            let result =
                RRBTransientVector<_>.MkEmptyWithToken((vec1 :?> RRBTransientVector<'T1>).Owner)
                :> RRBVector<_>

            use iter1 = vec1.IterItems().GetEnumerator()
            use iter2 = vec2.IterItems().GetEnumerator()
            use iter3 = vec3.IterItems().GetEnumerator()

            while iter1.MoveNext()
                  && iter2.MoveNext()
                  && iter3.MoveNext() do
                result.Push(iter1.Current, iter2.Current, iter3.Current)
                |> ignore

            result
        else
            Seq.zip3 vec1 vec2 vec3
            |> ofSeq
// TODO: Benchmark just like with Seq.map
