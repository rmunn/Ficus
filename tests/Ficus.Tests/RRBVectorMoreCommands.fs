module Ficus.Tests.RRBVectorMoreCommands

// https://fscheck.github.io/FsCheck/StatefulTesting.html

open Ficus.RRBArrayExtensions
open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck
open Expecto.Logging

type Cmd = Command<RRBVector<int>, int []>

let vecEqual vec arr msg =
    RRBVectorProps.checkPropertiesSimple vec
    (RRBVector.toArray vec) = arr |@ msg

module ParameterizedVecCommands =
    let logger = Log.create "Expecto"

    let wrapCmdWithLogging (cmd : Cmd) = { new Cmd()
                                           with override __.RunActual vec =
                                                   let result = cmd.RunActual vec
                                                   logger.info (
                                                       Message.eventX "After {cmd}, vec was {vec}"
                                                       >> Message.setField "cmd" (cmd.ToString())
                                                       >> Message.setField "vec" (RRBVectorGen.vecToTreeReprStr result)
                                                   )
                                                   result
                                                override __.RunModel arr = cmd.RunModel arr
                                                override __.Pre(arr) = cmd.Pre(arr)
                                                override __.Post(vec, arr) = cmd.Post(vec, arr)
                                                override __.ToString() = cmd.ToString() }
    // let wrapCmd = wrapCmdWithLogging  // To enable logging, uncomment this line
    let wrapCmd = id  // To disable logging, uncomment this line
    let push n = { new Cmd()
                   with override __.RunActual vec = seq { 1 .. n } |> Seq.fold (fun vec x -> vec |> RRBVector.push x) vec
                        override __.RunModel arr = Array.append arr [| 1 .. n |]
                        override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After pushing %d items, vec != arr" n
                        override __.ToString() = sprintf "push %d" n }
                 |> wrapCmd

    let pop n = { new Cmd()
                  with override __.RunActual vec = seq { 1 .. n } |> Seq.fold (fun vec x -> vec |> RRBVector.pop) vec
                       override __.RunModel arr = arr |> Array.truncate (arr.Length - n |> max 0)
                       override __.Pre(arr) = arr.Length >= n
                       override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After popping %d items, vec != arr" n
                       override __.ToString() = sprintf "pop %d" n }
                |> wrapCmd

    let insert (idx,item) = { new Cmd()
                            with override __.RunActual vec = let idx' = if idx < 0 then idx + vec.Length else idx in vec |> RRBVector.insert idx' item
                                 override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.copyAndInsertAt idx' item
                                 override __.Pre(arr) = (abs idx) <= arr.Length
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After inserting %d at index %d, vec != arr" item idx
                                 override __.ToString() = sprintf "insert (%d,%d)" idx item }
                            |> wrapCmd

    let remove idx = { new Cmd()
                       with override __.RunActual vec = let idx' = if idx < 0 then idx + vec.Length else idx in vec |> RRBVector.remove idx'
                            override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.copyAndRemoveAt idx'
                            override __.Pre(arr) = (abs idx) <= arr.Length && idx <> arr.Length
                            override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After removing item at index %d, vec != arr" idx
                            override __.ToString() = sprintf "remove %d" idx }
                     |> wrapCmd

    let take idx = { new Cmd()
                     with override __.RunActual vec = let idx' = if idx < 0 then idx + vec.Length else idx in vec |> RRBVector.take idx'
                          override __.RunModel arr = let idx' = if idx < 0 then idx + arr.Length else idx in arr |> Array.truncate idx'
                          override __.Pre(arr) = (abs idx) <= arr.Length
                          override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After keeping only first %d items, vec != arr" idx
                          override __.ToString() = sprintf "take %d" idx }
                     |> wrapCmd

    let skip idx = { new Cmd()
                     with override __.RunActual vec = vec |> RRBVector.skip idx
                          override __.RunModel arr = arr |> Array.skip idx
                          override __.Pre(arr) = idx <= arr.Length
                          override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After skipping %d items and keeping rest, vec != arr" idx
                          override __.ToString() = sprintf "skip %d" idx }
                     |> wrapCmd

    let split (NonNegativeInt idx,takeOrSkip) = if takeOrSkip then take idx else skip idx

    let mergeL otherVec = { new Cmd()
                            with override __.RunActual vec = RRBVector.append otherVec vec
                                 override __.RunModel arr = Array.append (RRBVector.toArray otherVec) arr
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After merging on the left, vec != arr"
                                 override __.ToString() = sprintf "mergeL %A" (RRBVectorGen.vecToTreeReprStr otherVec) }
                          |> wrapCmd

    let mergeR otherVec = { new Cmd()
                            with override __.RunActual vec = RRBVector.append vec otherVec
                                 override __.RunModel arr = Array.append arr (RRBVector.toArray otherVec)
                                 override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After merging on the right, vec != arr"
                                 override __.ToString() = sprintf "mergeR %A" (RRBVectorGen.vecToTreeReprStr otherVec) }
                          |> wrapCmd

    let merge (otherVec,leftOrRight) = if leftOrRight then mergeL otherVec else mergeR otherVec

    let calcSliceIdx length idx =
        if idx < 0
        then length + idx |> max 0
        else (length * idx) / 100

    let toStartPlusLen length (start,stop) =
        let a = calcSliceIdx length start |> max 0 |> min length
        let b = calcSliceIdx length stop |> max 0 |> min length
        if a <= b then a,(b-a) else b,(a-b)

    let slice start stop = { new Cmd() with
                                override __.RunActual vec =
                                    let from,len = toStartPlusLen (vec.Length) (start,stop)
                                    vec.[from .. from+len-1]

                                override __.RunModel arr =
                                    let from,len = toStartPlusLen (Array.length arr) (start,stop)
                                    Array.sub arr from len
                                override __.Post(vec, arr) =
                                    let from,len = toStartPlusLen (Array.length arr) (start,stop)
                                    vecEqual vec arr <| sprintf "After slicing from %d to %d (orig %d to %d), vec != arr" from (from+len-1) start stop
                                override __.ToString() = sprintf "slice %d %d" start stop }
                            |> wrapCmd
    let genSliceIdx = Gen.frequency [3, Gen.choose(0,100); 1, Gen.choose(-32,100)]

    let choose (f : int -> int option) = { new Cmd()
                                           with override __.RunActual vec = vec |> RRBVector.choose f
                                                override __.RunModel arr = arr |> Array.choose f
                                                override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After choose f, vec != arr"
                                                override __.ToString() = sprintf "choose choosef" }
                                         |> wrapCmd

    let distinct = { new Cmd()
                     with override __.RunActual vec = vec |> RRBVector.distinct
                          override __.RunModel arr = arr |> Array.distinct
                          override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After distinct, vec != arr"
                          override __.ToString() = sprintf "distinct" }
                     |> wrapCmd

    let distinctBy (f : int -> int) = { new Cmd()
                                        with override __.RunActual vec = vec |> RRBVector.distinctBy f
                                             override __.RunModel arr = arr |> Array.distinctBy f
                                             override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After distinctBy f, vec != arr"
                                             override __.ToString() = sprintf "distinctBy distinctf" }
                                      |> wrapCmd

    let filter (f : int -> bool) = { new Cmd()
                                     with override __.RunActual vec = vec |> RRBVector.filter f
                                          override __.RunModel arr = arr |> Array.filter f
                                          override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After filter f, vec != arr"
                                          override __.ToString() = sprintf "filter pred" }
                                   |> wrapCmd

    let map (f : int -> int) = { new Cmd()
                                 with override __.RunActual vec = vec |> RRBVector.map f
                                      override __.RunModel arr = arr |> Array.map f
                                      override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After map f, vec != arr"
                                      override __.ToString() = sprintf "map mapf" }
                               |> wrapCmd

    let partition b (f : int -> bool) = { new Cmd()
                                          with override __.RunActual vec = vec |> RRBVector.partition f |> (if b then fst else snd)
                                               override __.RunModel arr = arr |> Array.partition f |> (if b then fst else snd)
                                               override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After partitioning f and keeping %A items, vec != arr" b
                                               override __.ToString() = sprintf "partition %A partf" b }
                                        |> wrapCmd

    let rev _ = { new Cmd()
                with override __.RunActual vec = vec |> RRBVector.rev
                     override __.RunModel arr = arr |> Array.rev
                     override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After reversing, vec != arr"
                     override __.ToString() = "rev" }
                |> wrapCmd

    let scan (f : int -> int -> int) initState = { new Cmd()
                                                   with override __.RunActual vec = vec |> RRBVector.scan f initState
                                                        override __.RunModel arr = arr |> Array.scan f initState
                                                        override __.Post(vec, arr) = vecEqual vec arr <| sprintf "After scan f, vec != arr"
                                                        override __.ToString() = sprintf "scan scanf %d" initState }
                                                 |> wrapCmd

    // Generators for the above
    // TODO: Generate *non-negative* ints for indices
    let insertGen = Arb.generate<NonNegativeInt> |> Gen.two |> Gen.map (fun (NonNegativeInt n1, NonNegativeInt n2) -> insert (n1,n2))
    let removeGen = Arb.generate<NonNegativeInt> |> Gen.map (fun (NonNegativeInt n) -> remove n)
    let pushGen = Arb.generate<NonNegativeInt> |> Gen.map (fun (NonNegativeInt n) -> push n)
    let popGen = Arb.generate<NonNegativeInt> |> Gen.map (fun (NonNegativeInt n) -> pop n)
    let splitGen = Arb.generate<NonNegativeInt * bool> |> Gen.map split
    let mergeGen = Arb.generate<RRBVector<int> * bool> |> Gen.map merge
    let sliceGen = Gen.map2 slice genSliceIdx genSliceIdx
    // Simpler than: // let sliceGen = gen { let! start = genSliceIdx in let! stop = genSliceIdx in return slice start stop }
    let chooseGen = Arb.generate<int -> int option> |> Gen.map choose
    let distinctGen = Gen.constant distinct
    let distinctByGen = Arb.generate<int -> int> |> Gen.map distinctBy
    let filterGen = Arb.generate<int -> bool> |> Gen.map filter
    let mapGen = Arb.generate<int -> int> |> Gen.map map
    let partitionGen = (Arb.generate<bool>, Arb.generate<int -> bool>) ||> Gen.map2 partition
    let revGen = Gen.constant (rev ())
    let scanGen = (Arb.generate<int -> int -> int>, Arb.generate<int>) ||> Gen.map2 scan

open ParameterizedVecCommands
let specFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model =
            let len = Array.length model
            Gen.frequency [
                400_000, insertGen
                200_000, removeGen
                200_000, pushGen
                50_000, popGen
                len, splitGen
                100_000 - len |> max 0, mergeGen
                len, sliceGen
                len / 2, chooseGen
                len, distinctGen
                len, distinctByGen
                len / 4, filterGen
                20_000, mapGen
                len, partitionGen
                20_000, revGen
                25_000, scanGen
            ] }
