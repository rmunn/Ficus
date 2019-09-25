module Ficus.Tests.ExperimentalFsCheckCommands

// https://fscheck.github.io/FsCheck/StatefulTesting.html

open Ficus.RRBArrayExtensions
open Ficus.RRBVectorBetterNodes
open Ficus.RRBVector
open FsCheck
open FsCheck.Experimental
open Expecto
open Expecto.Logging
open Expecto.Logging.Message

let logger = Log.create "Expecto"

let vecEqual vec arr msg =
    RRBVectorProps.checkProperties vec msg
    RRBVector.toArray vec = arr |@ msg

type VecState<'T>(vec : RRBVector<'T>) =
    let mutable m_vec = vec
    member public __.Vec
        with get() = m_vec
        and set vec' = m_vec <- vec'

type Op = Operation<VecState<int>, int []>
type Setup = Setup<VecState<int>, int []>
type Machine = Machine<VecState<int>, int []>

module VecCommands =
    let push n =
        { new Op() with
            member __.Run model = Array.copyAndAppend n model
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.push n
                // logger.debug (
                //     eventX "Push: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr (sprintf "push %d" n)
            override __.ToString() = sprintf "push %d" n
        }
    let pushGen = gen { let! n = Arb.generate<int> in return push n }
    let pushGen' = Arb.generate<int> |> Gen.map push  // Same thing
    let pop =
        { new Op() with
            override __.Pre model = Array.length model > 0
            member __.Run model = Array.copyAndPop model
            member __.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.pop
                // logger.debug (
                //     eventX "Pop: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr "pop"
            override __.ToString() = "pop"
        }
    let popGen = Gen.constant pop

    let calcSliceIdx length idx =
        if idx < 0
        then length + idx |> max 0
        else (length * idx) / 100

    let toStartPlusLen length (start,stop) =
        let a = calcSliceIdx length start |> max 0 |> min length
        let b = calcSliceIdx length stop |> max 0 |> min length
        if a <= b then a,(b-a) else b,(a-b)

    let slice start stop =
        { new Op() with
            member __.Run model =
                let from,len = toStartPlusLen (Array.length model) (start,stop)
                Array.sub model from len
            member this.Check (vecState,arr) =
                let from,len = toStartPlusLen (vecState.Vec.Length) (start,stop)
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec.Slice (from,(from+len-1))
                // logger.debug (
                //     eventX "Slice from {from} len {len} (was {start}..{stop}): old {old} -> new {new}"
                //     >> setField "from" from
                //     >> setField "len" len
                //     >> setField "start" start
                //     >> setField "stop" stop
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr (sprintf "slice %d len %d (was %d..%d)" from len start stop)
            override __.ToString() = sprintf "slice %d..%d" start stop
        }
    let genSliceIdx = Gen.frequency [3, Gen.choose(0,100); 1, Gen.choose(-32,100)]
    let sliceGen = gen { let! start = genSliceIdx in let! stop = genSliceIdx in return slice start stop }

    let join otherVec =
        { new Op() with
            member __.Run model = let arr = RRBVector.toArray otherVec in Array.append model arr
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec.Append otherVec
                // logger.debug (
                //     eventX "Merged with {other}: old {old} -> new {new}"
                //     >> setField "other" (RRBVecGen.vecToTreeReprStr otherVec)
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr (sprintf "append %A" (RRBVecGen.vecToTreeReprStr otherVec))
            override __.ToString() = sprintf "append %A" (RRBVecGen.vecToTreeReprStr otherVec)
        }
    let joinGen = gen { let! other = Arb.generate<RRBVector<int>> in return join other }

    let insert idx n =
        { new Op() with
            member __.Run model =
                let idx' = calcSliceIdx (Array.length model) idx |> min (Array.length model - 1)
                model |> Array.copyAndInsertAt idx' n
            member this.Check (vecState,arr) =
                let idx' = calcSliceIdx (vecState.Vec.Length) idx |> min (vecState.Vec.Length - 1)
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec.Insert idx' n
                // logger.debug (
                //     eventX "Insert {item} at idx {idx} (was {oldIdx}): old {old} -> new {new}"
                //     >> setField "item" n
                //     >> setField "idx" idx'
                //     >> setField "oldIdx" idx
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr (sprintf "insert %d at %d (was %d)" n idx' idx)
            override __.ToString() = sprintf "insert %d at %d" n idx
        }
    let insertGen = gen { let! idx = genSliceIdx in let! n = Arb.generate<int> in return insert idx n }

    let remove idx =
        { new Op() with
            override __.Pre model = Array.length model > 0
            member __.Run model =
                let idx' = calcSliceIdx (Array.length model) idx |> min (Array.length model - 1)
                model |> Array.copyAndRemoveAt idx'
            member this.Check (vecState,arr) =
                let idx' = calcSliceIdx (vecState.Vec.Length) idx |> min (vecState.Vec.Length - 1)
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec.Remove idx'
                // logger.debug (
                //     eventX "Remove at idx {idx} (was {oldIdx}): old {old} -> new {new}"
                //     >> setField "idx" idx'
                //     >> setField "oldIdx" idx
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr (sprintf "remove at %d (was %d)" idx' idx)
            override __.ToString() = sprintf "remove at %d" idx
        }
    let removeGen = genSliceIdx |> Gen.map remove
(* Disabling this one for now: the test works, but it produces SUPER-huge vectors really fast, so it slows down the test immensely. Re-enable it once its odds are FAR lower.
    let collect (f : int -> RRBVector<int>) =
        let g = f >> RRBVector.toArray
        { new Op() with
            member __.Run model =
                logger.debug (
                    eventX "Doing collect"
                )
                model |> Array.collect g
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.collect f
                logger.debug (
                    eventX "Collect f: old {old} -> new {new}"
                    >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                    >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                )
                vecEqual vecState.Vec arr "collect f"
            override __.ToString() = "collect f"
        }
    let collectGen = Arb.generate<int -> RRBVector<int>> |> Gen.map collect
*)

    let choose (f : int -> int option) =
        { new Op() with
            member __.Run model =
                model |> Array.choose f
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.choose f
                // logger.debug (
                //     eventX "Choose f: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr "choose f"
            override __.ToString() = "choose f"
        }
    let chooseGen = Arb.generate<int -> int option> |> Gen.map choose

    let distinct =
        { new Op() with
            member __.Run model =
                model |> Array.distinct
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.distinct
                // logger.debug (
                //     eventX "Distinct: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr "distinct"
            override __.ToString() = "distinct"
        }
    let distinctGen = Gen.constant distinct

    let distinctBy (f : int -> int) =
        { new Op() with
            member __.Run model =
                model |> Array.distinctBy f
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.distinctBy f
                // logger.debug (
                //     eventX "DistinctBy f: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr "distinctBy f"
            override __.ToString() = "distinctBy f"
        }
    let distinctByGen = Arb.generate<int -> int> |> Gen.map distinctBy

    let filter (f : int -> bool) =
        { new Op() with
            member __.Run model =
                model |> Array.filter f
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.filter f
                // logger.debug (
                //     eventX "Filter f: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr "filter f"
            override __.ToString() = "filter f"
        }
    let filterGen = Arb.generate<int -> bool> |> Gen.map filter

    let map (f : int -> int) =
        { new Op() with
            member __.Run model =
                model |> Array.map f
            member this.Check (vecState,arr) =
                let oldVec = vecState.Vec
                vecState.Vec <- vecState.Vec |> RRBVector.map f
                // logger.debug (
                //     eventX "Map f: old {old} -> new {new}"
                //     >> setField "old" (RRBVecGen.vecToTreeReprStr oldVec)
                //     >> setField "new" (RRBVecGen.vecToTreeReprStr vecState.Vec)
                // )
                vecEqual vecState.Vec arr "map f"
            override __.ToString() = "map f"
        }
    let mapGen = Arb.generate<int -> int> |> Gen.map map

    let genOp = Gen.oneof [ pushGen; popGen; sliceGen; joinGen; insertGen; removeGen; chooseGen; distinctGen; distinctByGen; filterGen; mapGen ]
    // TODO: Consider using Gen.frequency to adjust the odds, so we get more inserts and removes and fewer collects and joins.

    let create vec =
        { new Setup() with
            member __.Actual() = new VecState<int>(vec)
            member __.Model() = RRBVector.toArray vec
        }

    let machine initial =
        { new Machine() with
            member __.Setup = create initial |> Gen.constant |> Arb.fromGen
            member __.Next arr = if arr.Length > 0 then genOp else pushGen
        }
