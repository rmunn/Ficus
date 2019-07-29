module ExpectoTemplate.RRBVectorTransientCommands

// https://fscheck.github.io/FsCheck/StatefulTesting.html

open Ficus.RRBVectorBetterNodes
open Ficus.RRBVector
open FsCheck

(*
TODO: Write a test with the following structure:

- Start with a persistent
- Save an array copy of it to refer back to
- Turn it into a transient
- Split the transient up into many pieces
- Do a bunch of these commands to the various pieces
- After each one, make sure the original persistent is still intact and hasn't been edited
- Join the pieces together
- Do a bunch more commands to the rejoined vector
- After each one, make sure the original persistent is still intact and hasn't been edited


Another test would be:

- Make a persistent, save it and an array copy
- Turn it transient
- Feed the transient to a MailboxProcessor
- Send it a bunch of commands (insert, remove, update, push, pop)
- Each time the MailboxProcessor runs, store current ThreadId in a set
- After the test, print out (at logging level debug) the set of ThreadIds
- Check whether the set is ever greater than 1 in size, and if so, log that fact at info level (then comment out later)
*)

type Cmd = Command<RRBTransientVector<int>, RRBPersistentVector<int>>
// TODO: Model should become RRBPersistentVector<int> * RRBPersistentVector<int> * int[], where the second one is the original vector (which should never change) and the third is the array of the original vector (which we constantly compare the original vector to to ensure it hasn't changed)

// TODO: Rename "vec" and "arr" to "tvec" and "pvec" respectively, and do so all down the line
let vecEqual vec arr msg =
    RRBVectorProps.checkPropertiesSimple vec
    vec = arr |@ msg

module VecCommands =
    let push1 = { new Cmd()
                  with override __.RunActual vec = vec.Push 42 :?> RRBTransientVector<_>
                       override __.RunModel vec = vec.Push 42 :?> RRBPersistentVector<_>
                       override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After pushing 42 once, vec != arr"
                       override __.ToString() = "push 42 once" }

    let push4 = { new Cmd()
                  with override __.RunActual vec = (((vec.Push 1).Push 2).Push 3).Push 4 :?> RRBTransientVector<_>
                       override __.RunModel vec =  vec |> RRBVector.push 1 |> RRBVector.push 2 |> RRBVector.push 3 |> RRBVector.push 4 :?> RRBPersistentVector<_>
                       override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After pushing four items, vec != arr"
                       override __.ToString() = "push four items" }

    let push9 = { new Cmd()
                  with override __.RunActual vec =
                                                   vec.Push 11 |> ignore
                                                   vec.Push 12 |> ignore
                                                   vec.Push 13 |> ignore
                                                   vec.Push 14 |> ignore
                                                   vec.Push 15 |> ignore
                                                   vec.Push 16 |> ignore
                                                   vec.Push 17 |> ignore
                                                   vec.Push 18 |> ignore
                                                   vec.Push 19 :?> RRBTransientVector<_>
                       override __.RunModel vec = vec |> RRBVector.push 11 |> RRBVector.push 12 |> RRBVector.push 13 |> RRBVector.push 14 |> RRBVector.push 15 |> RRBVector.push 16 |> RRBVector.push 17 |> RRBVector.push 18 |> RRBVector.push 19 :?> RRBPersistentVector<_>
                       override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After pushing nine items, vec != arr"
                       override __.ToString() = "push nine items" }

    let push33 = { new Cmd()
                   with override __.RunActual vec = seq { 101..133 } |> Seq.fold (fun (vec : RRBTransientVector<_>) n -> vec.Push n :?> RRBTransientVector<_>) vec
                        override __.RunModel vec = seq { 101..133 } |> Seq.fold (fun (vec : RRBPersistentVector<_>) n -> vec.Push n :?> RRBPersistentVector<_>) vec
                        override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After pushing 33 items, vec != arr"
                        override __.ToString() = "push 33 items" }

    let pop = { new Cmd()
                with override __.RunActual vec = vec |> RRBVector.pop :?> RRBTransientVector<_>
                     override __.RunModel vec = vec |> RRBVector.pop :?> RRBPersistentVector<_>
                     override __.Pre(arr) = arr.Length > 0
                     override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After popping, vec != arr"
                     override __.ToString() = "pop" }
    let insert5AtHead = { new Cmd()
                          with override __.RunActual vec = vec |> RRBVector.insert 0 5 :?> RRBTransientVector<_>
                               override __.RunModel vec = vec |> RRBVector.insert 0 5 :?> RRBPersistentVector<_>
                               override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After inserting 5, vec != arr"
                               override __.ToString() = "insert 5 at head" }
    let insert7InFirstLeaf = { new Cmd()
                               with override __.RunActual vec = vec |> RRBVector.insert 3 7 :?> RRBTransientVector<_>
                                    override __.RunModel vec = vec |> RRBVector.insert 3 7 :?> RRBPersistentVector<_>
                                    override __.Pre(arr) = arr.Length >= 3
                                    override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After inserting 7, vec != arr"
                                    override __.ToString() = "insert 7 in first leaf" }
    let insert9InTail = { new Cmd()
                          with override __.RunActual vec = vec |> RRBVector.insert (vec.Length - 2) 9 :?> RRBTransientVector<_>
                               override __.RunModel vec = vec |> RRBVector.insert (vec.Length - 2) 9 :?> RRBPersistentVector<_>
                               override __.Pre(arr) = arr.Length >= 2
                               override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After inserting 9, vec != arr"
                               override __.ToString() = "insert 9 in tail" }
    let removeFromHead = { new Cmd()
                           with override __.RunActual vec = vec |> RRBVector.remove 0 :?> RRBTransientVector<_>
                                override __.RunModel vec = vec |> RRBVector.remove 0 :?> RRBPersistentVector<_>
                                override __.Pre(arr) = arr.Length > 0
                                override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After removing from head, vec != arr"
                                override __.ToString() = "remove from head" }
    let removeFromFirstLeaf = { new Cmd()
                                with override __.RunActual vec = vec |> RRBVector.remove 3 :?> RRBTransientVector<_>
                                     override __.RunModel vec = vec |> RRBVector.remove 3 :?> RRBPersistentVector<_>
                                     override __.Pre(arr) = arr.Length > 3
                                     override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After removing at idx 3, vec != arr"
                                     override __.ToString() = "remove from first leaf" }
    let removeFromTail = { new Cmd()
                           with override __.RunActual vec = vec |> RRBVector.remove (vec.Length - 2) :?> RRBTransientVector<_>
                                override __.RunModel vec = vec |> RRBVector.remove (vec.Length - 2) :?> RRBPersistentVector<_>
                                override __.Pre(arr) = arr.Length >= 2
                                override __.Post(vec, arr) = vecEqual (vec :> RRBVector<_>) (arr :> RRBVector<_>) "After removing from tail, vec != arr"
                                override __.ToString() = "remove from tail" }

open VecCommands

let cmdsExtraSmall = [push1; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsSmall = [push1; push4; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsMedium = [push1; push4; push9; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsLarge = [push1; push4; push9; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsExtraLarge = [push4; push9; push33; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]

let specExtraSmallFromData (data : RRBPersistentVector<int>) =
    { new ICommandGenerator<RRBTransientVector<int>, RRBPersistentVector<int>> with
        member __.InitialActual = data.Transient()
        member __.InitialModel = data
        member __.Next model = Gen.elements cmdsExtraSmall }

let specSmallFromData (data : RRBPersistentVector<int>) =
    { new ICommandGenerator<RRBTransientVector<int>, RRBPersistentVector<int>> with
        member __.InitialActual = data.Transient()
        member __.InitialModel = data
        member __.Next model = Gen.elements cmdsSmall }

let specMediumFromData (data : RRBPersistentVector<int>) =
    { new ICommandGenerator<RRBTransientVector<int>, RRBPersistentVector<int>> with
        member __.InitialActual = data.Transient()
        member __.InitialModel = data
        member __.Next model = Gen.elements cmdsMedium }

let specLargeFromData (data : RRBPersistentVector<int>) =
    { new ICommandGenerator<RRBTransientVector<int>, RRBPersistentVector<int>> with
        member __.InitialActual = data.Transient()
        member __.InitialModel = data
        member __.Next model = Gen.elements cmdsLarge }

let specExtraLargeFromData (data : RRBPersistentVector<int>) =
    { new ICommandGenerator<RRBTransientVector<int>, RRBPersistentVector<int>> with
        member __.InitialActual = data.Transient()
        member __.InitialModel = data
        member __.Next model = Gen.elements cmdsExtraLarge }

let specExtraSmallFromEmpty = specExtraSmallFromData (RRBVector.empty<int> :?> RRBPersistentVector<int>)
let specSmallFromEmpty = specSmallFromData (RRBVector.empty<int> :?> RRBPersistentVector<int>)
let specMediumFromEmpty = specMediumFromData (RRBVector.empty<int> :?> RRBPersistentVector<int>)
let specLargeFromEmpty = specLargeFromData (RRBVector.empty<int> :?> RRBPersistentVector<int>)
let specExtraLargeFromEmpty = specExtraLargeFromData (RRBVector.empty<int> :?> RRBPersistentVector<int>)

let almostFullSapling = RRBVector.ofArray [|1..63|] :?> RRBPersistentVector<int>

let specExtraSmallFromAlmostFullSapling = specExtraSmallFromData almostFullSapling
let specSmallFromAlmostFullSapling = specSmallFromData almostFullSapling
let specMediumFromAlmostFullSapling = specMediumFromData almostFullSapling
let specLargeFromAlmostFullSapling = specLargeFromData almostFullSapling
let specExtraLargeFromAlmostFullSapling = specExtraLargeFromData almostFullSapling

(*
let fixedSpec =
  [ push1; insert5AtHead; insert9InTail; insert7InFirstLeaf;
    push1; insert5AtHead; push1; push1; insert5AtHead; push1;
    push1; insert9InTail; insert7InFirstLeaf; pop; insert5AtHead;
    insert7InFirstLeaf; removeFromHead; removeFromFirstLeaf; pop; pop;
    removeFromFirstLeaf; removeFromTail ]

let xsSpec =
  [ insert5AtHead; push1; push1; push1;
    insert7InFirstLeaf; insert9InTail; insert5AtHead; insert9InTail;
    insert7InFirstLeaf; removeFromHead; removeFromHead;
    removeFromFirstLeaf; removeFromFirstLeaf; removeFromHead;
    insert7InFirstLeaf; push1; insert7InFirstLeaf; insert9InTail;
    insert7InFirstLeaf; push1; push1; insert9InTail;
    removeFromHead; insert7InFirstLeaf ]

let medSpec =
  [ push4; push1; push4; push4;
    removeFromHead; insert9InTail; removeFromTail; push4;
    removeFromFirstLeaf; insert9InTail ]

let med2Spec =
  [ push4; insert5AtHead; insert7InFirstLeaf; removeFromTail;
    insert7InFirstLeaf; push4; insert5AtHead;
    removeFromFirstLeaf; push1; push1; push4;
    removeFromHead; push1; removeFromTail; insert9InTail ]

let shortenSpec1 =
  [ push4; push1; pop; pop; insert5AtHead;
    insert7InFirstLeaf; removeFromFirstLeaf; removeFromTail;
    push4; removeFromHead; removeFromFirstLeaf; pop;
    removeFromFirstLeaf; insert7InFirstLeaf; push1; insert9InTail;
    insert7InFirstLeaf; removeFromTail; push9; insert5AtHead;
    removeFromHead; push9; pop; insert5AtHead; removeFromTail;
    insert5AtHead; removeFromHead; insert7InFirstLeaf; removeFromHead;
    insert9InTail; insert9InTail; push1; insert9InTail;
    insert9InTail; insert5AtHead; push1; removeFromTail;
    insert7InFirstLeaf; removeFromHead; pop; push9; push9;
    push9; insert5AtHead; push1; removeFromHead;
    push9; removeFromFirstLeaf; removeFromHead; insert9InTail ]

let shortenSpec2 =
  [ push9; insert9InTail; insert5AtHead; push4; pop;
    removeFromFirstLeaf; push4; removeFromFirstLeaf;
    insert5AtHead; removeFromTail; pop; removeFromTail;
    insert7InFirstLeaf; push4; insert7InFirstLeaf;
    insert5AtHead; push33; removeFromHead; insert5AtHead;
    insert5AtHead; removeFromFirstLeaf; pop; removeFromTail;
    insert9InTail; insert5AtHead; insert9InTail; pop; insert9InTail;
    push9; insert5AtHead; removeFromFirstLeaf;
    removeFromFirstLeaf; insert9InTail ]

let shortenSpec3 =
  [ insert5AtHead; push9; pop; removeFromFirstLeaf;
    removeFromTail; insert7InFirstLeaf; insert9InTail; insert9InTail;
    removeFromTail; push4; push4; pop; removeFromTail;
    removeFromHead; push9; removeFromHead; removeFromFirstLeaf;
    push1; insert9InTail; pop; push4; removeFromHead;
    removeFromHead; removeFromTail; push4; insert5AtHead;
    removeFromFirstLeaf; insert5AtHead; pop; insert9InTail; pop;
    removeFromTail; push4; removeFromTail; removeFromHead; pop;
    insert7InFirstLeaf; insert7InFirstLeaf; removeFromTail;
    insert9InTail; pop; insert5AtHead; push4; insert5AtHead;
    push1; removeFromHead; removeFromTail; removeFromFirstLeaf;
    insert5AtHead; insert5AtHead; pop; insert7InFirstLeaf;
    insert9InTail; push1; removeFromFirstLeaf; insert9InTail;
    insert7InFirstLeaf; push4; removeFromFirstLeaf;
    removeFromTail; insert9InTail; pop; push1; insert9InTail;
    push9; insert9InTail; push1; push4;
    push9; insert9InTail; removeFromTail; removeFromFirstLeaf;
    removeFromFirstLeaf; insert9InTail ]

let shortenSpec4 =
  [ push33; push9; removeFromHead; push9;
    removeFromHead; insert7InFirstLeaf; push9; insert9InTail;
    pop; push4; insert9InTail
  ]
*)
