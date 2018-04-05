module ExpectoTemplate.RRBVectorFsCheckCommands

// https://fscheck.github.io/FsCheck/StatefulTesting.html

open Ficus.RRBArrayExtensions
open Ficus.RRBVectorNodes
open Ficus.RRBVector
open FsCheck
open Expecto

type Cmd = Command<RRBVector<int>, int []>

let vecEqual vec arr msg =
    RRBVectorProps.checkPropertiesSimple vec
    (RRBVector.toArray vec) = arr |@ msg

module VecCommands =
    let push1 = { new Cmd()
                  with override __.RunActual vec = vec |> RRBVector.push 42
                       override __.RunModel arr = arr |> Array.copyAndAppend 42
                       override __.Post(vec, arr) = vecEqual vec arr "After pushing 42 once, vec != arr"
                       override __.ToString() = "push 42 once" }

    let push4 = { new Cmd()
                  with override __.RunActual vec = vec |> RRBVector.push 1 |> RRBVector.push 2 |> RRBVector.push 3 |> RRBVector.push 4
                       override __.RunModel arr = Array.append arr [|1;2;3;4|]
                       override __.Post(vec, arr) = vecEqual vec arr "After pushing four items, vec != arr"
                       override __.ToString() = "push four items" }

    let push9 = { new Cmd()
                  with override __.RunActual vec = vec |> RRBVector.push 11 |> RRBVector.push 12 |> RRBVector.push 13 |> RRBVector.push 14 |> RRBVector.push 15 |> RRBVector.push 16 |> RRBVector.push 17 |> RRBVector.push 18 |> RRBVector.push 19
                       override __.RunModel arr = Array.append arr [|11..19|]
                       override __.Post(vec, arr) = vecEqual vec arr "After pushing nine items, vec != arr"
                       override __.ToString() = "push nine items" }

    let push33 = { new Cmd()
                   with override __.RunActual vec = seq { 101..133 } |> Seq.fold (fun vec n -> vec |> RRBVector.push n) vec
                        override __.RunModel arr = Array.append arr [|101..133|]
                        override __.Post(vec, arr) = vecEqual vec arr "After pushing 33 items, vec != arr"
                        override __.ToString() = "push 33 items" }

    let pop = { new Cmd()
                with override __.RunActual vec = vec |> RRBVector.pop
                     override __.RunModel arr = arr |> Array.copyAndPop
                     override __.Pre(arr) = arr.Length > 0
                     override __.Post(vec, arr) = vecEqual vec arr "After popping, vec != arr"
                     override __.ToString() = "pop" }
    let insert5AtHead = { new Cmd()
                          with override __.RunActual vec = vec |> RRBVector.insert 0 5
                               override __.RunModel arr = arr |> Array.copyAndInsertAt 0 5
                               override __.Post(vec, arr) = vecEqual vec arr "After inserting 5, vec != arr"
                               override __.ToString() = "insert 5 at head" }
    let insert7InFirstLeaf = { new Cmd()
                               with override __.RunActual vec = vec |> RRBVector.insert 3 7
                                    override __.RunModel arr = arr |> Array.copyAndInsertAt 3 7
                                    override __.Pre(arr) = arr.Length >= 3
                                    override __.Post(vec, arr) = vecEqual vec arr "After inserting 7, vec != arr"
                                    override __.ToString() = "insert 7 in first leaf" }
    let insert9InTail = { new Cmd()
                          with override __.RunActual vec = vec |> RRBVector.insert (vec.Length - 2) 9
                               override __.RunModel arr = arr |> Array.copyAndInsertAt (Array.length arr - 2) 9
                               override __.Pre(arr) = arr.Length >= 2
                               override __.Post(vec, arr) = vecEqual vec arr "After inserting 9, vec != arr"
                               override __.ToString() = "insert 9 in tail" }
    let removeFromHead = { new Cmd()
                           with override __.RunActual vec = vec |> RRBVector.remove 0
                                override __.RunModel arr = arr |> Array.copyAndRemoveFirst
                                override __.Pre(arr) = arr.Length > 0
                                override __.Post(vec, arr) = vecEqual vec arr "After removing from head, vec != arr"
                                override __.ToString() = "remove from head" }
    let removeFromFirstLeaf = { new Cmd()
                                with override __.RunActual vec = vec |> RRBVector.remove 3
                                     override __.RunModel arr = arr |> Array.copyAndRemoveAt 3
                                     override __.Pre(arr) = arr.Length > 3
                                     override __.Post(vec, arr) = vecEqual vec arr "After removing at idx 3, vec != arr"
                                     override __.ToString() = "remove from first leaf" }
    let removeFromTail = { new Cmd()
                           with override __.RunActual vec = vec |> RRBVector.remove (vec.Length - 2)
                                override __.RunModel arr = arr |> Array.copyAndRemoveAt (Array.length arr - 2)
                                override __.Pre(arr) = arr.Length >= 2
                                override __.Post(vec, arr) = vecEqual vec arr "After removing from tail, vec != arr"
                                override __.ToString() = "remove from tail" }

open VecCommands

let cmdsExtraSmall = [push1; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsSmall = [push1; push4; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsMedium = [push1; push4; push9; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsLarge = [push1; push4; push9; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]
let cmdsExtraLarge = [push4; push9; push33; pop; insert5AtHead; insert7InFirstLeaf; insert9InTail; removeFromHead; removeFromFirstLeaf; removeFromTail]

let specExtraSmallFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model = Gen.elements cmdsExtraSmall }

let specSmallFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model = Gen.elements cmdsSmall }

let specMediumFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model = Gen.elements cmdsMedium }

let specLargeFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model = Gen.elements cmdsLarge }

let specExtraLargeFromData data =
    { new ICommandGenerator<RRBVector<int>, int []> with
        member __.InitialActual = data
        member __.InitialModel = RRBVector.toArray data
        member __.Next model = Gen.elements cmdsExtraLarge }

let specExtraSmallFromEmpty = specExtraSmallFromData (RRBVector.empty<int>)
let specSmallFromEmpty = specSmallFromData (RRBVector.empty<int>)
let specMediumFromEmpty = specMediumFromData (RRBVector.empty<int>)
let specLargeFromEmpty = specLargeFromData (RRBVector.empty<int>)
let specExtraLargeFromEmpty = specExtraLargeFromData (RRBVector.empty<int>)

let almostFullSapling = RRBVector.ofArray [|1..63|]

let specExtraSmallFromAlmostFullSapling = specExtraSmallFromData almostFullSapling
let specSmallFromAlmostFullSapling = specSmallFromData almostFullSapling
let specMediumFromAlmostFullSapling = specMediumFromData almostFullSapling
let specLargeFromAlmostFullSapling = specLargeFromData almostFullSapling
let specExtraLargeFromAlmostFullSapling = specExtraLargeFromData almostFullSapling

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
