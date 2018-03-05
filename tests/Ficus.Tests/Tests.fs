module Tests

open Expecto
open Ficus

let slowBitcount n =
    // Slow, but obviously correct, algorithm
    let mutable currentBit = 1
    let mutable count = 0
    while currentBit <> 0 do
        if n &&& currentBit <> 0 then count <- count + 1
        currentBit <- currentBit <<< 1
    count

[<Tests>]
let tests =
  testList "samples" [
    testProperty "Bitcount produces right results" <| fun n ->
        BitUtils.bitcount n = slowBitcount n
  ]
