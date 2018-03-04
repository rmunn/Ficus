module Tests


open Expecto
open Ficus

[<Tests>]
let tests =
  testList "samples" [
    testCase "A few sample counts" <| fun _ ->
      Expect.equal (BitUtils.bitcount 0) 0 "0 had wrong bitcount"
      Expect.equal (BitUtils.bitcount -1) 32 "-1 had wrong bitcount"
      Expect.equal (BitUtils.bitcount 8) 1 "8 had wrong bitcount"
      Expect.equal (BitUtils.bitcount 7) 3 "7 had wrong bitcount"
  ]
