module Ficus.Literals

[<Literal>]
let internal shiftSize = 5 // How many bits of the index are used at each level

[<Literal>]
let internal radixSearchErrorMax = 2 // Number of extra search steps to allow; 2 is a good balance

[<Literal>]
let internal eMaxPlusOne = 3 // radixSearchErrorMax + 1

[<Literal>]
let internal blockSize = 32 // 2 ** shiftSize

[<Literal>]
let internal blockIndexMask = 31 // 2 ** shiftSize - 1. Use with &&& to get the current level's index

[<Literal>]
let internal blockSizeMin = 31 // blockSize - (radixSearchErrorMax / 2).
