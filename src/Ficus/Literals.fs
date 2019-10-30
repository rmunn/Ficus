module Ficus.Literals

let [<Literal>] internal shiftSize = 5  // How many bits of the index are used at each level
let [<Literal>] internal radixSearchErrorMax = 2  // Number of extra search steps to allow; 2 is a good balance
let [<Literal>] internal eMaxPlusOne = 3  // radixSearchErrorMax + 1
let [<Literal>] internal blockSize = 32  // 2 ** shiftSize
let [<Literal>] internal blockIndexMask = 31  // 2 ** shiftSize - 1. Use with &&& to get the current level's index
let [<Literal>] internal blockSizeMin = 31  // blockSize - (radixSearchErrorMax / 2).
