module Ficus.Literals

let [<Literal>] internal blockSizeShift = 5  // TODO: Rename to "shiftSize"
let [<Literal>] internal radixSearchErrorMax = 2  // Number of extra search steps to allow; 2 is a good balance
let [<Literal>] internal eMaxPlusOne = 3  // radixSearchErrorMax + 1
let [<Literal>] internal blockSize = 32  // 2 ** blockSizeShift
let [<Literal>] internal blockIndexMask = 31  // 2 ** blockSizeShift - 1. Use with &&& to get the current level's index
let [<Literal>] internal blockSizeMin = 31  // blockSize - (radixSearchErrorMax / 2).
