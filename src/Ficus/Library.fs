namespace Ficus

module BitUtils =
    // https://gist.github.com/rmunn/bc49d32a586cdfa5bcab1c3e7b45d7ac
    let bitcount n =
        let count2 = n - ((n >>> 1) &&& 0x55555555)
        let count4 = (count2 &&& 0x33333333) + ((count2 >>> 2) &&& 0x33333333)
        let count8 = (count4 + (count4 >>> 4)) &&& 0x0f0f0f0f
        (count8 * 0x01010101) >>> 24
