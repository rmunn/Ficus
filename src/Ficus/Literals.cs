namespace Ficus;

public static class Literals
{
    public const int shiftSize = 5; // How many bits of the index are used at each level. Maxiumum 7
    public const int blockSize = 32; // Should always be 2 ^ shiftSize, e.g. if shiftSize becomes 4 this should become 16
    public const int blockIndexMask = blockSize - 1; // Use with & to get the current level's index
    public const int radixSearchErrorMax = 2; // Number of extra search steps to allow; 2 is a good balance
    public const int eMaxPlusOne = radixSearchErrorMax + 1;
    public const int blockSizeMin = blockSize - (radixSearchErrorMax / 2);
}
