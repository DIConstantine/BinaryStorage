namespace Zylab.Interview.BinStorage {
    public struct IndexData {
        public long offset;
        public long size;
        public bool isCompressed;

        public static readonly IndexData Empty = new IndexData();
    }
}
