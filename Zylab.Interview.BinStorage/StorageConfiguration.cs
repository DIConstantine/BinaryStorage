namespace Zylab.Interview.BinStorage {
    public class StorageConfiguration {
        /// <summary>
        /// Maximum size in bytes of the storage file
        /// Zero means unlimited
        /// </summary>
        public long MaxStorageFile { get; set; }

        /// <summary>
        /// Maximum size in bytes of the index file
        /// Zero means unlimited
        /// </summary>
        public long MaxIndexFile { get; set; }

        /// <summary>
        /// Storage might compress data during persistence,
        /// if its size is greater than this value
        /// </summary>
        public long CompressionThreshold { get; set; }

        /// <summary>
        /// Folder where implementation should store Index and Storage File
        /// </summary>
        public string WorkingFolder { get; set; }
    }

}
