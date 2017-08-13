using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;

namespace Zylab.Interview.BinStorage {

    public class BinaryStorage : IBinaryStorage {

        private const string STORAGE_FILE_NAME = "storage.bin";
        private const string INDEX_FILE_NAME = "index.bin";

        private readonly StorageConfiguration configuration;
        private readonly FileStream storage;
        private readonly IIndexFile index;
        private readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim();
        private readonly Cache cache = new Cache(Utils.AVAILABLE_MEMORY_PROVIDER);
        private readonly object getLock = new object();

        //This constructor actually not needed, but as I'm not allowed to modify TestApp IndexFile will be passed as parameter
        public BinaryStorage(StorageConfiguration configuration) :
            this(Utils.CheckNotNull(configuration, Messages.ConfigurationNull), new IndexFile(Path.Combine(configuration.WorkingFolder, INDEX_FILE_NAME))) { }


        public BinaryStorage(StorageConfiguration configuration, IIndexFile index) {
            this.configuration = Utils.CheckNotNull(configuration, Messages.ConfigurationNull);
            storage = new FileStream(Path.Combine(configuration.WorkingFolder, STORAGE_FILE_NAME), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            this.index = index;
        }

        public void Add(string key, Stream data, StreamInfo parameters) {

            #region Check parameters
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(Messages.KeyNullOrEmpty);

            if (data == null || data.Length == 0)
                throw new ArgumentNullException(Messages.NoData);

            Utils.CheckNotNull(parameters, Messages.ParametersNull);

            if (parameters.Length != null && data.Length != parameters.Length)
                throw new ArgumentException(string.Format(Messages.LengthInconsistent, parameters.Length, data.Length));

            CheckHash(data, parameters);

            //It might be a case when this is not supported,
            //but let's leave it as it is for now
            data.Position = 0;
            #endregion

            //Add key to index and get id of entry associated with the key
            long id = index.Add(key);
            bool isCompressed = parameters.IsCompressed || data.Length > configuration.CompressionThreshold;
            IndexData indexData;

            // Do not compress if it's already compressed or is less than threshold
            if (parameters.IsCompressed || data.Length <= configuration.CompressionThreshold) {
                indexData = Store(data, isCompressed);
            } else
                //Memory stream is needed to read length of compressed data
                using (var memory = new MemoryStream()) {
                    using (var deflate = new DeflateStream(memory, CompressionLevel.Optimal, true)) {
                        data.CopyTo(deflate);
                    }
                    memory.Seek(0, SeekOrigin.Begin);
                    indexData = Store(memory, isCompressed);
                }

            //Add data to entry in index by id
            index.Add(id, indexData);
        }

        public Stream Get(string key) {
            return new MemoryStream(cache.GetOrAdd(key, GetData));
        }

        private byte[] GetData(string key) {
            var data = index.Get(key);

            //if entry exists, but there's no data associated
            if (IndexData.Empty.Equals(data))
                throw new KeyNotFoundException(string.Format(Messages.KeyNotFound, key));

            byte[] buffer = new byte[data.size];
            lock (getLock) {
                storage.Seek(data.offset, SeekOrigin.Begin);
                storage.Read(buffer, 0, buffer.Length);
            }

            if (!data.isCompressed) return buffer;

            using (var outStream = new MemoryStream()) {
                using (var inStream = new MemoryStream(buffer)) {
                    using (var deflate = new DeflateStream(inStream, CompressionMode.Decompress)) {
                        deflate.CopyTo(outStream);
                        return outStream.ToArray();
                    }
                }
            }
        }

        public bool Contains(string key) {
            return index.Contains(key);
        }

        public void Dispose() {
            rwLock.Dispose();
            storage.Dispose();
            index.Dispose();
        }

        private static void CheckHash(Stream data, StreamInfo parameters) {
            using (var md5 = MD5.Create()) {
                byte[] hash = md5.ComputeHash(data);
                if (parameters.Hash != null && !hash.SequenceEqual(parameters.Hash))
                    throw new ArgumentException(
                        string.Format(Messages.HashInconsistent, BitConverter.ToString(parameters.Hash), BitConverter.ToString(hash)));
            }
        }

        private IndexData Store(Stream data, bool isCompressed) {
            //lock writes to file to get correct offset in file
            rwLock.EnterWriteLock();

            long offset = storage.Length;
            try {
                data.CopyTo(storage);
            } finally {
                rwLock.ExitWriteLock();
            }

            return new IndexData { offset = offset, size = data.Length, isCompressed = isCompressed };
        }
    }
}
