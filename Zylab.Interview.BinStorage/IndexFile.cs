using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Zylab.Interview.BinStorage {
    public sealed unsafe class IndexFile : IIndexFile {
        //default address to be more or less sure that address space will be continuous
        private const long ADDRESS = 0x400000000;

        private const int INITIAL_ENTRY_COUNT = 2048;
        private const int HASH_SIZE = 16; // MD5 size
        private const int HASH_SIZE_QW = HASH_SIZE / sizeof(long); // MD5 size in quad words

        // 16 bytes for MD5 + 8 bytes for offset + 8 bytes for size (including 1 bit to show data is compressed)
        private const int ENTRY_SIZE = HASH_SIZE + sizeof(long) + sizeof(long);
        private const int ENTRY_SIZE_QW = ENTRY_SIZE / sizeof(long);
        private const int DATA_SIZE_QW = ENTRY_SIZE_QW - HASH_SIZE_QW;

        private const int ENTRIES_IN_BLOCK = 512;
        private const int BLOCK_SIZE = ENTRY_SIZE_QW * ENTRIES_IN_BLOCK;
        private const int INITIAL_SIZE = INITIAL_ENTRY_COUNT * ENTRY_SIZE;
        private const float DEFAULT_LOAD_FACTOR = 0.8f;

        private struct MemoryMappedArea {
            public MemoryMappedFile File;
            public long* Address;
            public long Length;
        }

        [Flags]
        public enum FileMapAccess : uint {
            Copy = 0x01,
            Write = 0x02,
            Read = 0x04,
            AllAccess = 0x08,
            Execute = 0x20,
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern long* MapViewOfFileEx(IntPtr mappingHandle, FileMapAccess access, uint offsetHigh,
            uint offsetLow, UIntPtr bytesToMap, byte* desiredAddress);

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool UnmapViewOfFile(byte* address);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool FlushViewOfFile(byte* address, IntPtr bytesToFlush);


        private readonly FileStream fileStream;
        private readonly List<MemoryMappedArea> areas;
        private readonly List<long> offsets;
        private long*[] addresses;
        private readonly float loadFactor;

        private static readonly int maxSupportedThreads;
        private readonly long*[] oldBlockOffsets;
        private readonly long*[] newBlockOffsets;

        private readonly object bLock = new object();
        //private readonly object maskLock = new object();

        private long indexMask;
        private int blocksCount;

        static IndexFile() {
            // This looks hacky, but have a benefit
            // Mmanaged thread id is fixed in time
            // Each working thread can store block offset for entry
            // And in case of rehashing we can get address of new block
            // This works faster than having List, LinkedList or Dictionary
            int maxThreads, ioThreads;
            ThreadPool.GetMaxThreads(out maxThreads, out ioThreads);
            maxSupportedThreads = maxThreads;
        }

        public IndexFile(string filePath) : this(filePath, DEFAULT_LOAD_FACTOR) { }

        public IndexFile(string filePath, float loadFactor) {
            if (loadFactor < 0 || loadFactor > 1)
                throw new ArgumentException(string.Format(Messages.LoadFactorOutOfRange, loadFactor));

            this.loadFactor = loadFactor;
            oldBlockOffsets = new long*[maxSupportedThreads];
            newBlockOffsets = new long*[maxSupportedThreads];

            fileStream = new FileStream(Utils.CheckNotNull(filePath, Messages.FilePathNull), FileMode.OpenOrCreate,
                FileAccess.ReadWrite, FileShare.None);
            long fileLength = fileStream.Length;

            if (fileLength == 0) {
                fileStream.SetLength(fileLength = INITIAL_SIZE);
            }
            else if ((fileLength & INITIAL_SIZE - 1) != 0) {
                fileStream.Close();
                throw new InvalidDataException(string.Format(Messages.InvalidEntry, fileLength));
            }

            var map = MemoryMappedFile.CreateFromFile(fileStream, null, fileLength, MemoryMappedFileAccess.ReadWrite,
                null, HandleInheritability.None, true);
            long* address = MapViewOfFileEx(map.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                FileMapAccess.Read | FileMapAccess.Write, 0, 0, new UIntPtr((ulong) fileLength), (byte*) ADDRESS);
            if (address == null)
                address = MapViewOfFileEx(map.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                    FileMapAccess.Read | FileMapAccess.Write, 0, 0, new UIntPtr((ulong) fileLength), null);
            if (address == null) throw new Win32Exception();

            long length = fileLength / sizeof(long);

            areas = new List<MemoryMappedArea> {
                new MemoryMappedArea {
                    Address = address,
                    File = map,
                    Length = length
                }
            };

            addresses = new[] {address};
            offsets = new List<long> {0};

            indexMask = GetMask(length);
            blocksCount = (int) (length / BLOCK_SIZE);

            //verifying that index is consitent;
            Verify();
        }

        public int EntriesCount { get; private set; }

        public long Add(string key) {
            long hash1, hash2;
            fixed (byte* hashPtr = GetKeyHash(key)) {
                hash1 = *(long*) hashPtr;
                hash2 = *((long*) hashPtr + 1);
            }

            lock (bLock) {
                long blockOffset = hash1 & indexMask;

                for (long* ptr = GetPointer(blockOffset);; ptr += DATA_SIZE_QW) {
                    long* p = ptr;
                    // if entry is zero
                    if (*ptr == 0 && *(ptr + 1) == 0) {
                        *ptr++ = hash1;
                        *ptr = hash2;
                        return (long) (++EntriesCount >= blocksCount * ENTRIES_IN_BLOCK * loadFactor
                            ? Grow(p)
                            : AddPtr(p));
                    }
                    if (*ptr++ == hash1 & *ptr++ == hash2)
                        throw new ArgumentException(string.Format(Messages.KeyExists, key));
                }
            }
        }

        public void Add(long id, IndexData data) {
            CheckDisposed();
            long size = data.size;
            if (data.isCompressed)
                //set highest bit to indicate that data is compressed
                size |= long.MinValue;

            long* ptr = (long*) id;
            int threadId = Thread.CurrentThread.ManagedThreadId;

            lock (bLock) {
                //check whether rehash has happened and there's new block offset for entry
                if (newBlockOffsets[threadId] != null && oldBlockOffsets[threadId] == ptr) {
                    //use and reset new block offset
                    ptr = newBlockOffsets[threadId];
                    newBlockOffsets[threadId] = null;
                }
            }

            //reset old block offset
            oldBlockOffsets[threadId] = null;

            ptr += HASH_SIZE_QW;

            *ptr++ = size;
            *ptr = data.offset;
        }

        public IndexData Get(string key) {
            return Find(key,
                (size, offset) => new IndexData {
                    offset = offset,
                    size = size & long.MaxValue,
                    isCompressed = Convert.ToBoolean(size & long.MinValue)
                },
                () => { throw new KeyNotFoundException(string.Format(Messages.KeyNotFound, key)); });
        }

        public bool Contains(string key) {
            return Find(key,
                (size, offset) => true,
                () => false);
        }

        private T Find<T>(string key, Func<long, long, T> foundFunc, Func<T> notFoundFunc) {
            byte[] hash = GetKeyHash(key);

            long blockOffset = BitConverter.ToInt64(hash, 0) & indexMask, hash1, hash2;
            //Iterate until zero entry will be found
            for (long* ptr = GetPointer(blockOffset);
                    (hash1 = *ptr++) != 0 | (hash2 = *ptr++) != 0;
                    ptr += DATA_SIZE_QW)
                //Invoke found if hashes are equal
                if (BitConverter.ToInt64(hash, 0) == hash1 && BitConverter.ToInt64(hash, sizeof(long)) == hash2)
                    //Parameters are size and offset
                    return foundFunc.Invoke(*ptr++, *ptr);

            return notFoundFunc.Invoke();
        }

        private static long GetMask(long length) {
            return (length / BLOCK_SIZE - 1) * BLOCK_SIZE;
        }

        private long* AddPtr(long* ptr) {
            int i = Thread.CurrentThread.ManagedThreadId;
            oldBlockOffsets[i] = ptr;
            return ptr;
        }

        private static byte[] GetKeyHash(string key) {
            using (var md5 = MD5.Create()) {
                return md5.ComputeHash(Encoding.UTF8.GetBytes(key));
            }
        }

        private long* Grow(long* ptr) {
            CheckDisposed();

            long offset = fileStream.Length;
            long qwOffset = offset / sizeof(long);
            long length = offset * 2;
            fileStream.SetLength(length);

            AddMapping(offset, qwOffset, length);

            long qwLength = length / sizeof(long);

            //lock when updating block mask
            //lock (maskLock) {
            indexMask = GetMask(qwLength);
            //}

            byte[] buffer = new byte[length];
            long* newPtr = ptr;

            fixed (byte* bufferPtr = buffer) {
                long* buffPtr = (long*) bufferPtr;

                //iterate over each block
                for (int block = 0; block < blocksCount; block++) {
                    long* sPtr = GetPointer(block * BLOCK_SIZE);

                    //iterate over each entry in block
                    for (int i = 0; i < ENTRIES_IN_BLOCK; i++) {
                        long* s = sPtr;

                        //if we've reached zero entry skip to next block
                        if (*s++ == 0 && *s++ == 0 && *s++ == 0 && *s == 0)
                            break;

                        long blockOffset = *sPtr & indexMask;
                        for (int j = 0; j < BLOCK_SIZE; j += ENTRY_SIZE_QW) {
                            long* tPtr = buffPtr + blockOffset + j;
                            long* t = tPtr;

                            //skip until we've found zero entry
                            if (*t++ != 0 || *t++ != 0 || *t++ != 0 || *t != 0) continue;

                            //if entry at sPtr is entry that have raised rehashing
                            if (ptr == sPtr)
                                //then save it's new position
                                newPtr = GetPointer(tPtr - buffPtr);

                            //if entry at sPtr is being processed on any other thread
                            for (int k = 0; k < newBlockOffsets.Length; k++)
                                if (oldBlockOffsets[k] == sPtr) {
                                    //then save it's new position
                                    newBlockOffsets[k] = GetPointer(tPtr - buffPtr);
                                    break;
                                }

                            *tPtr++ = *sPtr++;
                            *tPtr++ = *sPtr++;
                            *tPtr++ = *sPtr++;
                            *tPtr = *sPtr++;
                            break;
                        }
                    }
                }
            }

            blocksCount *= 2;

            CopyBuffer(buffer);

            return newPtr;
        }

        private void CopyBuffer(byte[] buffer) {
            fixed (byte* bufferPtr = buffer) {
                long* sPtr = (long*) bufferPtr;
                foreach (var a in areas) {
                    for (long* dPtr = a.Address; dPtr - a.Address < a.Length; *dPtr++ = *sPtr++) ;
                }
            }
        }

        private void Verify() {
            for (int block = 0; block < blocksCount; block++) {
                long blockOffset = block * BLOCK_SIZE, value;
                //Iterate until zero entry is found
                for (long* ptr = GetPointer(blockOffset);
                    (value = *ptr++) != 0 | *ptr++ != 0;
                    ptr += DATA_SIZE_QW, EntriesCount++) {
                    long offset = value & indexMask;
                    //If offset is not equal to expected than entry is not in it's block and index is corrupted
                    if (offset != blockOffset) {
                        Dispose();
                        throw new InvalidDataException(string.Format(Messages.InvalidEntry, (long) ptr));
                    }
                }
            }
        }

        private void AddMapping(long offset, long qwOffset, long length) {
            var mapping = MemoryMappedFile.CreateFromFile(fileStream, null, length, MemoryMappedFileAccess.ReadWrite,
                null, HandleInheritability.None, true);

            var lastArea = areas[areas.Count - 1];
            long* desiredAddress = lastArea.Address + lastArea.Length;
            long* address = MapViewOfFileEx(mapping.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                FileMapAccess.Read | FileMapAccess.Write, (uint) (offset >> 32), (uint) offset,
                new UIntPtr((ulong) offset), (byte*) desiredAddress);
            if (address == null) {
                //if we cannot extend region to map file, let OS choose address to map
                address = MapViewOfFileEx(mapping.SafeMemoryMappedFileHandle.DangerousGetHandle(),
                    FileMapAccess.Read | FileMapAccess.Write, (uint) (offset >> 32), (uint) offset,
                    new UIntPtr((ulong) offset), null);
            }
            if (address == null) throw new Win32Exception();
            var area = new MemoryMappedArea {
                Address = address,
                File = mapping,
                Length = qwOffset
            };
            areas.Add(area);
            if (desiredAddress != address) {
                offsets.Add(qwOffset);
                addresses = Add(addresses, address);
            }
        }

        //tricky way to add element to array of pointers as pointers cannot be used in generics
        private static long*[] Add(long*[] array, long* element) {
            var result = new long*[array.Length + 1];
            Array.Copy(array, result, array.Length);
            result[array.Length] = element;
            return result;
        }

        private long* GetPointer(long offset) {
            CheckDisposed();
            lock (bLock) {
                int i = offsets.Count;
                if (i <= 128) {
                    // linear search is more efficient for small arrays
                    while (--i > 0 && offsets[i] > offset) ;
                }
                else {
                    // binary search is more efficient for large arrays
                    i = offsets.BinarySearch(offset);
                    if (i < 0) i = ~i - 1;
                }
                return addresses[i] + offset - offsets[i];
            }
        }

        private bool isDisposed;

        public void Dispose() {
            if (isDisposed) return;
            isDisposed = true;
            foreach (var a in areas) {
                UnmapViewOfFile((byte*) a.Address);
                a.File.Dispose();
            }
            fileStream.Dispose();
            areas.Clear();
        }

        private void CheckDisposed() {
            if (isDisposed) throw new ObjectDisposedException(GetType().Name);
        }

        public void Flush() {
            CheckDisposed();
            if (areas.Any(area => !FlushViewOfFile((byte*) area.Address, new IntPtr(area.Length * sizeof(long))))) {
                throw new Win32Exception();
            }
            fileStream.Flush(true);
        }
    }
}