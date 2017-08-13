using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Zylab.Interview.BinStorage {
    public sealed class Cache : ConcurrentDictionary<string, byte[]> {
        private const int CONCURRENCY_LEVEL = 4;
        private const int DEFAULT_CACHE_CAPACITY = 128;

        private readonly Func<long> availableMemoryProvider;
        private readonly ConcurrentQueue<string> queue;
        private readonly int capacity;
        private readonly object syncLock = new object();

        public Cache(Func<long> availableMemoryProvider) : this(availableMemoryProvider, DEFAULT_CACHE_CAPACITY) { }

        public Cache(Func<long> availableMemoryProvider, int capacity) : base(CONCURRENCY_LEVEL, capacity) {
            this.availableMemoryProvider =
                Utils.CheckNotNull(availableMemoryProvider, Messages.AvailableMemoryProviderNull);
            this.capacity = capacity;
            queue = new ConcurrentQueue<string>();
        }

        public new byte[] GetOrAdd(string key, Func<string, byte[]> valueFactory) {
            byte[] result;

            //Return value if it's present
            if (TryGetValue(key, out result))
                return result;

            result = valueFactory(key);

            //Probably this lock is not needed
            lock (syncLock) {
                string s;
                byte[] v;

                //Cleanup if there's not enough memory or we hit the capacity
                while ((queue.Count >= capacity ||
                        TotalSize + result.LongLength > availableMemoryProvider.Invoke()) &&
                       queue.TryDequeue(out s))
                    TryRemove(s, out v);

                //Add value only if there's enough space and it's not already added
                if (TotalSize + result.LongLength <= availableMemoryProvider.Invoke() && TryAdd(key, result))
                    queue.Enqueue(key);
            }

            return result;
        }

        private long TotalSize {
            get { return Values.Sum(data => data.LongLength); }
        }
    }
}