using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Zylab.Interview.BinStorage.UnitTests {
    [TestClass]
    public class CacheTest {

        [TestMethod]
        public void ElementShouldBeAddedIfThereIsEnoughMemory() {
            var cache = new Cache(() => 10);
            byte[] data = {1, 2, 42};

            CollectionAssert.AreEqual(data, cache.GetOrAdd("key", key => data));
        }

        [TestMethod]
        public void ZeroMemoryShouldNotStopCacheFromWorking() {
            var cache = new Cache(() => 0);
            byte[] data = {1, 2, 42};

            Assert.AreEqual(0, cache.Count);
            CollectionAssert.AreEqual(data, cache.GetOrAdd("key", key => data));
        }

        [TestMethod]
        public void ElementsWillBeRemovedIfThereIsNotEnoughMemory() {
            var cache = new Cache(() => 10);
            byte[] data1 = {1, 2, 42};
            byte[] data2 = {1, 2, 3, 4, 5, 6, 7};
            byte[] big_data = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

            cache.GetOrAdd("key1", key => data1);
            cache.GetOrAdd("key2", key => data2);

            Assert.AreEqual(2, cache.Count);

            cache.GetOrAdd("big", key => big_data);

            Assert.AreEqual(1, cache.Count);
        }

        [TestMethod]
        public void ElementsAreNotQueuedIfTheyAreAlreadyAdded() {
            var cache = new Cache(() => 10);
            byte[] data = {1, 2, 42};
            byte[] big_data = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

            const string KEY1 = "key";
            const string KEY2 = "big";

            cache.GetOrAdd(KEY1, key => data);
            cache.GetOrAdd(KEY1, key => data);
            cache.GetOrAdd(KEY2, key => big_data);

            Assert.AreEqual(1, cache.Count);
            Assert.IsFalse(cache.ContainsKey(KEY1));
            byte[] result;
            Assert.IsTrue(cache.TryGetValue(KEY2, out result));
            CollectionAssert.AreEqual(big_data, result);
        }

        [TestMethod]
        public void ElementThatWasAddedFirstIsDroppedFirst() {
            var cache = new Cache(() => 10);
            byte[] data1 = {1, 2, 42};
            byte[] data2 = {1, 2, 3};
            byte[] data3 = {1, 2, 3, 4, 5, 6, 7};

            const string KEY1 = "key1";
            const string KEY2 = "key2";
            const string KEY3 = "key3";

            cache.GetOrAdd(KEY1, key => data1);
            cache.GetOrAdd(KEY2, key => data2);
            cache.GetOrAdd(KEY3, key => data3);

            Assert.AreEqual(2, cache.Count);
            Assert.IsFalse(cache.ContainsKey(KEY1));
            byte[] data;
            Assert.IsTrue(cache.TryGetValue(KEY2, out data));
            CollectionAssert.AreEqual(data2, data);
            Assert.IsTrue(cache.TryGetValue(KEY3, out data));
            CollectionAssert.AreEqual(data3, data);
        }

        [TestMethod]
        public void ElementShouldBeReturnedEvenIfThereIsNotEnoughMemory() {
            var cache = new Cache(() => 3);
            byte[] big_data = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

            Assert.AreEqual(big_data, cache.GetOrAdd("key", key => big_data));
            Assert.AreEqual(0, cache.Count);
        }

        [TestMethod]
        public void WhenAddingNewElementMoreOldElementsShouldBeRemovedIfAvailableMemoryDecreases() {
            int i = 0;
            int[] memory = {10, 3};
            var cache = new Cache(() => memory[i]);
            byte[] data1 = {1, 2, 42};
            byte[] data2 = {1, 2, 3};
            byte[] data3 = {1, 2, 3, 4, 5, 6, 7};

            const string KEY1 = "key1";
            const string KEY2 = "key2";
            const string KEY3 = "key3";

            cache.GetOrAdd(KEY1, key => data1);
            cache.GetOrAdd(KEY2, key => data2);
            cache.GetOrAdd(KEY3, key => data3);

            Assert.AreEqual(2, cache.Count);

            i++;

            cache.GetOrAdd(KEY1, key => data1);
            Assert.AreEqual(1, cache.Count);
            byte[] data;
            Assert.IsTrue(cache.TryGetValue(KEY1, out data));
            CollectionAssert.AreEqual(data1, data);
        }

        [TestMethod]
        public void BiggerNewElementCanBeAddedIfAvailableMemoryIncreases() {
            int i = 0;
            int[] memory = {10, 20};
            var cache = new Cache(() => memory[i]);

            byte[] data1 = {1, 2, 42};
            byte[] data2 = {1, 2, 3};
            byte[] data3 = {1, 2, 3, 4, 5, 6, 7};

            const string KEY1 = "key1";
            const string KEY2 = "key2";
            const string KEY3 = "key3";

            cache.GetOrAdd(KEY1, key => data1);
            cache.GetOrAdd(KEY2, key => data2);
            cache.GetOrAdd(KEY3, key => data3);

            Assert.AreEqual(2, cache.Count);

            i++;

            cache.GetOrAdd(KEY1, key => data1);
            Assert.AreEqual(3, cache.Count);
        }

        [TestMethod]
        public void OldestElementShouldBeDroppedIfThereIsEnoughMemoryButWeHitCapacity() {
            var cache = new Cache(() => 10, 1);
            byte[] data1 = { 1, 2, 42 };
            byte[] data2 = { 1, 2, 3 };

            const string KEY1 = "key1";
            const string KEY2 = "key2";

            cache.GetOrAdd(KEY1, key => data1);
            cache.GetOrAdd(KEY2, key => data2);

            Assert.AreEqual(1, cache.Count);
            Assert.IsFalse(cache.ContainsKey(KEY1));
            byte[] data;
            Assert.IsTrue(cache.TryGetValue(KEY2, out data));
            CollectionAssert.AreEqual(data2, data);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void NullProviderThrowsException() {
            new Cache(null);
        }
    }
}