using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Collections.Generic;

namespace Zylab.Interview.BinStorage.UnitTests {
    [TestClass]
    public class IndexFileTest {
        private const string INDEX_FILE = "indexFile";

        [TestCleanup]
        public void Cleanup() {
            if (File.Exists(INDEX_FILE))
                File.Delete(INDEX_FILE);
        }

        private static void WithIndex(Action<IIndexFile> code) {
            using (var index = new IndexFile(INDEX_FILE))
                code.Invoke(index);
        }

        [TestMethod]
        public void AddedDataShouldBePossibleToGet() {
            WithIndex(index => {
                const string KEY = "some key";
                long id = index.Add(KEY);
                var data = new IndexData {size = 100, offset = 200, isCompressed = true};
                index.Add(id, data);

                Assert.AreEqual(1, index.EntriesCount);
                Assert.IsTrue(index.Contains(KEY));
                Assert.AreEqual(data, index.Get(KEY));
            });
        }

        [TestMethod]
        public void IfDataNotAddedEntryStilShouldPersist() {
            WithIndex(index => {
                const string KEY = "key";
                index.Add(KEY);

                Assert.AreEqual(1, index.EntriesCount);
                Assert.IsTrue(index.Contains(KEY));
                Assert.AreEqual(IndexData.Empty, index.Get(KEY));
            });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void DuplicateKeyThrowsException() {
            WithIndex(index => {
                const string KEY = "key";
                index.Add(KEY);
                index.Add(KEY);
            });
        }

        [TestMethod]
        public void NotAddedKeyIsNotContained() {
            WithIndex(index => Assert.IsFalse(index.Contains("absent key")));
        }

        [TestMethod]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void GettingAbsentKeyThrowsException() {
            WithIndex(index => index.Get("absent key"));
        }

        [TestMethod]
        public void KeyShouldBePersistedAfterIndexClosed() {
            const string KEY = "persisted key";

            WithIndex(index => index.Add(KEY));

            WithIndex(index => Assert.IsTrue(index.Contains(KEY)));
        }

        [TestMethod]
        public void KeyShouldBeAvailableAfterIndexGrowAndRehashing() {
            const string KEY = "key";

            using (var index = new IndexFile(INDEX_FILE, 0)) {
                index.Add(KEY);
                Assert.AreEqual(1, index.EntriesCount);
                Assert.IsTrue(index.Contains(KEY));
                Assert.AreEqual(IndexData.Empty, index.Get(KEY));
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidDataException))]
        public void IfIndexIsWrongSizeItShouldThrowExceptionOnCreation() {
            File.WriteAllText(INDEX_FILE, @"currupted file");
            WithIndex(index => { });
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidDataException))]
        public void IfIndexIsCorruptedItShouldThrowExceptionOnCreation() {
            byte[] buffer = {
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
            };
            using (var stream = new FileStream(INDEX_FILE, FileMode.Append))
                for (int i = 0; i < 2048; i++)
                    stream.Write(buffer, 0, buffer.Length);

            WithIndex(index => { });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void NullFilePathThrowsException() {
            new IndexFile(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void LoadFactorLessThanZeroThrowsException() {
            new IndexFile(INDEX_FILE, -1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void LoadFactorMoreThanOneThrowsException() {
            new IndexFile(INDEX_FILE, 2);
        }

        [TestMethod]
        [ExpectedException(typeof(IOException))]
        public void ItIsNotPossibelTwoCreateToIndexesOnSameFile() {
            WithIndex(index => WithIndex(index2 => { }));
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void AddingNewEntryShouldThrowAnExceptionIfIndexIsDisposed() {
            WithIndex(index => {
                index.Dispose();
                index.Add("any key");
            });
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void AddingDataByIdShouldThrowAnExceptionIfIndexIsDisposed() {
            WithIndex(index => {
                index.Dispose();
                index.Add(0, IndexData.Empty);
            });
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CheckingForExistanceShouldThrowAnExceptionIfIndexIsDisposed() {
            WithIndex(index => {
                index.Dispose();
                index.Contains("any key");
            });
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void FlushingOfDisposedObjectShouldThrowAnExceptionIfIndexIsDisposed() {
            WithIndex(index => {
                index.Dispose();
                index.Flush();
            });
        }

        [TestMethod]
        public void DisposingDisposedObjectShouldNotDoAnything() {
            WithIndex(index => {
                index.Dispose();
                index.Dispose();
            });
        }
    }
}