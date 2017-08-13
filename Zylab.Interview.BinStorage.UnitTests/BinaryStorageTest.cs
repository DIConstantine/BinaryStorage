using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Text;

namespace Zylab.Interview.BinStorage.UnitTests {

    [TestClass]
    public class BinaryStorageTest {

        private const string DIRECTORY = "tempDir";

        [TestInitialize]
        public void Setup() {
            if (!Directory.Exists(DIRECTORY))
                Directory.CreateDirectory(DIRECTORY);
        }

        [TestCleanup]
        public void Cleanup() {
            if (Directory.Exists(DIRECTORY))
                Directory.Delete(DIRECTORY, true);

        }

        private static void WithStorage(Action<IBinaryStorage> code) {
            using (var storage = new BinaryStorage(new StorageConfiguration { WorkingFolder = DIRECTORY }))
                code.Invoke(storage);
        }

        private static void WithStream(string str, Func<Stream, Action<IBinaryStorage>> code) {
            using (var stream = str.ToStream())
                WithStorage(code.Invoke(stream));
        }

        [TestMethod]
        public void AddedStreamShouldBeSameAfterGettingBack() {
            const string KEY = "key";
            const string DATA = "Hello, world!";

            WithStream(DATA, stream =>
                storage => {
                    storage.Add(KEY, stream, new StreamInfo());

                    Assert.IsTrue(storage.Contains(KEY));

                    using (var memory = new MemoryStream()) {
                        storage.Get(KEY).CopyTo(memory);
                        string result = Encoding.UTF8.GetString(memory.ToArray());
                        Assert.AreEqual(DATA, result);
                    }
                });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void AddingSameKeyShouldThrowAnException() {
            const string KEY = "key";
            WithStream("any data", stream =>
                storage => {
                    storage.Add(KEY, stream, new StreamInfo());
                    storage.Add(KEY, stream, new StreamInfo());
                });
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExceptionShouldBeThrownWhenConfigurationIsNull() {
            new BinaryStorage(null);
        }

        [TestMethod]
        [ExpectedException(typeof(IOException))]
        public void CannotCreateTwoStoragesOnTheSameDirectory() {
            WithStorage(storage => WithStorage(storage2 => { }));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExceptionShouldBeThrownWhenKeyIsNull() {
            WithStorage(storage => storage.Add(null, Stream.Null, StreamInfo.Empty));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExceptionShouldBeThrownWhenKeyIsEmpty() {
            WithStorage(storage => storage.Add(string.Empty, Stream.Null, StreamInfo.Empty));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExceptionShouldBeThrownWhenStreamIsNull() {
            WithStorage(storage => storage.Add("key", null, StreamInfo.Empty));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExceptionShouldBeThrownWhenStreamIsEmpty() {
            WithStorage(storage => storage.Add("key", Stream.Null, StreamInfo.Empty));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ExceptionShouldBeThrownWhenParametersAreNull() {
            WithStream("data", stream =>
             storage => storage.Add("key", stream, null));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ExceptionShouldBeThrownWhenLengthIsInconsistent() {
            WithStream("data", stream =>
                storage => storage.Add("key", stream, new StreamInfo { Length = 0 }));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void ExceptionShouldBeThrownWhenHashIsInconsistent() {
            WithStream("data", stream =>
                storage => storage.Add("key", stream, new StreamInfo { Hash = new byte[0] }));
        }

        [TestMethod]
        [ExpectedException(typeof(IOException))]
        public void ItIsNotPossibelToCreateTwoStoragesOnSameFile() {
            WithStorage(storage => WithStorage(storage2 => { }));
        }
    }
}
