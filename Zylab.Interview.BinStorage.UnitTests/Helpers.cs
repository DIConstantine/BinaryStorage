using System.IO;
using System.Text;

namespace Zylab.Interview.BinStorage.UnitTests {
    public static class Helpers {
        public static Stream ToStream(this string str) {
            var stream = new MemoryStream();
            byte[] data = Encoding.UTF8.GetBytes(str);
            stream.Write(data, 0, data.Length);
            stream.Position = 0;
            return stream;
        }
    }
}
