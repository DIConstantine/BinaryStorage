using System;
using System.Runtime.InteropServices;

namespace Zylab.Interview.BinStorage {
    public static class Utils {
        [StructLayout(LayoutKind.Sequential)]
        private class MEMORYSTATUSEX {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;

            public MEMORYSTATUSEX() {
                dwLength = (uint) Marshal.SizeOf(typeof(MEMORYSTATUSEX));
            }
        }

        private const float LOAD_FACTOR = 0.8f;

        [DllImport("kernel32.dll")]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        public static Func<long> AVAILABLE_MEMORY_PROVIDER = () => {
            var memStatus = new MEMORYSTATUSEX();
            return GlobalMemoryStatusEx(memStatus) ? (long) (memStatus.ullAvailPhys * LOAD_FACTOR) : 0;
        };

        public static T CheckNotNull<T>(T obj, string message) {
            if (obj == null)
                throw new ArgumentNullException(message);
            return obj;
        }
    }
}