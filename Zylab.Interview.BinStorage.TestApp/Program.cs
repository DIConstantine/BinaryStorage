using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Zylab.Interview.BinStorage.TestApp
{
	class Program
	{
		static void Main(string[] args)
		{
			if (args.Length < 2 
				|| !Directory.Exists(args[0])
				|| !Directory.Exists(args[1]))
			{
				Console.WriteLine("Usage: Zylab.Interview.BinStorage.TestApp.exe InputFolder StorageFolder");
				return;
			}

			// Create storage and add data
			Console.WriteLine("Creating storage from " + args[0]);
			Stopwatch sw = Stopwatch.StartNew();
			using (var storage = new BinaryStorage(new StorageConfiguration() { WorkingFolder = args[1] }))
			{
				Directory.EnumerateFiles(args[0], "*", SearchOption.AllDirectories)
					.AsParallel().WithDegreeOfParallelism(4).ForAll(s =>
					{
						AddFile(storage, s);
					});

			}
			Console.WriteLine("Time to create: " + sw.Elapsed);

			// Open storage and read data
			Console.WriteLine("Verifying data");
			sw = Stopwatch.StartNew();
			using (var storage = new BinaryStorage(new StorageConfiguration() { WorkingFolder = args[1] }))
			{
				Directory.EnumerateFiles(args[0], "*", SearchOption.AllDirectories)
					.AsParallel().WithDegreeOfParallelism(4).ForAll(s =>
					{
						using (var resultStream = storage.Get(s)) {
							using (var sourceStream = new FileStream(s, FileMode.Open, FileAccess.Read)) {
								if (sourceStream.Length != resultStream.Length) {
									throw new Exception(string.Format("Length did not match: Source - '{0}', Result - {1}", sourceStream.Length, resultStream.Length));
								}

								byte[] hash1, hash2;
								using (MD5 md5 = MD5.Create()) {
									hash1 = md5.ComputeHash(sourceStream);

									md5.Initialize();
									hash2 = md5.ComputeHash(resultStream);
								}

								if (!hash1.SequenceEqual(hash2)) {
									throw new Exception(string.Format("Hashes do not match for file - '{0}'  ", s));
								}
							}
						}
					});
			}
			Console.WriteLine("Time to verify: " + sw.Elapsed);
		}

		static void AddFile(IBinaryStorage storage, string fileName)
		{
			using (var file = new FileStream(fileName, FileMode.Open))
			{
				storage.Add(fileName, file, StreamInfo.Empty);
			}
		}

		static void AddBytes(IBinaryStorage storage, string key, byte[] data)
		{
			StreamInfo streamInfo = new StreamInfo();
			using (MD5 md5 = MD5.Create())
			{
				streamInfo.Hash = md5.ComputeHash(data);
			}
			streamInfo.Length = data.Length;
			streamInfo.IsCompressed = false;

			using (var ms = new MemoryStream(data))
			{
				storage.Add(key, ms, streamInfo);
			}
		}

		static void Dump(IBinaryStorage storage, string key, string fileName)
		{
			using (var file = new FileStream(fileName, FileMode.Create))
			{
				storage.Get(key).CopyTo(file);
			}
		}

	}
}
