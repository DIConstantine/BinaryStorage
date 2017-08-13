using System;

namespace Zylab.Interview.BinStorage {
    public interface IIndexFile : IDisposable {

        /// <summary>
        /// Add entry assosiated with the key
        /// </summary>
        /// <param name="key">Unique identifier assiciated with index entry</param>
        /// <returns>Id associated with the key</returns>
        /// <exception cref="ArgumentException">
        /// An element with the same key already exists
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        ///  Object is disposed
        /// </exception>
        long Add(string key);

        /// <summary>
        /// Add data to entry with particular id
        /// </summary>
        /// <param name="id">Id of index entry</param>
        /// <param name="data">Data associated with index entry</param>
        /// <exception cref="ObjectDisposedException">
        ///  Object is disposed
        /// </exception>
        void Add(long id, IndexData data);

        /// <summary>
        /// Get data associated with the key
        /// </summary>
        /// <param name="key">Unique identifier assiciated with index entry</param>
        /// <returns>Data stored in index entry</returns>
        /// <exception cref="System.Collections.Generic.KeyNotFoundException">
        /// Key does not exist
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        ///  Object is disposed
        /// </exception>
        IndexData Get(string key);

        /// <summary>
        /// Check whether exists entry associated with the key
        /// </summary>
        /// <param name="key">Unique identifier assiciated with index entry</param>
        /// <returns>True if exists, False otherwise</returns>
        /// <exception cref="ObjectDisposedException">
        ///  Object is disposed
        /// </exception>
        bool Contains(string key);

        /// <summary>
        /// Flushes data to disk
        /// </summary>
        /// <exception cref="ObjectDisposedException">
        ///  Object is disposed
        /// </exception>
        void Flush();

        /// <summary>
        /// Gets count of entries in index
        /// </summary>
        int EntriesCount { get; }
    }
}
