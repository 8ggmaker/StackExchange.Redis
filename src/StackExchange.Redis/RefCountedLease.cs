using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;

namespace StackExchange.Redis
{
    /// <summary>
    /// A reference-counted memory owner backed by <see cref="ArrayPool{T}"/>.
    /// When multiple handlers share the same underlying pooled buffer, each handler receives the same
    /// <see cref="RefCountedLease{T}"/> instance. Calling <see cref="Dispose"/> decrements the reference count;
    /// the pooled array is returned only when the last reference is released.
    /// </summary>
    /// <typeparam name="T">The type of data being leased.</typeparam>
    public sealed class RefCountedLease<T> : IMemoryOwner<T>
    {
        /// <summary>
        /// A lease of length zero.
        /// </summary>
        public static RefCountedLease<T> Empty { get; } = new RefCountedLease<T>(Array.Empty<T>(), 0, 1);

        private T[]? _arr;
        private int _refCount;

        /// <summary>
        /// The length of the leased data.
        /// </summary>
        public int Length { get; }

        internal RefCountedLease(T[] arr, int length, int refCount)
        {
            if (refCount <= 0) throw new ArgumentOutOfRangeException(nameof(refCount));
            _arr = arr;
            Length = length;
            _refCount = refCount;
        }

        /// <summary>
        /// Create a new <see cref="RefCountedLease{T}"/> backed by a pooled array.
        /// </summary>
        /// <param name="length">The size required.</param>
        /// <param name="refCount">The initial reference count.</param>
        /// <param name="clear">Whether to erase the memory.</param>
        internal static RefCountedLease<T> Create(int length, int refCount, bool clear = false)
        {
            if (length == 0) return Empty;
            var arr = ArrayPool<T>.Shared.Rent(length);
            if (clear) Array.Clear(arr, 0, length);
            return new RefCountedLease<T>(arr, length, refCount);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static T[] ThrowDisposed() => throw new ObjectDisposedException(nameof(RefCountedLease<T>));

        private T[] ArrayBuffer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _arr ?? ThrowDisposed();
        }

        /// <summary>
        /// The data as a <see cref="Memory{T}"/>.
        /// </summary>
        public Memory<T> Memory => new Memory<T>(ArrayBuffer, 0, Length);

        /// <summary>
        /// The data as a <see cref="Span{T}"/>.
        /// </summary>
        public Span<T> Span => new Span<T>(ArrayBuffer, 0, Length);

        /// <summary>
        /// The data as an <see cref="ArraySegment{T}"/>.
        /// </summary>
        public ArraySegment<T> ArraySegment => new ArraySegment<T>(ArrayBuffer, 0, Length);

        /// <summary>
        /// Decrements the reference count. When the count reaches zero, the
        /// pooled array is returned to <see cref="ArrayPool{T}.Shared"/>.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
            {
                if (Length != 0)
                {
                    var arr = Interlocked.Exchange(ref _arr, null);
                    if (arr != null) ArrayPool<T>.Shared.Return(arr);
                }
            }
        }
    }
}
