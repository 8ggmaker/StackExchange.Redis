using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
#if NETCOREAPP3_1
using System.Reflection;
#endif

namespace StackExchange.Redis
{
    /// <summary>
    /// Represents a lease-based message that is broadcast via publish/subscribe.
    /// The consumer is responsible for disposing the <see cref="Lease"/> when done.
    /// </summary>
    public readonly struct ChannelLeaseMessage : IDisposable
    {
        private readonly ChannelLeaseMessageQueue _queue;

        /// <summary>
        /// The Channel:Length string representation.
        /// </summary>
        public override string ToString() => ((string?)Channel) + ":" + Lease.Length + " bytes";

        /// <inheritdoc/>
        public override int GetHashCode() => Channel.GetHashCode();

        /// <inheritdoc/>
        public override bool Equals(object? obj) => obj is ChannelLeaseMessage cm && cm.Channel == Channel;

        internal ChannelLeaseMessage(ChannelLeaseMessageQueue queue, in RedisChannel channel, RefCountedLease<byte> lease)
        {
            _queue = queue;
            Channel = channel;
            Lease = lease;
        }

        /// <summary>
        /// The channel that the subscription was created from.
        /// </summary>
        public RedisChannel SubscriptionChannel => _queue.Channel;

        /// <summary>
        /// The channel that the message was broadcast to.
        /// </summary>
        public RedisChannel Channel { get; }

        /// <summary>
        /// The leased data that was broadcast. The consumer must call <see cref="RefCountedLease{T}.Dispose"/>
        /// when finished with the data to return the buffer to the pool.
        /// </summary>
        public RefCountedLease<byte> Lease { get; }

        /// <summary>
        /// Disposes the underlying lease, returning the buffer to the pool.
        /// </summary>
        public void Dispose() => Lease?.Dispose();

        /// <summary>
        /// Checks if 2 messages are .Equal().
        /// </summary>
        public static bool operator ==(ChannelLeaseMessage left, ChannelLeaseMessage right) => left.Equals(right);

        /// <summary>
        /// Checks if 2 messages are not .Equal().
        /// </summary>
        public static bool operator !=(ChannelLeaseMessage left, ChannelLeaseMessage right) => !left.Equals(right);
    }

    /// <summary>
    /// Represents a message queue of ordered pub/sub lease notifications.
    /// Each message contains a <see cref="RefCountedLease{T}"/> that must be disposed by the consumer.
    /// </summary>
    /// <remarks>
    /// To create a ChannelLeaseMessageQueue, use <see cref="ISubscriber.SubscribeLease(RedisChannel, CommandFlags)"/>
    /// or <see cref="ISubscriber.SubscribeLeaseAsync(RedisChannel, CommandFlags)"/>.
    /// </remarks>
    public sealed class ChannelLeaseMessageQueue : IAsyncEnumerable<ChannelLeaseMessage>
    {
        private readonly Channel<ChannelLeaseMessage> _queue;

        /// <summary>
        /// The Channel that was subscribed for this queue.
        /// </summary>
        public RedisChannel Channel { get; }
        internal RedisSubscriber? _parent;

        /// <summary>
        /// The string representation of this channel.
        /// </summary>
        public override string? ToString() => (string?)Channel;

        /// <summary>
        /// An awaitable task the indicates completion of the queue (including drain of data).
        /// </summary>
        public Task Completion => _queue.Reader.Completion;

        internal ChannelLeaseMessageQueue(in RedisChannel redisChannel, RedisSubscriber parent)
        {
            Channel = redisChannel;
            _parent = parent;
            _queue = System.Threading.Channels.Channel.CreateUnbounded<ChannelLeaseMessage>(s_ChannelOptions);
        }

        private static readonly UnboundedChannelOptions s_ChannelOptions = new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false,
        };

        internal void Write(in RedisChannel channel, RefCountedLease<byte> lease)
        {
            var writer = _queue.Writer;
            writer.TryWrite(new ChannelLeaseMessage(this, channel, lease));
        }

        /// <summary>
        /// Consume a message from the channel.
        /// </summary>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> to use.</param>
        public ValueTask<ChannelLeaseMessage> ReadAsync(CancellationToken cancellationToken = default)
            => _queue.Reader.ReadAsync(cancellationToken);

        /// <summary>
        /// Attempt to synchronously consume a message from the channel.
        /// </summary>
        /// <param name="item">The <see cref="ChannelLeaseMessage"/> read from the Channel.</param>
        public bool TryRead(out ChannelLeaseMessage item) => _queue.Reader.TryRead(out item);

        /// <summary>
        /// Attempt to query the backlog length of the queue.
        /// </summary>
        /// <param name="count">The (approximate) count of items in the Channel.</param>
        public bool TryGetCount(out int count)
        {
#if NETCOREAPP3_1
            try
            {
                var prop = _queue.GetType().GetProperty("ItemsCountForDebugger", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                if (prop is not null)
                {
                    count = (int)prop.GetValue(_queue)!;
                    return true;
                }
            }
            catch { }
#else
            var reader = _queue.Reader;
            if (reader.CanCount)
            {
                count = reader.Count;
                return true;
            }
#endif

            count = default;
            return false;
        }

        private Delegate? _onMessageHandler;
        private void AssertOnMessage(Delegate handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            if (Interlocked.CompareExchange(ref _onMessageHandler, handler, null) != null)
                throw new InvalidOperationException("Only a single " + nameof(OnMessage) + " is allowed");
        }

        /// <summary>
        /// Create a message loop that processes messages sequentially.
        /// </summary>
        /// <param name="handler">The handler to run when receiving a message.</param>
        public void OnMessage(Action<ChannelLeaseMessage> handler)
        {
            AssertOnMessage(handler);

            ThreadPool.QueueUserWorkItem(
                state => ((ChannelLeaseMessageQueue)state!).OnMessageSyncImpl().RedisFireAndForget(), this);
        }

        private async Task OnMessageSyncImpl()
        {
            var handler = (Action<ChannelLeaseMessage>?)_onMessageHandler;
            while (!Completion.IsCompleted)
            {
                ChannelLeaseMessage next;
                try { if (!TryRead(out next)) next = await ReadAsync().ForAwait(); }
                catch (ChannelClosedException) { break; }
                catch (Exception ex)
                {
                    _parent?.multiplexer?.OnInternalError(ex);
                    break;
                }

                try { handler?.Invoke(next); }
                catch { }
            }
        }

        internal ChannelLeaseMessageQueue? _next;

        internal static void Combine(ref ChannelLeaseMessageQueue? head, ChannelLeaseMessageQueue queue)
        {
            if (queue != null)
            {
                ChannelLeaseMessageQueue? old;
                do
                {
                    old = Volatile.Read(ref head);
                    queue._next = old;
                }
                while (Interlocked.CompareExchange(ref head, queue, old) != old);
            }
        }

        /// <summary>
        /// Create a message loop that processes messages sequentially.
        /// </summary>
        /// <param name="handler">The handler to execute when receiving a message.</param>
        public void OnMessage(Func<ChannelLeaseMessage, Task> handler)
        {
            AssertOnMessage(handler);

            ThreadPool.QueueUserWorkItem(
                state => ((ChannelLeaseMessageQueue)state!).OnMessageAsyncImpl().RedisFireAndForget(), this);
        }

        internal static void Remove(ref ChannelLeaseMessageQueue? head, ChannelLeaseMessageQueue queue)
        {
            if (queue is null)
            {
                return;
            }

            bool found;
            do
            {
                var current = Volatile.Read(ref head);
                if (current == null) return;
                if (current == queue)
                {
                    found = true;
                    if (Interlocked.CompareExchange(ref head, Volatile.Read(ref current._next), current) == current)
                    {
                        return;
                    }
                }
                else
                {
                    ChannelLeaseMessageQueue? previous = current;
                    current = Volatile.Read(ref previous._next);
                    found = false;
                    do
                    {
                        if (current == queue)
                        {
                            found = true;
                            if (Interlocked.CompareExchange(ref previous._next, Volatile.Read(ref current._next), current) == current)
                            {
                                return;
                            }
                            else
                            {
                                break;
                            }
                        }
                        previous = current;
                        current = Volatile.Read(ref previous!._next);
                    }
                    while (current != null);
                }
            }
            while (found);
        }

        internal static int Count(ref ChannelLeaseMessageQueue? head)
        {
            var current = Volatile.Read(ref head);
            int count = 0;
            while (current != null)
            {
                count++;
                current = Volatile.Read(ref current._next);
            }
            return count;
        }

        internal static void WriteAll(ref ChannelLeaseMessageQueue? head, in RedisChannel channel, in RawResult rawPayload)
        {
            // Count queues first to set correct refCount
            int queueCount = Count(ref head);
            if (queueCount == 0) return;

            // Create one shared lease with refCount = number of queues
            var lease = CreateLeaseFromPayload(in rawPayload, queueCount);
            if (lease == null) return;

            var current = Volatile.Read(ref head);
            while (current != null)
            {
                current.Write(channel, lease);
                current = Volatile.Read(ref current._next);
            }
        }

        private static RefCountedLease<byte>? CreateLeaseFromPayload(in RawResult rawPayload, int refCount)
        {
            if (rawPayload.IsNull) return null;
            var payload = rawPayload.Payload;
            if (payload.IsEmpty) return RefCountedLease<byte>.Empty;
            var lease = RefCountedLease<byte>.Create(checked((int)payload.Length), refCount, clear: false);
            payload.CopyTo(lease.Span);
            return lease;
        }

        private async Task OnMessageAsyncImpl()
        {
            var handler = (Func<ChannelLeaseMessage, Task>?)_onMessageHandler;
            while (!Completion.IsCompleted)
            {
                ChannelLeaseMessage next;
                try { if (!TryRead(out next)) next = await ReadAsync().ForAwait(); }
                catch (ChannelClosedException) { break; }
                catch (Exception ex)
                {
                    _parent?.multiplexer?.OnInternalError(ex);
                    break;
                }

                try
                {
                    var task = handler?.Invoke(next);
                    if (task != null && task.Status != TaskStatus.RanToCompletion) await task.ForAwait();
                }
                catch { }
            }
        }

        internal static void MarkAllCompleted(ref ChannelLeaseMessageQueue? head)
        {
            var current = Interlocked.Exchange(ref head, null);
            while (current != null)
            {
                current.MarkCompleted();
                current = Volatile.Read(ref current._next);
            }
        }

        private void MarkCompleted(Exception? error = null)
        {
            _parent = null;
            _queue.Writer.TryComplete(error);
        }

        internal void UnsubscribeImpl(Exception? error = null, CommandFlags flags = CommandFlags.None)
        {
            var parent = _parent;
            _parent = null;
            parent?.UnsubscribeLeaseAsync(Channel, null, this, flags);
            _queue.Writer.TryComplete(error);
        }

        internal async Task UnsubscribeAsyncImpl(Exception? error = null, CommandFlags flags = CommandFlags.None)
        {
            var parent = _parent;
            _parent = null;
            if (parent != null)
            {
                await parent.UnsubscribeLeaseAsync(Channel, null, this, flags).ForAwait();
            }
            _queue.Writer.TryComplete(error);
        }

        /// <summary>
        /// Stop receiving messages on this channel.
        /// </summary>
        /// <param name="flags">The flags to use when unsubscribing.</param>
        public void Unsubscribe(CommandFlags flags = CommandFlags.None) => UnsubscribeImpl(null, flags);

        /// <summary>
        /// Stop receiving messages on this channel.
        /// </summary>
        /// <param name="flags">The flags to use when unsubscribing.</param>
        public Task UnsubscribeAsync(CommandFlags flags = CommandFlags.None) => UnsubscribeAsyncImpl(null, flags);

        /// <inheritdoc cref="IAsyncEnumerable{ChannelLeaseMessage}.GetAsyncEnumerator(CancellationToken)"/>
#if NETCOREAPP3_0_OR_GREATER
        public IAsyncEnumerator<ChannelLeaseMessage> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => _queue.Reader.ReadAllAsync().GetAsyncEnumerator(cancellationToken);
#else
        public async IAsyncEnumerator<ChannelLeaseMessage> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            while (await _queue.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_queue.Reader.TryRead(out var item))
                {
                    yield return item;
                }
            }
        }
#endif
    }
}
