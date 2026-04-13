using System;
using System.Text;
using Pipelines.Sockets.Unofficial;

namespace StackExchange.Redis
{
    /// <summary>
    /// An <see cref="ICompletable"/> that invokes lease-based subscription handlers with a <see cref="RefCountedLease{T}"/>.
    /// Each handler receives its own <see cref="RefCountedLease{T}"/> that shares the same underlying pooled buffer.
    /// The buffer is returned to the pool only when every handler has called <see cref="RefCountedLease{T}.Dispose"/>.
    /// </summary>
    internal sealed class LeaseMessageCompletable : ICompletable
    {
        private readonly RedisChannel channel;
        private readonly RefCountedLease<byte> lease;
        private readonly Action<RedisChannel, RefCountedLease<byte>> handler;

        public LeaseMessageCompletable(RedisChannel channel, RefCountedLease<byte> lease, Action<RedisChannel, RefCountedLease<byte>> handler)
        {
            this.channel = channel;
            this.lease = lease;
            this.handler = handler;
        }

        public override string? ToString() => (string?)channel;

        public bool TryComplete(bool isAsync)
        {
            if (isAsync)
            {
                if (handler != null)
                {
                    ConnectionMultiplexer.TraceWithoutContext("Invoking lease (async)...: " + (string?)channel, "Subscription");
                    if (handler.IsSingle())
                    {
                        try { handler(channel, lease); } catch { }
                    }
                    else
                    {
                        foreach (var sub in handler.AsEnumerable())
                        {
                            try { sub.Invoke(channel, lease); } catch { }
                        }
                    }
                    ConnectionMultiplexer.TraceWithoutContext("Invoke lease complete (async)", "Subscription");
                }
                return true;
            }
            else
            {
                return handler == null; // anything async to do?
            }
        }

        void ICompletable.AppendStormLog(StringBuilder sb) => sb.Append("event, pub/sub lease: ").Append((string?)channel);
    }
}
