using System;
using System.Text;
using Pipelines.Sockets.Unofficial;

namespace StackExchange.Redis
{
    /// <summary>
    /// An <see cref="ICompletable"/> that invokes lease-based subscription handlers with a pooled <see cref="Lease{T}"/>,
    /// and disposes the lease after all handlers have completed.
    /// </summary>
    internal sealed class LeaseMessageCompletable : ICompletable
    {
        private readonly RedisChannel channel;
        private readonly Lease<byte> lease;
        private readonly Action<RedisChannel, Lease<byte>> handler;

        public LeaseMessageCompletable(RedisChannel channel, Lease<byte> lease, Action<RedisChannel, Lease<byte>> handler)
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
                try
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
                }
                finally
                {
                    lease.Dispose();
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
