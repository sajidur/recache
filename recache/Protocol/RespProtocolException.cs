using System;

namespace recache.Protocol
{
    public sealed class RespProtocolException : Exception
    {
        public RespProtocolException(string message)
            : base(message)
        {
        }

        public RespProtocolException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
