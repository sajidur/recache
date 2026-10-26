using System;

namespace recache.Storage
{
    // Thrown from deep inside the store for a condition the client
    // should simply see as an error reply (non-integer INCR target,
    // overflow). The message is the full RESP error text.
    public sealed class CommandErrorException : Exception
    {
        public CommandErrorException(string message)
            : base(message)
        {
        }
    }
}
