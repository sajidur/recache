using System;

namespace recache.Storage
{
    public sealed class WrongTypeException : Exception
    {
        public WrongTypeException()
            : base(
                "WRONGTYPE Operation against a key " +
                "holding the wrong kind of value")
        {
        }
    }
}
