using System;

namespace recache.Storage
{
    public sealed class JsonPathException : Exception
    {
        public JsonPathException(string message)
            : base(message)
        {
        }
    }
}
