using System.Security.Cryptography;
using System.Text;
using recache.Configuration;

namespace recache.Security
{
    public sealed class AuthenticationManager
    {
        private readonly RecacheOptions _options;

        public AuthenticationManager(RecacheOptions options)
        {
            _options = options;
        }

        public bool RequiresAuth =>
            _options.RequireAuth &&
            !string.IsNullOrEmpty(_options.Password);

        public bool Validate(string password)
        {
            if (string.IsNullOrEmpty(_options.Password))
                return false;

            var expected = Encoding.UTF8.GetBytes(_options.Password);
            var actual = Encoding.UTF8.GetBytes(password);

            if (expected.Length != actual.Length)
            {
                // Still run a fixed-time comparison against a same-length
                // buffer so mismatched lengths don't leak via timing.
                CryptographicOperations.FixedTimeEquals(expected, expected);
                return false;
            }

            return CryptographicOperations.FixedTimeEquals(expected, actual);
        }
    }
}
