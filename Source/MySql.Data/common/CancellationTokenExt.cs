using System.Threading;

namespace MySql.Data.MySqlClient.common {
    internal static class CancellationTokenExt {
        internal static bool IsntCancelled(this CancellationToken cancellationToken ) {
            return cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested;
        }
    }
}
