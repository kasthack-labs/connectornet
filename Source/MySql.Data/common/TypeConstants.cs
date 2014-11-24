using System;

namespace MySql.Data.MySqlClient.common {
    internal static class TypeConstants {
        internal static readonly Type String = typeof( string );
        internal static readonly Type Int32 = typeof( int );
        internal static readonly Type UInt32 = typeof( uint );
        internal static readonly Type Boolean = typeof( bool );
        internal static readonly Type UInt64 = typeof( ulong );
        internal static readonly Type Type = typeof( Type );
        internal static readonly Type DateTime = typeof( DateTime );
    }
}