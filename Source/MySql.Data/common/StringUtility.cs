using System;
using System.Globalization;
using System.Text;

namespace MySql.Data.MySqlClient {
    public static class StringUtility {
        public static string InvariantToUpper( this string s ) =>
#if CF
      s.ToUpper(CultureInfo.InvariantCulture);
#else
            s.ToUpperInvariant();
#endif

        public static string InvariantToLower( this string s ) =>
#if CF
            s.ToLower(CultureInfo.InvariantCulture);
#else
            s.ToLowerInvariant();
#endif
        public static bool InvariantStartsWith( this string a, string b ) => a.StartsWith( b, StringComparison.Ordinal );
        public static int InvariantIndexOf( this string a, string b ) => a.IndexOf( b, StringComparison.Ordinal );
        public static string InvariantToString<T>( this T a ) where T : IConvertible => a.ToString( CultureInfo.InvariantCulture );
        public static void InvariantAppendFormat( this StringBuilder b, string format, string value ) => b.AppendFormat( CultureInfo.InvariantCulture, format, value );
        public static void InvariantAppendFormat( this StringBuilder b, string format, params object[] value ) => b.AppendFormat( CultureInfo.InvariantCulture, format, value );
        public static void InvariantAppendFormat( this StringBuilder b, string format, StringBuilder value ) => b.AppendFormat( CultureInfo.InvariantCulture, format, value );
    }
}