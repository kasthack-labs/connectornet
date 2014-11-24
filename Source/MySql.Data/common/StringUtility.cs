using System;
using System.Globalization;
using System.Text;

namespace MySql.Data.MySqlClient {
    public static class StringUtility {
        public static string InvariantToUpper( this string s ) => s.ToUpperInvariant();
        public static string InvariantToLower( this string s ) => s.ToLowerInvariant();
        public static bool InvariantStartsWith( this string a, string b ) => a.StartsWith( b, StringComparison.Ordinal );
        public static int InvariantIndexOf( this string a, string b ) => a.IndexOf( b, StringComparison.Ordinal );
        public static string InvariantToString<T>( this T a ) where T : IConvertible => a.ToString( CultureInfo.InvariantCulture );
        public static void InvariantAppendFormat( this StringBuilder b, string format, string value ) => b.AppendFormat( CultureInfo.InvariantCulture, format, value );
        public static void InvariantAppendFormat( this StringBuilder b, string format, params object[] value ) => b.AppendFormat( CultureInfo.InvariantCulture, format, value );
        public static void InvariantAppendFormat( this StringBuilder b, string format, StringBuilder value ) => b.AppendFormat( CultureInfo.InvariantCulture, format, value );
    }
}