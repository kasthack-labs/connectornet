using System;
using MySql.Data.MySqlClient;

namespace MySql.Data.Types {
    internal static class DsInfoHelper {
        // we use name indexing because this method will only be called
        // when GetSchema is called for the DataSourceInformation 
        // collection and then it wil be cached.

        public static void FillRow(
            MySqlSchemaRow row,
            string typename,
            MySqlDbType providerDbType,
            Type dataType,
            int columnSize = 0,
            string createFormat = null,
            bool isAutoIncrementable = false,
            bool isFixedLength = true,
            bool isUnsigned = false) {
            row[ "TypeName" ] = typename;
            row[ "ProviderDbType" ] = providerDbType;
            row[ "ColumnSize" ] = columnSize;
            row[ "CreateFormat" ] = createFormat??typename;
            row[ "CreateParameters" ] = DBNull.Value;
            row[ "DataType" ] = dataType.ToString();
            row[ "IsAutoincrementable" ] = isAutoIncrementable;
            row[ "IsBestMatch" ] = true;
            row[ "IsCaseSensitive" ] = false;
            row[ "IsFixedLength" ] = isFixedLength;
            row[ "IsFixedPrecisionScale" ] = true;
            row[ "IsLong" ] = false;
            row[ "IsNullable" ] = true;
            row[ "IsSearchable" ] = true;
            row[ "IsSearchableWithLike" ] = false;
            row[ "IsUnsigned" ] = isUnsigned;
            row[ "MaximumScale" ] = 0;
            row[ "MinimumScale" ] = 0;
            row[ "IsConcurrencyType" ] = DBNull.Value;
            row[ "IsLiteralSupported" ] = false;
            row[ "LiteralPrefix" ] = DBNull.Value;
            row[ "LiteralSuffix" ] = DBNull.Value;
            row[ "NativeDataType" ] = DBNull.Value;
        }
    }
}
