using System;
using MySql.Data.MySqlClient;

namespace MySql.Data.Types {
    internal static class DsInfoHelper {
        public static void FillDsInfoRow(
            MySqlSchemaRow row,
            string typename,
            MySqlDbType providerDbType,
            int columnSize,
            string createFormat,
            Type dataType,
            bool isAutoIncrementable,
            bool isFixedLength ) {
            row[ "TypeName" ] = typename;
            row[ "ProviderDbType" ] = providerDbType;
            row[ "ColumnSize" ] = columnSize;
            row[ "CreateFormat" ] = createFormat;
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
            row[ "IsUnsigned" ] = false;
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
