namespace MySql.Data.Constants.ColumnNames {
    public static class Procedures {
        public const string SpecificName = "SPECIFIC_NAME";
        public const string RoutineCatalog = "ROUTINE_CATALOG";
        public const string RoutineSchema = "ROUTINE_SCHEMA";
        public const string RoutineName = "ROUTINE_NAME";
        public const string RoutineType = "ROUTINE_TYPE";
        public const string DtdIdentifier = "DTD_IDENTIFIER";
        public const string RoutineBody = "ROUTINE_BODY";
        public const string RoutineDefinition = "ROUTINE_DEFINITION";
        public const string ExternalName = "EXTERNAL_NAME";
        public const string ExternalLanguage = "EXTERNAL_LANGUAGE";
        public const string ParameterStyle = "PARAMETER_STYLE";
        public const string IsDeterministic = "IS_DETERMINISTIC";
        public const string SqlDataAccess = "SQL_DATA_ACCESS";
        public const string SqlPath = "SQL_PATH";
        public const string SecurityType = "SECURITY_TYPE";
        public const string Created = "CREATED";
        public const string LastAltered = "LAST_ALTERED";
        public const string SqlMode = "SQL_MODE";
        public const string RoutineComment = "ROUTINE_COMMENT";
        public const string Definer = "DEFINER";
    }
    public static class Indexes {
        public const string IndexCatalog = "INDEX_CATALOG";
        public const string IndexSchema = "INDEX_SCHEMA";
        public const string IndexName = "INDEX_NAME";
        public const string Unique = "UNIQUE";
        public const string Primary = "PRIMARY";
        public const string Type = "TYPE";
        public const string Comment = "COMMENT";
        public const string KeyName = "KEY_NAME";
        public const string Table = "TABLE";
        public const string NonUnique = "NON_UNIQUE";
        public const string IndexType = "INDEX_TYPE";
        public const string SortOrder = "SORT_ORDER";
    }
    public static class Columns {
        public const string OrdinalPosition = "ORDINAL_POSITION";
        public const string ColumnName = "COLUMN_NAME";
        public const string ColumnDefault = "COLUMN_DEFAULT";
        public const string IsNullable = "IS_NULLABLE";
        public const string DataType = "DATA_TYPE";
        public const string CharacterMaximumLength = "CHARACTER_MAXIMUM_LENGTH";
        public const string CharacterOctetLength = "CHARACTER_OCTET_LENGTH";
        public const string NumericPrecision = "NUMERIC_PRECISION";
        public const string NumericScale = "NUMERIC_SCALE";
        public const string CharacterSetName = "CHARACTER_SET_NAME";
        public const string CollationName = "COLLATION_NAME";
        public const string ColumnType = "COLUMN_TYPE";
        public const string ColumnKey = "COLUMN_KEY";
        public const string Extra = "EXTRA";
        public const string Privileges = "PRIVILEGES";
        public const string ColumnComment = "COLUMN_COMMENT";
    }

    public static class Constraints {
        public const string ConstraintCatalog = "CONSTRAINT_CATALOG";
        public const string ConstraintSchema = "CONSTRAINT_SCHEMA";
        public const string ConstraintName = "CONSTRAINT_NAME";
        public const string MatchOption = "MATCH_OPTION";
        public const string UpdateRule = "UPDATE_RULE";
        public const string DeleteRule = "DELETE_RULE";
        public const string ReferencedTableCatalog = "REFERENCED_TABLE_CATALOG";
        public const string ReferencedTableSchema = "REFERENCED_TABLE_SCHEMA";
        public const string ReferencedTableName = "REFERENCED_TABLE_NAME";
        public const string ReferencedColumnName = "REFERENCED_COLUMN_NAME";
    }
    public static class Shared {
        public const string TableCatalog = "TABLE_CATALOG";
        public const string TableSchema = "TABLE_SCHEMA";
        public const string TableName = "TABLE_NAME";
    }
    public static class Tables {
        public const string TableType = "TABLE_TYPE";
        public const string Engine = "ENGINE";
        public const string Version = "VERSION";
        public const string RowFormat = "ROW_FORMAT";
        public const string TableRows = "TABLE_ROWS";
        public const string TableComment = "TABLE_COMMENT";
        public const string CreateOptions = "CREATE_OPTIONS";
        public const string AvgRowLength = "AVG_ROW_LENGTH";
        public const string DataLength = "DATA_LENGTH";
        public const string MaxDataLength = "MAX_DATA_LENGTH";
        public const string IndexLength = "INDEX_LENGTH";
        public const string DataFree = "DATA_FREE";
        public const string AutoIncrement = "AUTO_INCREMENT";
        public const string CreateTime = "CREATE_TIME";
        public const string UpdateTime = "UPDATE_TIME";
        public const string CheckTime = "CHECK_TIME";
        public const string TableCollation = "TABLE_COLLATION";
        public const string Checksum = "CHECKSUM";
        public const string SchemaName = "SCHEMA_NAME";
    }

    public static class DsInfo {
        public const string TypeName = "TypeName";
        public const string ProviderDbType = "ProviderDbType";
        public const string ColumnSize = "ColumnSize";
        public const string CreateFormat = "CreateFormat";
        public const string CreateParameters = "CreateParameters";
        public const string DataType = "DataType";
        public const string IsAutoincrementable = "IsAutoincrementable";
        public const string IsBestMatch = "IsBestMatch";
        public const string IsCaseSensitive = "IsCaseSensitive";
        public const string IsFixedLength = "IsFixedLength";
        public const string IsFixedPrecisionScale = "IsFixedPrecisionScale";
        public const string IsLong = "IsLong";
        public const string IsNullable = "IsNullable";
        public const string IsSearchable = "IsSearchable";
        public const string IsSearchableWithLike = "IsSearchableWithLike";
        public const string IsUnsigned = "IsUnsigned";
        public const string MaximumScale = "MaximumScale";
        public const string MinimumScale = "MinimumScale";
        public const string IsConcurrencyType = "IsConcurrencyType";
        public const string IsLiteralSupported = "IsLiteralSupported";
        public const string LiteralPrefix = "LiteralPrefix";
        public const string LiteralSuffix = "LiteralSuffix";
        public const string NativeDataType = "NativeDataType";
    }
}