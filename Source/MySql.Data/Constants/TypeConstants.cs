using System;
using System.ComponentModel.Design.Serialization;
using System.Data.Common;
using System.Transactions;
using MySql.Data.MySqlClient;
using MySql.Data.MySqlClient.Authentication;
using MySql.Data.Types;

namespace MySql.Data.Constants {
    internal static class Types {
        internal static readonly Type
                                        TBoolean = typeof(bool),
                                        TByte = typeof(byte),
                                        TByteArray = typeof(byte[]),
                                        TDateTime = typeof(DateTime),
                                        TDecimal = typeof(decimal),
                                        TDouble = typeof(double),
                                        TEnum = typeof( Enum ),
                                        TGroupByBehavior = typeof(GroupByBehavior),
                                        TGuid = typeof(Guid),
                                        TIdentifierCase = typeof(IdentifierCase),
                                        TInstanceDescriptor = typeof(InstanceDescriptor),
                                        TInt16 = typeof(short),
                                        TInt32 = typeof(int),
                                        TInt64 = typeof(long),
                                        TIsolationLevel = typeof(IsolationLevel),
                                        TMySqlDateTime = typeof(MySqlDateTime),
                                        TMySqlDbType = typeof(MySqlDbType),
                                        TObject = typeof(object),
                                        TSByte = typeof(sbyte),
                                        TSecBuffer = typeof( SecBuffer ),
                                        TSingle = typeof(float),
                                        TString = typeof( string ),
                                        TSupportedJoinOperators = typeof(SupportedJoinOperators),
                                        TTimeSpan = typeof(TimeSpan),
                                        TType = typeof( Type ),
                                        TUInt16 = typeof(ushort),
                                        TUInt32 = typeof( uint ),
                                        TUInt64 = typeof( ulong );
    }
}