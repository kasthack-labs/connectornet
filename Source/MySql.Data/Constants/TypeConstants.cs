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
                                        Boolean = typeof(bool),
                                        Byte = typeof(byte),
                                        DateTime = typeof(DateTime),
                                        Double = typeof(double),
                                        Enum = typeof( Enum ),
                                        GroupByBehavior = typeof(GroupByBehavior),
                                        IdentifierCase = typeof(IdentifierCase),
                                        InstanceDescriptor = typeof(InstanceDescriptor),
                                        Int16 = typeof(short),
                                        Int32 = typeof(int),
                                        Int64 = typeof(long),
                                        IsolationLevel = typeof(IsolationLevel),
                                        MySqlDateTime = typeof(MySqlDateTime),
                                        MySqlDbType = typeof(MySqlDbType),
                                        Object = typeof(object),
                                        SecBuffer = typeof( SecBuffer ),
                                        String = typeof( string ),
                                        SupportedJoinOperators = typeof(SupportedJoinOperators),
                                        TimeSpan = typeof(TimeSpan),
                                        Type = typeof( Type ),
                                        UInt16 = typeof(ushort),
                                        UInt32 = typeof( uint ),
                                        UInt64 = typeof( ulong );
    }
}