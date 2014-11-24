// Copyright © 2009, 2011, Oracle and/or its affiliates. All rights reserved.
//
// MySQL Connector/NET is licensed under the terms of the GPLv2
// <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most 
// MySQL Connectors. There are special exceptions to the terms and 
// conditions of the GPLv2 as it is applied to this software, see the 
// FLOSS License Exception
// <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
//
// This program is free software; you can redistribute it and/or modify 
// it under the terms of the GNU General Public License as published 
// by the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful, but 
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
// or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License 
// for more details.
//
// You should have received a copy of the GNU General Public License along 
// with this program; if not, write to the Free Software Foundation, Inc., 
// 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient {
    internal class TracingDriver : Driver {
        private static long _driverCounter;
        private readonly long _driverId;
        private ResultSet _activeResult;
        private int _rowSizeInBytes;

        public TracingDriver( MySqlConnectionStringBuilder settings ) : base( settings ) {
            _driverId = Interlocked.Increment( ref _driverCounter );
        }

        public override void Open() {
            base.Open();
            MySqlTrace.TraceEvent(
                TraceEventType.Information,
                MySqlTraceEventType.ConnectionOpened,
                Resources.TraceOpenConnection,
                _driverId,
                Settings.ConnectionString,
                ThreadId );
        }

        public override void Close() {
            base.Close();
            MySqlTrace.TraceEvent(
                TraceEventType.Information,
                MySqlTraceEventType.ConnectionClosed,
                Resources.TraceCloseConnection,
                _driverId );
        }

        public override void SendQuery( MySqlPacket p ) {
            _rowSizeInBytes = 0;
            var cmdText = Encoding.GetString( p.Buffer, 5, p.Length - 5 );
            string normalizedQuery = null;

            if ( cmdText.Length > 300 ) {
                var normalizer = new QueryNormalizer();
                normalizedQuery = normalizer.Normalize( cmdText );
                cmdText = cmdText.Substring( 0, 300 );
            }

            base.SendQuery( p );

            MySqlTrace.TraceEvent(
                TraceEventType.Information,
                MySqlTraceEventType.QueryOpened,
                Resources.TraceQueryOpened,
                _driverId,
                ThreadId,
                cmdText );
            if ( normalizedQuery != null )
                MySqlTrace.TraceEvent(
                    TraceEventType.Information,
                    MySqlTraceEventType.QueryNormalized,
                    Resources.TraceQueryNormalized,
                    _driverId,
                    ThreadId,
                    normalizedQuery );
        }

        protected override int GetResult( int statementId, ref int affectedRows, ref long insertedId ) {
            try {
                var fieldCount = base.GetResult( statementId, ref affectedRows, ref insertedId );
                MySqlTrace.TraceEvent(
                    TraceEventType.Information,
                    MySqlTraceEventType.ResultOpened,
                    Resources.TraceResult,
                    _driverId,
                    fieldCount,
                    affectedRows,
                    insertedId );

                return fieldCount;
            }
            catch ( MySqlException ex ) {
                // we got an error so we report it
                MySqlTrace.TraceEvent(
                    TraceEventType.Information,
                    MySqlTraceEventType.Error,
                    Resources.TraceOpenResultError,
                    _driverId,
                    ex.Number,
                    ex.Message );
                throw ex;
            }
        }

        public override ResultSet NextResult( int statementId, bool force ) {
            // first let's see if we already have a resultset on this statementId
            if ( _activeResult != null ) {
                //oldRS = activeResults[statementId];
                if ( Settings.UseUsageAdvisor ) ReportUsageAdvisorWarnings( statementId, _activeResult );
                MySqlTrace.TraceEvent(
                    TraceEventType.Information,
                    MySqlTraceEventType.ResultClosed,
                    Resources.TraceResultClosed,
                    _driverId,
                    _activeResult.TotalRows,
                    _activeResult.SkippedRows,
                    _rowSizeInBytes );
                _rowSizeInBytes = 0;
                _activeResult = null;
            }

            _activeResult = base.NextResult( statementId, force );
            return _activeResult;
        }

        public override int PrepareStatement( string sql, ref MySqlField[] parameters ) {
            var statementId = base.PrepareStatement( sql, ref parameters );
            MySqlTrace.TraceEvent(
                TraceEventType.Information,
                MySqlTraceEventType.StatementPrepared,
                Resources.TraceStatementPrepared,
                _driverId,
                sql,
                statementId );
            return statementId;
        }

        public override void CloseStatement( int id ) {
            base.CloseStatement( id );
            MySqlTrace.TraceEvent(
                TraceEventType.Information,
                MySqlTraceEventType.StatementClosed,
                Resources.TraceStatementClosed,
                _driverId,
                id );
        }

        public override void SetDatabase( string dbName ) {
            base.SetDatabase( dbName );
            MySqlTrace.TraceEvent( TraceEventType.Information, MySqlTraceEventType.NonQuery, Resources.TraceSetDatabase, _driverId, dbName );
        }

        public override void ExecuteStatement( MySqlPacket packetToExecute ) {
            base.ExecuteStatement( packetToExecute );
            var pos = packetToExecute.Position;
            packetToExecute.Position = 1;
            var statementId = packetToExecute.ReadInteger( 4 );
            packetToExecute.Position = pos;

            MySqlTrace.TraceEvent(
                TraceEventType.Information,
                MySqlTraceEventType.StatementExecuted,
                Resources.TraceStatementExecuted,
                _driverId,
                statementId,
                ThreadId );
        }

        public override bool FetchDataRow( int statementId, int columns ) {
            try {
                var b = base.FetchDataRow( statementId, columns );
                if ( b ) _rowSizeInBytes += ( Handler as NativeDriver ).Packet.Length;
                return b;
            }
            catch ( MySqlException ex ) {
                MySqlTrace.TraceEvent(
                    TraceEventType.Error,
                    MySqlTraceEventType.Error,
                    Resources.TraceFetchError,
                    _driverId,
                    ex.Number,
                    ex.Message );
                throw ex;
            }
        }

        public override void CloseQuery( MySqlConnection connection, int statementId ) {
            base.CloseQuery( connection, statementId );

            MySqlTrace.TraceEvent( TraceEventType.Information, MySqlTraceEventType.QueryClosed, Resources.TraceQueryDone, _driverId );
        }

        public override List<MySqlError> ReportWarnings( MySqlConnection connection ) {
            var warnings = base.ReportWarnings( connection );
            foreach ( var warning in warnings )
                MySqlTrace.TraceEvent(
                    TraceEventType.Warning,
                    MySqlTraceEventType.Warning,
                    Resources.TraceWarning,
                    _driverId,
                    warning.Level,
                    warning.Code,
                    warning.Message );
            return warnings;
        }

        private bool AllFieldsAccessed( ResultSet rs ) {
            if ( rs.Fields == null
                 || rs.Fields.Length == 0 ) return true;

            for ( var i = 0; i < rs.Fields.Length; i++ ) if ( !rs.FieldRead( i ) ) return false;
            return true;
        }

        private void ReportUsageAdvisorWarnings( int statementId, ResultSet rs ) {
#if !RT
            if ( !Settings.UseUsageAdvisor ) return;

            if ( HasStatus( ServerStatusFlags.NoIndex ) )
                MySqlTrace.TraceEvent(
                    TraceEventType.Warning,
                    MySqlTraceEventType.UsageAdvisorWarning,
                    Resources.TraceUAWarningNoIndex,
                    _driverId,
                    UsageAdvisorWarningFlags.NoIndex );
            else if ( HasStatus( ServerStatusFlags.BadIndex ) )
                MySqlTrace.TraceEvent(
                    TraceEventType.Warning,
                    MySqlTraceEventType.UsageAdvisorWarning,
                    Resources.TraceUAWarningBadIndex,
                    _driverId,
                    UsageAdvisorWarningFlags.BadIndex );

            // report abandoned rows
            if ( rs.SkippedRows > 0 )
                MySqlTrace.TraceEvent(
                    TraceEventType.Warning,
                    MySqlTraceEventType.UsageAdvisorWarning,
                    Resources.TraceUAWarningSkippedRows,
                    _driverId,
                    UsageAdvisorWarningFlags.SkippedRows,
                    rs.SkippedRows );

            // report not all fields accessed
            if ( !AllFieldsAccessed( rs ) ) {
                var notAccessed = new StringBuilder( "" );
                var delimiter = "";
                for ( var i = 0; i < rs.Size; i++ )
                    if ( !rs.FieldRead( i ) ) {
                        notAccessed.AppendFormat( "{0}{1}", delimiter, rs.Fields[ i ].ColumnName );
                        delimiter = ",";
                    }
                MySqlTrace.TraceEvent(
                    TraceEventType.Warning,
                    MySqlTraceEventType.UsageAdvisorWarning,
                    Resources.TraceUAWarningSkippedColumns,
                    _driverId,
                    UsageAdvisorWarningFlags.SkippedColumns,
                    notAccessed.ToString() );
            }

            // report type conversions if any
            if ( rs.Fields == null ) return;
            foreach ( var f in rs.Fields ) {
                var s = new StringBuilder();
                var delimiter = "";
                foreach ( var t in f.TypeConversions ) {
                    s.AppendFormat( "{0}{1}", delimiter, t.Name );
                    delimiter = ",";
                }
                if ( s.Length > 0 )
                    MySqlTrace.TraceEvent(
                        TraceEventType.Warning,
                        MySqlTraceEventType.UsageAdvisorWarning,
                        Resources.TraceUAWarningFieldConversion,
                        _driverId,
                        UsageAdvisorWarningFlags.FieldConversion,
                        f.ColumnName,
                        s.ToString() );
            }
#endif
        }
    }
}