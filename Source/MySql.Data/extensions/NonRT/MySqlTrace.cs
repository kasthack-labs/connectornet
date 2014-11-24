﻿// Copyright © 2009, 2013, Oracle and/or its affiliates. All rights reserved.
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

using System;
using System.Diagnostics;
using System.Reflection;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient {
    public sealed class MySqlTrace {
#if !CF
        protected static string QaHost;

        static MySqlTrace() {
            foreach ( var listener in Source.Listeners )
                if ( listener.GetType().ToString().Contains( "MySql.EMTrace.EMTraceListener" ) ) {
                    QueryAnalysisEnabled = true;
                    break;
                }
        }

        public static TraceListenerCollection Listeners => Source.Listeners;

        public static SourceSwitch Switch {
            get {
                return Source.Switch;
            }
            set {
                Source.Switch = value;
            }
        }

        public static bool QueryAnalysisEnabled { get; protected set; }

        public static void EnableQueryAnalyzer( string host, int postInterval ) {
            if ( QueryAnalysisEnabled ) return;
            // create a EMTraceListener and add it to our source
            var l =
                (TraceListener)
                Activator.CreateInstance(
                    "MySql.EMTrace",
                    "MySql.EMTrace.EMTraceListener",
                    false,
                    BindingFlags.CreateInstance,
                    null,
                    new object[] { host, postInterval }, null, null).Unwrap();
            if ( l == null ) throw new MySqlException( Resources.UnableToEnableQueryAnalysis );
            Source.Listeners.Add( l );
            Switch.Level = SourceLevels.All;
        }

        public static void DisableQueryAnalyzer() {
            QueryAnalysisEnabled = false;
            foreach ( TraceListener l in Source.Listeners )
                if ( l.GetType().ToString().Contains( "EMTraceListener" ) ) {
                    Source.Listeners.Remove( l );
                    break;
                }
        }

        internal static TraceSource Source { get; } = new TraceSource( "mysql" );
#endif

        internal static void LogInformation( int id, string msg ) {
#if !CF
            Source.TraceEvent( TraceEventType.Information, id, msg, MySqlTraceEventType.NonQuery, -1 );
            Trace.TraceInformation( msg );
#endif
        }

        internal static void LogWarning( int id, string msg ) {
#if !CF
            Source.TraceEvent( TraceEventType.Warning, id, msg, MySqlTraceEventType.NonQuery, -1 );
            Trace.TraceWarning( msg );
#endif
        }

        internal static void LogError( int id, string msg ) {
#if !CF
            Source.TraceEvent( TraceEventType.Error, id, msg, MySqlTraceEventType.NonQuery, -1 );
            Trace.TraceError( msg );
#endif
        }

#if !CF
        internal static void TraceEvent(
            TraceEventType eventType,
            MySqlTraceEventType mysqlEventType,
            string msgFormat,
            params object[] args ) {
            Source.TraceEvent( eventType, (int) mysqlEventType, msgFormat, args );
        }
#endif
    }

    public enum UsageAdvisorWarningFlags {
        NoIndex = 1,
        BadIndex,
        SkippedRows,
        SkippedColumns,
        FieldConversion
    }

    public enum MySqlTraceEventType {
        ConnectionOpened = 1,
        ConnectionClosed,
        QueryOpened,
        ResultOpened,
        ResultClosed,
        QueryClosed,
        StatementPrepared,
        StatementExecuted,
        StatementClosed,
        NonQuery,
        UsageAdvisorWarning,
        Warning,
        Error,
        QueryNormalized
    }
}