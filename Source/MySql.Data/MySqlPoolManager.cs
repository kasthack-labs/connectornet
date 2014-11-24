// Copyright � 2004, 2013, Oracle and/or its affiliates. All rights reserved.
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Security;
using System.Security.Principal;
using System.Threading;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for MySqlPoolManager.
    /// </summary>
    internal class MySqlPoolManager {
        private static readonly Dictionary<string, MySqlPool> Pools = new Dictionary<string, MySqlPool>();
        private static readonly List<MySqlPool> ClearingPools = new List<MySqlPool>();

        // Timeout in seconds, after which an unused (idle) connection 
        // should be closed.
        internal static int MaxConnectionIdleTime = 180;

        static MySqlPoolManager() {
            AppDomain.CurrentDomain.ProcessExit += EnsureClearingPools;
            AppDomain.CurrentDomain.DomainUnload += EnsureClearingPools;
        }

        private static void EnsureClearingPools( object sender, EventArgs e ) { ClearAllPools(); }

        // we add a small amount to the due time to let the cleanup detect
        //expired connections in the first cleanup.
        private static Timer _timer = new Timer(
            CleanIdleConnections,
            null,
            ( MaxConnectionIdleTime * 1000 ) + 8000,
            MaxConnectionIdleTime * 1000 );

        private static string GetKey( MySqlConnectionStringBuilder settings ) {
            var key = "";
            lock ( settings ) {
                key = settings.ConnectionString;
            }
            if ( settings.IntegratedSecurity
                 && !settings.ConnectionReset )
                try {
                    // Append SID to the connection string to generate a key
                    // With Integrated security different Windows users with the same
                    // connection string may be mapped to different MySQL accounts.
                    var id = WindowsIdentity.GetCurrent();

                    key += ";" + id.User;
                }
                catch ( SecurityException ex ) {
                    // Documentation for WindowsIdentity.GetCurrent() states 
                    // SecurityException can be thrown. In this case the 
                    // connection can only be pooled if reset is done.
                    throw new MySqlException( Resources.NoWindowsIdentity, ex );
                }
            return key;
        }

        public static MySqlPool GetPool( MySqlConnectionStringBuilder settings ) {
            var text = GetKey( settings );

            lock ( Pools ) {
                MySqlPool pool;
                Pools.TryGetValue( text, out pool );

                if ( pool == null ) {
                    pool = new MySqlPool( settings );
                    Pools.Add( text, pool );
                }
                else pool.Settings = settings;

                return pool;
            }
        }

        public static void RemoveConnection( Driver driver ) {
            Debug.Assert( driver != null );

            var pool = driver.Pool;
            if ( pool == null ) return;

            pool.RemoveConnection( driver );
        }

        public static void ReleaseConnection( Driver driver ) {
            Debug.Assert( driver != null );

            var pool = driver.Pool;
            if ( pool == null ) return;

            pool.ReleaseConnection( driver );
        }

        public static void ClearPool( MySqlConnectionStringBuilder settings ) {
            Debug.Assert( settings != null );
            string text;
            try {
                text = GetKey( settings );
            }
            catch ( MySqlException ) {
                // Cannot retrieve windows identity for IntegratedSecurity=true
                // This can be ignored.
                return;
            }
            ClearPoolByText( text );
        }

        private static void ClearPoolByText( string key ) {
            lock ( Pools ) {
                // if pools doesn't have it, then this pool must already have been cleared
                if ( !Pools.ContainsKey( key ) ) return;

                // add the pool to our list of pools being cleared
                var pool = Pools[ key ];
                ClearingPools.Add( pool );

                // now tell the pool to clear itself
                pool.Clear();

                // and then remove the pool from the active pools list
                Pools.Remove( key );
            }
        }

        public static void ClearAllPools() {
            lock ( Pools ) {
                // Create separate keys list.
                var keys = new List<string>( Pools.Count );

                foreach ( var key in Pools.Keys ) keys.Add( key );

                // Remove all pools by key.
                foreach ( var key in keys ) ClearPoolByText( key );
            }
        }

        public static void RemoveClearedPool( MySqlPool pool ) {
            Debug.Assert( ClearingPools.Contains( pool ) );
            ClearingPools.Remove( pool );
        }

        /// <summary>
        /// Remove drivers that have been idle for too long.
        /// </summary>
        public static void CleanIdleConnections( object obj ) {
            var oldDrivers = new List<Driver>();
            lock ( Pools ) {
                foreach ( var key in Pools.Keys ) {
                    var pool = Pools[ key ];
                    oldDrivers.AddRange( pool.RemoveOldIdleConnections() );
                }
            }
            foreach ( var driver in oldDrivers ) driver.Close();
        }
    }
}