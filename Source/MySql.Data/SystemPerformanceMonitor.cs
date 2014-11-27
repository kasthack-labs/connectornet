// Copyright © 2004, 2010, Oracle and/or its affiliates. All rights reserved.
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
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient {
    internal class SystemPerformanceMonitor : PerformanceMonitor {
        private static PerformanceCounter _procedureHardQueries;
        private static PerformanceCounter _procedureSoftQueries;

        public SystemPerformanceMonitor( MySqlConnection connection ) : base( connection ) {
            if ( !connection.Settings.UsePerformanceMonitor || _procedureHardQueries != null ) return;
            try {
                var categoryName = Resources.PerfMonCategoryName;
                _procedureHardQueries = new PerformanceCounter( categoryName, "HardProcedureQueries", false );
                _procedureSoftQueries = new PerformanceCounter( categoryName, "SoftProcedureQueries", false );
            }
            catch ( Exception ex ) {
                MySqlTrace.LogError( connection.ServerThread, ex.Message );
            }
        }

#if DEBUG
        private void EnsurePerfCategoryExist() {
            var ccdc = new CounterCreationDataCollection {
                new CounterCreationData { CounterType = PerformanceCounterType.NumberOfItems32, CounterName = "HardProcedureQueries" },
                new CounterCreationData { CounterType = PerformanceCounterType.NumberOfItems32, CounterName = "SoftProcedureQueries" }
            };

            if ( !PerformanceCounterCategory.Exists( Resources.PerfMonCategoryName ) )
                PerformanceCounterCategory.Create( Resources.PerfMonCategoryName, null, ccdc );
        }
#endif

        public new void AddHardProcedureQuery() {
            if ( !Connection.Settings.UsePerformanceMonitor || _procedureHardQueries == null ) return;
            _procedureHardQueries.Increment();
        }

        public new void AddSoftProcedureQuery() {
            if ( !Connection.Settings.UsePerformanceMonitor || _procedureSoftQueries == null ) return;
            _procedureSoftQueries.Increment();
        }
    }
}