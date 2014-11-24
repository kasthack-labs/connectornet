// Copyright (c) 2004-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.
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
using System.Data.Common;
using System.Reflection;
using System.Security.Permissions;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// DBProviderFactory implementation for MysqlClient.
    /// </summary>
    [ReflectionPermission( SecurityAction.Assert, MemberAccess = true )]
    public sealed class MySqlClientFactory : DbProviderFactory, IServiceProvider {
        /// <summary>
        /// Gets an instance of the <see cref="MySqlClientFactory"/>. 
        /// This can be used to retrieve strongly typed data objects. 
        /// </summary>
        public static MySqlClientFactory Instance = new MySqlClientFactory();

        private Type _dbServicesType;
        private FieldInfo _mySqlDbProviderServicesInstance;

        /// <summary>
        /// Returns a strongly typed <see cref="DbCommandBuilder"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbCommandBuilder</b>.</returns>
        public override DbCommandBuilder CreateCommandBuilder() => new MySqlCommandBuilder();

        /// <summary>
        /// Returns a strongly typed <see cref="DbCommand"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbCommand</b>.</returns>
        public override DbCommand CreateCommand() => new MySqlCommand();

        /// <summary>
        /// Returns a strongly typed <see cref="DbConnection"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbConnection</b>.</returns>
        public override DbConnection CreateConnection() => new MySqlConnection();

        /// <summary>
        /// Returns a strongly typed <see cref="DbDataAdapter"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbDataAdapter</b>. </returns>
        public override DbDataAdapter CreateDataAdapter() => new MySqlDataAdapter();

        /// <summary>
        /// Returns a strongly typed <see cref="DbParameter"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbParameter</b>.</returns>
        public override DbParameter CreateParameter() => new MySqlParameter();

        /// <summary>
        /// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbConnectionStringBuilder</b>.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new MySqlConnectionStringBuilder();

        /// <summary>
        /// Returns true if a <b>MySqlDataSourceEnumerator</b> can be created; 
        /// otherwise false. 
        /// </summary>
        public override bool CanCreateDataSourceEnumerator => false;

        #region IServiceProvider Members
        /// <summary>
        /// Provide a simple caching layer
        /// </summary>
        private Type DbServicesType {
            get {
                if ( _dbServicesType == null )
                    // Get the type this way so we don't have to reference System.Data.Entity
                    // from our core provider
                    _dbServicesType = Type.GetType( @"System.Data.Common.DbProviderServices, System.Data.Entity, 
                        Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089", false );
                return _dbServicesType;
            }
        }

        private FieldInfo MySqlDbProviderServicesInstance {
            get {
                if ( _mySqlDbProviderServicesInstance == null ) {
                    var fullName = Assembly.GetExecutingAssembly().FullName;
                    var assemblyName = fullName.Replace( "MySql.Data", "MySql.Data.Entity" );
                    var assemblyEf5Name = fullName.Replace( "MySql.Data", "MySql.Data.Entity.EF5" );
                    fullName = String.Format( "MySql.Data.MySqlClient.MySqlProviderServices, {0}", assemblyEf5Name );

                    var providerServicesType = Type.GetType( fullName, false );
                    if ( providerServicesType == null ) {
                        fullName = String.Format( "MySql.Data.MySqlClient.MySqlProviderServices, {0}", assemblyName );
                        providerServicesType = Type.GetType( fullName, false );
                        if ( providerServicesType == null ) throw new DllNotFoundException( fullName );
                    }
                    _mySqlDbProviderServicesInstance = providerServicesType.GetField(
                        "Instance",
                        BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance );
                }
                return _mySqlDbProviderServicesInstance;
            }
        }

        object IServiceProvider.GetService( Type serviceType ) {
            // DbProviderServices is the only service we offer up right now
            if ( serviceType != DbServicesType ) return null;

            if ( MySqlDbProviderServicesInstance == null ) return null;

            return MySqlDbProviderServicesInstance.GetValue( null );
        }
        #endregion
    }
}