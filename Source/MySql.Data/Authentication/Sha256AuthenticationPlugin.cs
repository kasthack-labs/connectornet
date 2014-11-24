// Copyright © 2013, Oracle and/or its affiliates. All rights reserved.
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
namespace MySql.Data.MySqlClient.Authentication {
    /// <summary>
    /// The implementation of the sha256_password authentication plugin.
    /// </summary>
    public class Sha256AuthenticationPlugin : MySqlAuthenticationPlugin {
        private byte[] _rawPubkey;
        public override string PluginName => "sha256_password";
        protected override byte[] MoreData( byte[] data ) {
            _rawPubkey = data;
            var buffer = GetPassword() as byte[];
            return buffer;
        }
        public override object GetPassword() {
            if ( Settings.SslMode == MySqlSslMode.None )
                throw new NotImplementedException( "You can use sha256 plugin only in SSL connections in this implementation." );
            // send as clear text, since the channel is already encrypted
            var passBytes = Encoding.GetBytes( Settings.Password );
            var buffer = new byte[passBytes.Length + 1];
            Array.Copy( passBytes, 0, buffer, 0, passBytes.Length );
            buffer[ passBytes.Length ] = 0;
            return buffer;
        }
    }
}