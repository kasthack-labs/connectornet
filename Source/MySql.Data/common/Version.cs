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
using System.Globalization;
using MySql.Data.MySqlClient;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.Common {
    /// <summary>
    /// Summary description for Version.
    /// </summary>
    internal struct DbVersion {
        private readonly int _major;
        private readonly int _minor;
        private readonly int _build;
        private readonly string _srcString;

        public DbVersion( string s, int major, int minor, int build ) {
            _major = major;
            _minor = minor;
            _build = build;
            _srcString = s;
        }

        public int Major => _major;

        public int Minor => _minor;

        public int Build => _build;

        public static DbVersion Parse( string versionString ) {
            var start = 0;
            var index = versionString.IndexOf( '.', start );
            if ( index == -1 ) throw new MySqlException( Resources.BadVersionFormat );
            var val = versionString.Substring( start, index - start ).Trim();
            var major = Convert.ToInt32( val, NumberFormatInfo.InvariantInfo );

            start = index + 1;
            index = versionString.IndexOf( '.', start );
            if ( index == -1 ) throw new MySqlException( Resources.BadVersionFormat );
            val = versionString.Substring( start, index - start ).Trim();
            var minor = Convert.ToInt32( val, NumberFormatInfo.InvariantInfo );

            start = index + 1;
            var i = start;
            while ( i < versionString.Length
                    && Char.IsDigit( versionString, i ) ) i++;
            val = versionString.Substring( start, i - start ).Trim();
            var build = Convert.ToInt32( val, NumberFormatInfo.InvariantInfo );

            return new DbVersion( versionString, major, minor, build );
        }

        public bool IsAtLeast( int majorNum, int minorNum, int buildNum ) {
            if ( _major > majorNum ) return true;
            if ( _major == majorNum
                 && _minor > minorNum ) return true;
            if ( _major == majorNum
                 && _minor == minorNum
                 && _build >= buildNum ) return true;
            return false;
        }

        public override string ToString() => _srcString;
    }
}