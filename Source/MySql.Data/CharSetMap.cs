// Copyright © 2004, 2014, Oracle and/or its affiliates. All rights reserved.
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
using System.Text;
using MySql.Data.Common;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for CharSetMap.
    /// </summary>
    internal class CharSetMap {
        private static Dictionary<string, string> _defaultCollations;
        private static Dictionary<string, int> _maxLengths;
        private static Dictionary<string, CharacterSet> _mapping;
        private static readonly object LockObject;

        // we use a static constructor here since we only want to init
        // the mapping once
        static CharSetMap() {
            LockObject = new Object();
            InitializeMapping();
        }

        public static CharacterSet GetCharacterSet( DbVersion version, string charSetName ) {
            CharacterSet cs = null;
            if ( _mapping.ContainsKey( charSetName ) ) cs = _mapping[ charSetName ];

            if ( cs == null ) throw new MySqlException( "Character set '" + charSetName + "' is not supported by .Net Framework." );
            return cs;
        }

        /// <summary>
        /// Returns the text encoding for a given MySQL character set name
        /// </summary>
        /// <param name="version">Version of the connection requesting the encoding</param>
        /// <param name="charSetName">Name of the character set to get the encoding for</param>
        /// <returns>Encoding object for the given character set name</returns>
        public static Encoding GetEncoding( DbVersion version, string charSetName ) {
            try {
                return Encoding.GetEncoding( GetCharacterSet( version, charSetName ).Name );
            }
            catch ( NotSupportedException ) {
                return Encoding.GetEncoding( "utf-8" );
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private static void InitializeMapping() { LoadCharsetMap(); }

        private static void LoadCharsetMap() {
            _mapping = new Dictionary<string, CharacterSet> {
                { "latin1", new CharacterSet( "windows-1252", 1 ) },
                { "big5", new CharacterSet( "big5", 2 ) },
                { "cp850", new CharacterSet( "ibm850", 1 ) },
                { "koi8r", new CharacterSet( "koi8-u", 1 ) },
                { "latin2", new CharacterSet( "latin2", 1 ) },
                { "ujis", new CharacterSet( "EUC-JP", 3 ) },
                { "hebrew", new CharacterSet( "hebrew", 1 ) },
                { "tis620", new CharacterSet( "windows-874", 1 ) },
                { "euckr", new CharacterSet( "euc-kr", 2 ) },
                { "sjis", new CharacterSet( "sjis", 2 ) },
                { "koi8u", new CharacterSet( "koi8-u", 1 ) },
                { "macce", new CharacterSet( "x-mac-ce", 1 ) },
                { "macroman", new CharacterSet( "x-mac-romanian", 1 ) },
                { "cp852", new CharacterSet( "ibm852", 2 ) },
                { "latin7", new CharacterSet( "iso-8859-7", 1 ) },
                { "cp1251", new CharacterSet( "windows-1251", 1 ) },
                { "gb2312", new CharacterSet( "gb2312", 2 ) },
                { "greek", new CharacterSet( "greek", 1 ) },
                { "cp1250", new CharacterSet( "windows-1250", 1 ) },
                { "utf8", new CharacterSet( "utf-8", 3 ) },
                { "ucs2", new CharacterSet( "UTF-16BE", 2 ) },
                { "cp866", new CharacterSet( "cp866", 1 ) },
                { "latin5", new CharacterSet( "latin5", 1 ) },
                { "cp1256", new CharacterSet( "cp1256", 1 ) },
                { "cp1257", new CharacterSet( "windows-1257", 1 ) },
                { "ascii", new CharacterSet( "us-ascii", 1 ) },
                { "latin3", new CharacterSet( "latin3", 1 ) },
                { "latin4", new CharacterSet( "latin4", 1 ) },
                { "latin1_de", new CharacterSet( "iso-8859-1", 1 ) },
                { "german1", new CharacterSet( "iso-8859-1", 1 ) },
                { "danish", new CharacterSet( "iso-8859-1", 1 ) },
                { "czech", new CharacterSet( "iso-8859-2", 1 ) },
                { "hungarian", new CharacterSet( "iso-8859-2", 1 ) },
                { "croat", new CharacterSet( "iso-8859-2", 1 ) },
                { "latvian", new CharacterSet( "iso-8859-13", 1 ) },
                { "latvian1", new CharacterSet( "iso-8859-13", 1 ) },
                { "estonia", new CharacterSet( "iso-8859-13", 1 ) },
                { "dos", new CharacterSet( "ibm437", 1 ) },
                { "utf8mb4", new CharacterSet( "utf-8", 4 ) },
                { "utf16", new CharacterSet( "utf-16BE", 2 ) },
                { "utf16le", new CharacterSet( "utf-16", 2 ) },
                { "utf32", new CharacterSet( "utf-32BE", 4 ) }
            };

            _mapping.Add( "hp8", _mapping[ "latin1" ] );
            _mapping.Add( "dec8", _mapping[ "latin1" ] );
            _mapping.Add( "swe7", _mapping[ "latin1" ] );
            _mapping.Add( "eucjpms", _mapping[ "ujis" ] );
            _mapping.Add( "cp932", _mapping[ "sjis" ] );
            _mapping.Add( "euc_kr", _mapping[ "euckr" ] );
            _mapping.Add( "koi8_ru", _mapping[ "koi8u" ] );
            _mapping.Add( "gbk", _mapping[ "gb2312" ] );
            _mapping.Add( "win1250", _mapping[ "cp1250" ] );
            _mapping.Add( "armscii8", _mapping[ "latin1" ] );
            _mapping.Add( "keybcs2", _mapping[ "latin1" ] );
            _mapping.Add( "win1251ukr", _mapping[ "cp1251" ] );
            _mapping.Add( "cp1251csas", _mapping[ "cp1251" ] );
            _mapping.Add( "cp1251cias", _mapping[ "cp1251" ] );
            _mapping.Add( "win1251", _mapping[ "cp1251" ] );
            _mapping.Add( "usa7", _mapping[ "ascii" ] );
            _mapping.Add( "binary", _mapping[ "ascii" ] );
        }

        internal static void InitCollections( MySqlConnection connection ) {
            _defaultCollations = new Dictionary<string, string>();
            _maxLengths = new Dictionary<string, int>();

            var cmd = new MySqlCommand( "SHOW CHARSET", connection );
            using ( var reader = cmd.ExecuteReader() )
                while ( reader.Read() ) {
                    _defaultCollations.Add( reader.GetString( 0 ), reader.GetString( 2 ) );
                    _maxLengths.Add( reader.GetString( 0 ), Convert.ToInt32( reader.GetValue( 3 ) ) );
                }
        }

        internal static string GetDefaultCollation( string charset, MySqlConnection connection ) {
            lock ( LockObject ) {
                if ( _defaultCollations == null ) InitCollections( connection );
            }
            if ( !_defaultCollations.ContainsKey( charset ) ) return null;
            return _defaultCollations[ charset ];
        }

        internal static int GetMaxLength( string charset, MySqlConnection connection ) {
            lock ( LockObject ) {
                if ( _maxLengths == null ) InitCollections( connection );
            }

            if ( !_maxLengths.ContainsKey( charset ) ) return 1;
            return _maxLengths[ charset ];
        }
    }

    internal class CharacterSet {
        public string Name;
        public int ByteCount;

        public CharacterSet( string name, int byteCount ) {
            this.Name = name;
            this.ByteCount = byteCount;
        }
    }
}