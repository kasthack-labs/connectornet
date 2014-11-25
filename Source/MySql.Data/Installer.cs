// Copyright © 2004, 2013, Oracle and/or its affiliates. All rights reserved.
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
using System.Collections;
using System.ComponentModel;
using System.Configuration.Install;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Permissions;
using System.Xml;
using Microsoft.Win32;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// We are adding a custom installer class to our assembly so our installer
    /// can make proper changes to the machine.config file.
    /// </summary>
    [RunInstaller( true )]
    [PermissionSet( SecurityAction.InheritanceDemand, Name = "FullTrust" )]
    [PermissionSet( SecurityAction.LinkDemand, Name = "FullTrust" )]
    public class CustomInstaller : Installer {
        /// <summary>
        /// We override Install so we can add our assembly to the proper
        /// machine.config files.
        /// </summary>
        /// <param name="stateSaver"></param>
        public override void Install( IDictionary stateSaver ) {
            base.Install( stateSaver );
            AddProviderToMachineConfig();
        }

        private static void AddProviderToMachineConfig() {
            var installRoot = Registry.GetValue( @"HKEY_LOCAL_MACHINE\Software\Microsoft\.NETFramework\", "InstallRoot", null );
            if ( installRoot == null ) throw new Exception( "Unable to retrieve install root for .NET framework" );
            UpdateMachineConfigs( installRoot.ToString(), true );

            var installRoot64 = installRoot.ToString();
            installRoot64 = installRoot64.Substring( 0, installRoot64.Length - 1 );
            installRoot64 = string.Format( "{0}64{1}", installRoot64, Path.DirectorySeparatorChar );
            if ( Directory.Exists( installRoot64 ) ) UpdateMachineConfigs( installRoot64, true );
        }

        internal static void UpdateMachineConfigs( string rootPath, bool add ) {
            var dirs = new[] { "v2.0.50727", "v4.0.30319" };
            foreach ( var frameworkDir in dirs ) {
                var path = rootPath + frameworkDir;

                var configPath = String.Format( @"{0}\CONFIG", path );
                if ( Directory.Exists( configPath ) )
                    if ( add ) AddProviderToMachineConfigInDir( configPath );
                    else RemoveProviderFromMachineConfigInDir( configPath );
            }
        }

        private static XmlElement CreateNodeAssemblyBindingRedirection(
            XmlElement mysqlNode,
            XmlDocument doc,
            string oldVersion,
            string newVersion ) {
            if ( doc == null
                 || mysqlNode == null ) return null;

            const string ns = "urn:schemas-microsoft-com:asm.v1";

            //mysql.data
            var dA = (XmlElement) doc.CreateNode( XmlNodeType.Element, "dependentAssembly", ns );
            var aI = (XmlElement) doc.CreateNode( XmlNodeType.Element, "assemblyIdentity", ns );
            aI.SetAttribute( "name", "MySql.Data" );
            aI.SetAttribute( "publicKeyToken", "c5687fc88969c44d" );
            aI.SetAttribute( "culture", "neutral" );

            var bR = (XmlElement) doc.CreateNode( XmlNodeType.Element, "bindingRedirect", ns );
            bR.SetAttribute( "oldVersion", oldVersion );
            bR.SetAttribute( "newVersion", newVersion );
            dA.AppendChild( aI );
            dA.AppendChild( bR );
            mysqlNode.AppendChild( dA );

            //mysql.data.entity
            dA = (XmlElement) doc.CreateNode( XmlNodeType.Element, "dependentAssembly", ns );
            aI = (XmlElement) doc.CreateNode( XmlNodeType.Element, "assemblyIdentity", ns );
            aI.SetAttribute( "name", "MySql.Data.Entity" );
            aI.SetAttribute( "publicKeyToken", "c5687fc88969c44d" );
            aI.SetAttribute( "culture", "neutral" );

            bR = (XmlElement) doc.CreateNode( XmlNodeType.Element, "bindingRedirect", ns );
            bR.SetAttribute( "oldVersion", oldVersion );
            bR.SetAttribute( "newVersion", newVersion );
            dA.AppendChild( aI );
            dA.AppendChild( bR );
            mysqlNode.AppendChild( dA );

            //mysql.web

            dA = (XmlElement) doc.CreateNode( XmlNodeType.Element, "dependentAssembly", ns );
            aI = (XmlElement) doc.CreateNode( XmlNodeType.Element, "assemblyIdentity", ns );
            aI.SetAttribute( "name", "MySql.Web" );
            aI.SetAttribute( "publicKeyToken", "c5687fc88969c44d" );
            aI.SetAttribute( "culture", "neutral" );

            bR = (XmlElement) doc.CreateNode( XmlNodeType.Element, "bindingRedirect", ns );
            bR.SetAttribute( "oldVersion", oldVersion );
            bR.SetAttribute( "newVersion", newVersion );
            dA.AppendChild( aI );
            dA.AppendChild( bR );
            mysqlNode.AppendChild( dA );

            return mysqlNode;
        }

        private static void AddProviderToMachineConfigInDir( string path ) {
            var configFile = String.Format( @"{0}\machine.config", path );
            if ( !File.Exists( configFile ) ) return;

            // now read the config file into memory
            var sr = new StreamReader( configFile );
            var configXml = sr.ReadToEnd();
            sr.Close();

            // load the XML into the XmlDocument
            var doc = new XmlDocument();
            doc.LoadXml( configXml );

            doc = RemoveOldBindingRedirection( doc );

            // create our new node
            var newNode = (XmlElement) doc.CreateNode( XmlNodeType.Element, "add", "" );

            // add the proper attributes
            newNode.SetAttribute( "name", "MySQL Data Provider" );
            newNode.SetAttribute( "invariant", "MySql.Data.MySqlClient" );
            newNode.SetAttribute( "description", ".Net Framework Data Provider for MySQL" );

            // add the type attribute by reflecting on the executing assembly
            var a = Assembly.GetExecutingAssembly();
            var type = String.Format( "MySql.Data.MySqlClient.MySqlClientFactory, {0}", a.FullName.Replace( "Installers", "Data" ) );
            newNode.SetAttribute( "type", type );

            var nodes = doc.GetElementsByTagName( "DbProviderFactories" );

            foreach ( XmlNode node in nodes[ 0 ].ChildNodes ) {
                if ( node.Attributes == null ) continue;
                if ( node.Attributes.Cast<XmlAttribute>().Any( attr => attr.Name == "invariant" && attr.Value == "MySql.Data.MySqlClient" ) ) {
                    nodes[ 0 ].RemoveChild( node );
                }
            }
            nodes[ 0 ].AppendChild( newNode );

            try {
                XmlElement mysqlNode;

                //add binding redirection to our assemblies
                if ( doc.GetElementsByTagName( "assemblyBinding" ).Count == 0 ) {
                    mysqlNode = (XmlElement) doc.CreateNode( XmlNodeType.Element, "assemblyBinding", "" );
                    mysqlNode.SetAttribute( "xmlns", "urn:schemas-microsoft-com:asm.v1" );
                }
                else mysqlNode = (XmlElement) doc.GetElementsByTagName( "assemblyBinding" )[ 0 ];

                var newVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString();
                mysqlNode = CreateNodeAssemblyBindingRedirection( mysqlNode, doc, "6.7.4.0", newVersion );

                var runtimeNode = doc.GetElementsByTagName( "runtime" );
                runtimeNode[ 0 ].AppendChild( mysqlNode );
            }
            catch {}

            // Save the document to a file and auto-indent the output.
            var writer = new XmlTextWriter( configFile, null );
            writer.Formatting = Formatting.Indented;
            doc.Save( writer );
            writer.Flush();
            writer.Close();
        }

        private static XmlDocument RemoveOldBindingRedirection( XmlDocument doc ) {
            if ( doc.GetElementsByTagName( "assemblyBinding" ).Count == 0 ) return doc;

            var nodesDependantAssembly = doc.GetElementsByTagName( "assemblyBinding" )[ 0 ].ChildNodes;
            if ( nodesDependantAssembly != null ) {
                var nodesCount = nodesDependantAssembly.Count;
                for ( var i = 0; i < nodesCount; i++ )
                    if ( nodesDependantAssembly[ 0 ].ChildNodes[ 0 ].Attributes[ 0 ].Name == "name"
                         && nodesDependantAssembly[ 0 ].ChildNodes[ 0 ].Attributes[ 0 ].Value.Contains( "MySql" ) ) doc.GetElementsByTagName( "assemblyBinding" )[ 0 ].RemoveChild( nodesDependantAssembly[ 0 ] );
            }
            return doc;
        }

        /// <summary>
        /// We override Uninstall so we can remove out assembly from the
        /// machine.config files.
        /// </summary>
        /// <param name="savedState"></param>
        public override void Uninstall( IDictionary savedState ) {
            base.Uninstall( savedState );
            RemoveProviderFromMachineConfig();
        }

        private static void RemoveProviderFromMachineConfig() {
            var installRoot = Registry.GetValue( @"HKEY_LOCAL_MACHINE\Software\Microsoft\.NETFramework\", "InstallRoot", null );
            if ( installRoot == null ) throw new Exception( "Unable to retrieve install root for .NET framework" );
            UpdateMachineConfigs( installRoot.ToString(), false );

            var installRoot64 = installRoot.ToString();
            installRoot64 = installRoot64.Substring( 0, installRoot64.Length - 1 );
            installRoot64 = string.Format( "{0}64{1}", installRoot64, Path.DirectorySeparatorChar );
            if ( Directory.Exists( installRoot64 ) ) UpdateMachineConfigs( installRoot64, false );
        }

        private static void RemoveProviderFromMachineConfigInDir( string path ) {
            var configFile = String.Format( @"{0}\machine.config", path );
            if ( !File.Exists( configFile ) ) return;

            // now read the config file into memory
            var sr = new StreamReader( configFile );
            var configXml = sr.ReadToEnd();
            sr.Close();

            // load the XML into the XmlDocument
            var doc = new XmlDocument();
            doc.LoadXml( configXml );

            var nodes = doc.GetElementsByTagName( "DbProviderFactories" );
            foreach ( XmlNode node in nodes[ 0 ].ChildNodes ) {
                if ( node.Attributes == null ) continue;
                var name = node.Attributes[ "name" ].Value;
                if ( name == "MySQL Data Provider" ) {
                    nodes[ 0 ].RemoveChild( node );
                    break;
                }
            }

            try {
                doc = RemoveOldBindingRedirection( doc );
            }
            catch {}

            // Save the document to a file and auto-indent the output.
            var writer = new XmlTextWriter( configFile, null );
            writer.Formatting = Formatting.Indented;
            doc.Save( writer );
            writer.Flush();
            writer.Close();
        }
    }
}