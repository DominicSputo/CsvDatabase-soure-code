using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace CsvDatabase
{
    public static class CsvToXml
    {

        /// <summary>
        /// Writes a XML File using the CSV File Data. Note Overwrites old file.
        /// </summary>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>        
        /// <param name="Xml_File">The XML FileName and Path to write to.</param>
        /// <param name="CSV_Filter">The CSV Filter to use. Example FirstName LIKE Domini | LastName LIKE Sput. FirstName = Dominic. FirstName IN Dominic | Rob | John</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="CSV_TimeOut">Number of Seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        public static void WriteXml(string CSV_File, string Xml_File, string CSV_Filter = "", char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            Regex rgx = new Regex("[^a-zA-Z0-9]");
            string sTableName = rgx.Replace(Path.GetFileNameWithoutExtension(Xml_File), "");
            XmlWriterSettings xmlWriterSettings = new XmlWriterSettings();
            xmlWriterSettings.Indent = true;
            xmlWriterSettings.NewLineOnAttributes = true;
            FileStream fsWriteCM = null;
            XmlWriter xmlWriterCM = null;
            try
            {
                CsvDataReader dtr = new CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, true, CSV_TimeOut);
                int iFieldCount = dtr.FieldCount;
                fsWriteCM = new FileStream(Xml_File, FileMode.Create);
                xmlWriterCM = XmlWriter.Create(fsWriteCM, xmlWriterSettings);
                xmlWriterCM.WriteStartDocument();
                xmlWriterCM.WriteStartElement("DATA");
                while (dtr.Read())
                {
                    for (int i = 0; i < iFieldCount; i++)
                    {
                        if (i == 0)
                        {
                            xmlWriterCM.WriteStartElement(sTableName);
                            xmlWriterCM.WriteElementString(dtr.GetName(i), dtr[i] + "");
                        }
                        else if (i == (iFieldCount - 1))
                        {
                            xmlWriterCM.WriteElementString(dtr.GetName(i), dtr[i] + "");
                            xmlWriterCM.WriteEndElement();
                        }
                        else
                        {
                            xmlWriterCM.WriteElementString(dtr.GetName(i), dtr[i] + "");
                        }
                    }
                }
                xmlWriterCM.WriteEndElement();
                xmlWriterCM.WriteEndDocument();
                xmlWriterCM.Flush();

            }
            catch (Exception ex)
            {

            }
            finally
            {
                if (xmlWriterCM != null)
                {
                    xmlWriterCM.Dispose();
                }
                if (fsWriteCM != null)
                {
                    fsWriteCM.Dispose();
                }
            }
        }

        /// <summary>
        /// Writes a XML File using a DataReader. Note Overwrites old file.
        /// </summary>
        /// <param name="DataReader">The DataReader</param>
        /// <param name="Xml_File">The XML FileName and Path to write to.</param>      
        public static void WriteXml(this IDataReader DataReader, string Xml_File)
        {
            Regex rgx = new Regex("[^a-zA-Z0-9]");
            string sTableName = rgx.Replace(Path.GetFileNameWithoutExtension(Xml_File), "");
            XmlWriterSettings xmlWriterSettings = new XmlWriterSettings();
            xmlWriterSettings.Indent = true;
            xmlWriterSettings.NewLineOnAttributes = true;
            FileStream fsWriteCM = null;
            XmlWriter xmlWriterCM = null;
            try
            {
                int iFieldCount = DataReader.FieldCount;
                fsWriteCM = new FileStream(Xml_File, FileMode.Create);
                xmlWriterCM = XmlWriter.Create(fsWriteCM, xmlWriterSettings);
                xmlWriterCM.WriteStartDocument();
                xmlWriterCM.WriteStartElement("DATA");
                while (DataReader.Read())
                {
                    for (int i = 0; i < iFieldCount; i++)
                    {
                        if (i == 0)
                        {
                            xmlWriterCM.WriteStartElement(sTableName);
                            xmlWriterCM.WriteElementString(DataReader.GetName(i), DataReader[i] + "");
                        }
                        else if (i == (iFieldCount - 1))
                        {
                            xmlWriterCM.WriteElementString(DataReader.GetName(i), DataReader[i] + "");
                            xmlWriterCM.WriteEndElement();
                        }
                        else
                        {
                            xmlWriterCM.WriteElementString(DataReader.GetName(i), DataReader[i] + "");
                        }
                    }
                }
                xmlWriterCM.WriteEndElement();
                xmlWriterCM.WriteEndDocument();
                xmlWriterCM.Flush();
            }
            catch (Exception ex)
            {

            }
            finally
            {
                if (xmlWriterCM != null)
                {
                    xmlWriterCM.Dispose();
                }
                if (fsWriteCM != null)
                {
                    fsWriteCM.Dispose();
                }
            }
        }

    }
}
