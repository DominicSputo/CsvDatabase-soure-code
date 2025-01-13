using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace CsvDatabase
{
    public class XmlToCsv
    {

        /// <summary>
        /// Converts a XML File to CSV File. Does not delete XML File. For less than 4GB XML Files. Appends data to CSV File if CSV File exist with data
        /// </summary>
        /// <param name="XML_File">XML File to write to CSV File</param>
        /// <param name="CSV_File">CSV File to create using XML File data</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | </param>
        /// <param name="CSV_TimeOut">Number of Seconds to add a record to CSV File. Default 5 seconds.</param>
        public static void WriteCsv(string XML_File, string CSV_File, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            DataSet ds = new DataSet();
            ds.ReadXml(XML_File);
            CsvBulk.Insert(ds.Tables[0], CSV_File, CSV_Delimiter);
        }

        private static bool _bFileNotFound = false;
        private class Property
        {
            public string Column { get; set; }
            public string Value { get; set; }
        }
        /// <summary>
        /// Creates the CSV File if does not exist.
        /// </summary>
        /// <param name="CSV_File">CSV File to create</param>
        /// <param name="CSV_TempFile">CSV Temp File to check for</param>
        private static void CreateCsvFile(string CSV_File, string CSV_TempFile)
        {
            DateTime dtStart = DateTime.Now;
            FileStream fs = null;
            while (true)
            {
                if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                {
                    break;
                }
                else
                {
                    if (!File.Exists(CSV_File))
                    {
                        try
                        {
                            fs = new FileStream(CSV_File, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                            break;
                        }
                        catch (Exception ex)
                        {
                        }
                        finally
                        {
                            if (fs != null)
                            {
                                fs.Dispose();
                            }
                        }
                    }
                    else
                    {   //CSV File Already Exists
                        break;
                    }
                }
            }
        }

        private static void FindDelimiter(string sLine, ref char cDelimiter)
        {
            if (sLine.IndexOf(cDelimiter) == -1)
            {
                char[] carDelimiters = new char[] { '\t', '|', ',', '^', ';', '<', '>', ':', '~', '\\', '/', '*', '-' };
                int iDelimitersLength = carDelimiters.Length;
                for (int i = 0; i < iDelimitersLength; i++)
                {
                    if (sLine.IndexOf(carDelimiters[i]) != -1)
                    {
                        cDelimiter = carDelimiters[i];
                        return;
                    }
                }
            }
        }

        ///// <summary>
        ///// Converts a XML File to CSV File. Does not delete XML File. For greater than 4GB XML Files. Appends data to CSV File if CSV File exist with data
        ///// </summary>
        ///// <param name="XML_File">XML File to write to CSV File</param>
        ///// <param name="CSV_Header">Use XML File elements for CSV_Header.</param>
        ///// <param name="CSV_File">CSV File to create using XML File data</param>
        ///// <param name="CSV_Delimiter">The CSV Delimiter used. Default |</param>
        ///// <param name="CSV_TimeOut">Number of Seconds to add a record to CSV File. Default 5 seconds.</param>
        public static void WriteCsv(string XML_File, string[] CSV_Header, string CSV_File, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {            
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);            
            bool bFileLocked = true;
            FileStream fsTempFile1 = null;
            FileStream fsTempFile2 = null;
            StreamReader srTempFile1 = null;
            FileStream fs = null;
            StreamWriter sw = null;
            StreamReader sr = null;
            bFileLocked = true;
            string[] sarHeaderRow = null;
            bool bNoHeaderRow = true;
            string sHeaderRow = "";

            int iHeaderLength = CSV_Header.Length;
            FileStream fsReadCM = null;
            XmlReader readerCM = null;
            fsReadCM = new FileStream(XML_File, FileMode.Open);
            readerCM = XmlReader.Create(fsReadCM);

            while (bFileLocked)
            {
                try
                {
                    _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile, CSV_TempFile2);
                    if (_bFileNotFound)
                    {
                        CreateCsvFile(CSV_File, CSV_TempFile);
                    }
                    File.Move(CSV_File, CSV_TempFile);
                    fsTempFile1 = new FileStream(CSV_TempFile, FileMode.Open, FileAccess.Read, FileShare.None);
                    srTempFile1 = new StreamReader(fsTempFile1);
                    fsTempFile2 = new FileStream(CSV_TempFile2, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fsTempFile2);
                    while (!srTempFile1.EndOfStream)
                    {
                        string sLine = (srTempFile1.ReadLine() + "").Trim();
                        if (sLine != "")
                        {
                            if (bNoHeaderRow)
                            {
                                sHeaderRow = sLine;
                                FindDelimiter(sHeaderRow, ref CSV_Delimiter);
                                bNoHeaderRow = false;
                            }
                            sw.WriteLine(sLine);
                        }
                    }
                    sw.Flush();
                    if (sw != null)
                    {
                        sw.Dispose();
                        sw = null;
                    }
                    if (fsTempFile2 != null)
                    {
                        fsTempFile2.Dispose();
                        fsTempFile2 = null;
                    }
                    sarHeaderRow = sHeaderRow.ToUpper().Split(CSV_Delimiter);
                    int iHeaderRowLength = sarHeaderRow.Length;
                    bool bHeader = true;
                    if (bNoHeaderRow)
                    { //Empty CSV File
                        bFileLocked = true;
                        while (bFileLocked)
                        {
                            try
                            {
                                string sHeader = "";
                                fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                sw = new StreamWriter(fs);

                                string sLine = "";
                                while (readerCM.Read())
                                {
                                    // Only detect start elements.
                                    if (readerCM.IsStartElement())
                                    {
                                        for (int i = 0; i < iHeaderLength; i++)
                                        {
                                            if (readerCM.Name.ToUpper() == CSV_Header[i].ToUpper())
                                            {
                                                if (readerCM.Read())
                                                {
                                                    sLine = sLine + (readerCM.Value + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                    if (bHeader)
                                                    {
                                                        sHeader = sHeader + CSV_Header[i].ToUpper() + CSV_Delimiter;
                                                    }
                                                    if (i == (iHeaderLength - 1))
                                                    {
                                                        if (!bHeader)
                                                        {
                                                            sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));
                                                        }
                                                        else if (bHeader)
                                                        {
                                                            sw.WriteLine(sHeader.Substring(0, (sHeader.Length - 1)));
                                                            sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));
                                                            bHeader = false;
                                                        }
                                                        sLine = "";
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if (readerCM != null)
                                {
                                    readerCM.Dispose();
                                }
                                if (fsReadCM != null)
                                {
                                    fsReadCM.Dispose();
                                }
                                sw.Flush();
                                bFileLocked = false;
                            }
                            catch
                            {
                                bHeader = true;
                                bFileLocked = true;
                            }
                            finally
                            {
                                if (sw != null)
                                {
                                    sw.Dispose();
                                }
                                if (fs != null)
                                {
                                    fs.Dispose();
                                }
                            }
                        }
                        bFileLocked = true;
                        while (bFileLocked)
                        {
                            try
                            {
                                if (File.Exists(CSV_TempFile2))
                                {
                                    File.Move(CSV_TempFile2, CSV_File);
                                }
                                bFileLocked = false;
                            }
                            catch (Exception ex)
                            {
                                bFileLocked = true;
                            }
                        }
                    }
                    else
                    { //CSV File Not Empty
                        List<Property> lstInsertHeader = new List<Property>();
                        for (int i = 0; i < iHeaderLength; i++)
                        {
                            lstInsertHeader.Add(new Property { Column = CSV_Header[i].ToUpper() });
                        }
                        int iInsertHeaderLength = lstInsertHeader.Count();
                        bool bPerfectMatchingHeader = true;
                        bool bSimilarMatchingHeader = true;
                        if (iHeaderRowLength == iInsertHeaderLength)
                        {
                            for (int i = 0; i < iHeaderRowLength; i++)
                            {
                                for (int x = 0; x < iInsertHeaderLength; x++)
                                {
                                    if (i == x)
                                    {
                                        if (sarHeaderRow[i] != lstInsertHeader[x].Column)
                                        {                                           //Not a perfect match
                                            bPerfectMatchingHeader = false;
                                        }
                                    }
                                    if (i == (iHeaderRowLength - 1))
                                    {
                                        if (sarHeaderRow[i] != lstInsertHeader[x].Column)
                                        {                                           //Did not find a similar match
                                            bSimilarMatchingHeader = false;
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            bPerfectMatchingHeader = false;
                            bSimilarMatchingHeader = false;
                        }
                        if (bPerfectMatchingHeader)
                        {
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fs);
                                    string sLine = "";
                                    while (readerCM.Read())
                                    {
                                        // Only detect start elements.
                                        if (readerCM.IsStartElement())
                                        {
                                            for (int i = 0; i < iHeaderLength; i++)
                                            {
                                                if (readerCM.Name.ToUpper() == CSV_Header[i].ToUpper())
                                                {
                                                    if (readerCM.Read())
                                                    {
                                                        sLine = sLine + (readerCM.Value + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;                                                       
                                                        if (i == (iHeaderLength - 1))
                                                        {
                                                            sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));                                                            
                                                            sLine = "";
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (readerCM != null)
                                    {
                                        readerCM.Dispose();
                                    }
                                    if (fsReadCM != null)
                                    {
                                        fsReadCM.Dispose();
                                    }
                                    sw.Flush();
                                    bFileLocked = false;
                                }
                                catch (Exception ex)
                                {
                                    bFileLocked = true;
                                }
                                finally
                                {
                                    if (sw != null)
                                    {
                                        sw.Dispose();
                                    }
                                    if (fs != null)
                                    {
                                        fs.Dispose();
                                    }
                                }
                            }
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    if (File.Exists(CSV_TempFile2))
                                    {
                                        File.Move(CSV_TempFile2, CSV_File);
                                    }
                                    bFileLocked = false;
                                }
                                catch (Exception ex)
                                {
                                    bFileLocked = true;
                                }
                            }
                        }
                        else if (bSimilarMatchingHeader)
                        {
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fs);
                                    string sLine = "";
                                    List<Property> lstProperty = new List<Property>();                                   
                                    while (readerCM.Read())
                                    {
                                        // Only detect start elements.
                                        if (readerCM.IsStartElement())
                                        {
                                            for (int i = 0; i < iHeaderLength; i++)
                                            {                                                
                                                if (readerCM.Name.ToUpper() == CSV_Header[i].ToUpper())
                                                {                                                    
                                                    if (readerCM.Read())
                                                    {
                                                        lstProperty.Add(new Property { Column = CSV_Header[i].ToUpper(), Value = (readerCM.Value + "").Replace(CSV_Delimiter.ToString(), "") });
                                                        if (i == (iHeaderLength - 1))
                                                        {
                                                            int iPropertyLength = lstProperty.Count();
                                                            for (int x = 0; x < iPropertyLength; x++)
                                                            {
                                                                for (int r = 0; r < iHeaderRowLength; r++)
                                                                {
                                                                    if (sarHeaderRow[r] == lstProperty[x].Column)
                                                                    {
                                                                        sLine = sLine + lstProperty[x].Value + CSV_Delimiter;
                                                                    }
                                                                }
                                                            }
                                                            sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));
                                                            sLine = "";
                                                            lstProperty = new List<Property>();
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (readerCM != null)
                                    {
                                        readerCM.Dispose();
                                    }
                                    if (fsReadCM != null)
                                    {
                                        fsReadCM.Dispose();
                                    }
                                    sw.Flush();
                                    bFileLocked = false;
                                }
                                catch
                                {
                                    bFileLocked = true;
                                }
                                finally
                                {
                                    if (sw != null)
                                    {
                                        sw.Dispose();
                                    }
                                    if (fs != null)
                                    {
                                        fs.Dispose();
                                    }
                                }
                            }
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    if (File.Exists(CSV_TempFile2))
                                    {
                                        File.Move(CSV_TempFile2, CSV_File);
                                    }
                                    bFileLocked = false;
                                }
                                catch (Exception ex)
                                {
                                    bFileLocked = true;
                                }
                            }
                        }
                        else
                        { //Not Completely Similar or a Complete Match
                            string sNewHeaderRow = sHeaderRow;
                            string sAppendHeader = "";
                            string sToAppendToExistingRows = "";
                            for (int x = 0; x < iInsertHeaderLength; x++)
                            {
                                for (int i = 0; i < iHeaderRowLength; i++)
                                {
                                    if (lstInsertHeader[x].Column == sarHeaderRow[i])
                                    {
                                        break;
                                    }
                                    else if (i == (iHeaderRowLength - 1))
                                    {
                                        sToAppendToExistingRows = sToAppendToExistingRows + CSV_Delimiter;
                                        sAppendHeader = sAppendHeader + CSV_Delimiter + lstInsertHeader[x].Column;
                                    }
                                }
                            }
                            sNewHeaderRow = sNewHeaderRow + sAppendHeader;
                            FileStream fsTemp = null;
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fsTemp = new FileStream(CSV_TempFile2, FileMode.Open, FileAccess.Read, FileShare.None);
                                    sr = new StreamReader(fsTemp);
                                    fs = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fs);
                                    bHeader = true;
                                    while (!sr.EndOfStream)
                                    {
                                        string sLine1 = (sr.ReadLine() + "").Trim();
                                        if (sLine1 != "")
                                        {
                                            if (!bHeader)
                                            {
                                                sw.WriteLine(sLine1 + sToAppendToExistingRows);
                                            }
                                            else
                                            { //First Row AKA Header Row
                                                sw.WriteLine(sNewHeaderRow);
                                                bHeader = false;
                                            }
                                        }
                                    }
                                    string[] sarNewHeaderRow = sNewHeaderRow.Split(CSV_Delimiter);
                                    int iNewHeaderRow = sarNewHeaderRow.Length;
                                    string sLine = "";
                                    List<Property> lstProperty = new List<Property>();
                                    while (readerCM.Read())
                                    {
                                        // Only detect start elements.
                                        if (readerCM.IsStartElement())
                                        {
                                            for (int i = 0; i < iHeaderLength; i++)
                                            {
                                                if (readerCM.Name.ToUpper() == CSV_Header[i].ToUpper())
                                                {
                                                    if (readerCM.Read())
                                                    {
                                                        lstProperty.Add(new Property { Column = CSV_Header[i].ToUpper(), Value = (readerCM.Value + "").Replace(CSV_Delimiter.ToString(), "") });
                                                        if (i == (iHeaderLength - 1))
                                                        {
                                                            int iPropertyLength = lstProperty.Count();
                                                            for (int r = 0; r < iNewHeaderRow; r++)
                                                            {
                                                                for (int x = 0; x < iPropertyLength; x++)
                                                                {
                                                                    if (sarNewHeaderRow[r] == lstProperty[x].Column)
                                                                    {
                                                                        sLine = sLine + lstProperty[x].Value + CSV_Delimiter;
                                                                        break;
                                                                    }
                                                                    else if (x == (iPropertyLength - 1))
                                                                    {
                                                                        sLine = sLine + CSV_Delimiter;
                                                                    }
                                                                }
                                                            }
                                                            sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));
                                                            sLine = "";
                                                            lstProperty = new List<Property>();
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (readerCM != null)
                                    {
                                        readerCM.Dispose();
                                    }
                                    if (fsReadCM != null)
                                    {
                                        fsReadCM.Dispose();
                                    }
                                    sw.Flush();
                                    bFileLocked = false;
                                }
                                catch (Exception ex)
                                {
                                    bFileLocked = true;
                                }
                                finally
                                {
                                    if (sr != null)
                                    {
                                        sr.Dispose();
                                    }
                                    if (sw != null)
                                    {
                                        sw.Dispose();
                                    }
                                    if (fsTemp != null)
                                    {
                                        fsTemp.Dispose();
                                    }
                                    if (fs != null)
                                    {
                                        fs.Dispose();
                                    }
                                }
                            }
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    if (File.Exists(CSV_TempFile2))
                                    {
                                        File.Delete(CSV_TempFile2);
                                    }
                                    bFileLocked = false;
                                }
                                catch (Exception ex)
                                {
                                    bFileLocked = true;
                                }
                            }
                        }
                    }
                    bFileLocked = false;
                }
                catch (Exception ex)
                {
                    bFileLocked = true;
                }
                finally
                {
                    if (srTempFile1 != null)
                    {
                        srTempFile1.Dispose();
                    }
                    if (fsTempFile1 != null)
                    {
                        fsTempFile1.Dispose();
                    }
                }
            }

            bFileLocked = true;
            while (bFileLocked)
            {
                try
                {
                    if (File.Exists(CSV_TempFile) == true)
                    {
                        File.Delete(CSV_TempFile);
                    }
                    bFileLocked = false;
                }
                catch (Exception ex)
                {
                    bFileLocked = true;
                }
            }
        }


    }
}
