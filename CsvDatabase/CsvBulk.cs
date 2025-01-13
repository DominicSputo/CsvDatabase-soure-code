using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Collections;
using System.Threading.Tasks;
using System.Data;

namespace CsvDatabase
{
    public static class CsvBulk
    {
        private static bool _bFileNotFound = false;

        private class Property
        {
            public string Column { get; set; }
            public string Value { get; set; }
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

        /// <summary>
        /// Creates CSV if File Does NOT Exist. Bulk Insert into CSV. CSV_InsertList can match CSV Header property names. None matching CSV_InsertList Properties will be created as New Header Columns to Insert into CSV File.
        /// </summary>
        /// <param name="CSV_InsertList">The DataTable to Bulk Insert Into CSV File. DataTable can contain Column Names that match CSV Header. None matching DataTable Column Names will be created as New Header Columns to Insert into CSV File.</param>
        /// <param name="CSV_File">The CSV File to Bulk Insert into.</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Multiple Delimiters will try to be found if no match.</param>
        public static void Insert(DataTable CSV_InsertDataTable, string CSV_File, char CSV_Delimiter = '|')
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
            int iDataTableColumnCount = CSV_InsertDataTable.Columns.Count;
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
                            sw.WriteLine(sLine.Replace("\r", "").Replace("\n", ""));
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
                                //foreach (var record in CSV_InsertList)
                                foreach (DataRow dr in CSV_InsertDataTable.Rows)
                                {
                                    string sLine = "";
                                    for (int i = 0; i < iDataTableColumnCount; i++)
                                    {
                                        sLine = sLine + (dr[i] + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        if (bHeader)
                                        {
                                            sHeader = sHeader + CSV_InsertDataTable.Columns[i].ColumnName.ToUpper() + CSV_Delimiter;
                                        }
                                    }
                                    if (!bHeader)
                                    {
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                    }
                                    else if (bHeader)
                                    {
                                        sw.WriteLine(sHeader.Substring(0, (sHeader.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                        bHeader = false;
                                    }
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
                                if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                {
                                    File.Delete(CSV_File);
                                }
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
                        for (int i = 0; i < iDataTableColumnCount; i++)
                        {
                            lstInsertHeader.Add(new Property { Column = CSV_InsertDataTable.Columns[i].ColumnName.ToUpper() });
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
                                    foreach (DataRow dr in CSV_InsertDataTable.Rows)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        for (int i = 0; i < iDataTableColumnCount; i++)
                                        {
                                            sLine = sLine + (dr[i] + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        }
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
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
                                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                    {
                                        File.Delete(CSV_File);
                                    }
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
                                    foreach (DataRow dr in CSV_InsertDataTable.Rows)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        for (int i = 0; i < iDataTableColumnCount; i++)
                                        {
                                            lstProperty.Add(new Property { Column = CSV_InsertDataTable.Columns[i].ColumnName.ToUpper(), Value = (dr[i] + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int x = 0; x < iPropertyLength; x++)
                                        {
                                            for (int i = 0; i < iHeaderRowLength; i++)
                                            {
                                                if (sarHeaderRow[i] == lstProperty[x].Column)
                                                {
                                                    sLine = sLine + lstProperty[x].Value + CSV_Delimiter;
                                                }
                                            }
                                        }
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
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
                                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                    {
                                        File.Delete(CSV_File);
                                    }
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
                                        string sLine = (sr.ReadLine() + "").Trim();
                                        if (sLine != "")
                                        {
                                            if (!bHeader)
                                            {
                                                sw.WriteLine((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                            }
                                            else
                                            { //First Row AKA Header Row
                                                sw.WriteLine(sNewHeaderRow.Replace("\r", "").Replace("\n", ""));
                                                bHeader = false;
                                            }
                                        }
                                    }
                                    string[] sarNewHeaderRow = sNewHeaderRow.Split(CSV_Delimiter);
                                    int iNewHeaderRow = sarNewHeaderRow.Length;
                                    foreach (DataRow dr in CSV_InsertDataTable.Rows)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        for (int i = 0; i < iDataTableColumnCount; i++)
                                        {
                                            lstProperty.Add(new Property { Column = CSV_InsertDataTable.Columns[i].ColumnName.ToUpper(), Value = (dr[i] + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int i = 0; i < iNewHeaderRow; i++)
                                        {
                                            for (int x = 0; x < iPropertyLength; x++)
                                            {
                                                if (sarNewHeaderRow[i] == lstProperty[x].Column)
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
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
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

        /// <summary>
        /// Creates CSV if File Does NOT Exist. Bulk Insert into CSV. CSV_InsertList can match CSV Header property names. None matching CSV_InsertList Properties will be created as New Header Columns to Insert into CSV File.
        /// </summary>
        /// <param name="CSV_InsertList">The IEnumerable Class List to Bulk Insert Into CSV File. IEnumerable Class List can contain Property names that match CSV Header. None matching CSV_InsertList Properties will be created as New Header Columns to Insert into CSV File.</param>
        /// <param name="CSV_File">The CSV File to Bulk Insert into.</param>
        /// <param name="CSV_TimeOut">Time to Check to Create New CSV File</param> 
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Multiple Delimiters will try to be found if no match.</param>
        public static void Insert(IEnumerable CSV_InsertList, string CSV_File, char CSV_Delimiter = '|')
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
                            sw.WriteLine(sLine.Replace("\r", "").Replace("\n", ""));
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
                                foreach (var record in CSV_InsertList)
                                {
                                    string sLine = "";
                                    foreach (PropertyInfo p in record.GetType().GetProperties()) //BindingFlags.Public | BindingFlags.Static
                                    {
                                        sLine = sLine + (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        if (bHeader)
                                        {
                                            sHeader = sHeader + p.Name.ToUpper() + CSV_Delimiter;
                                        }
                                    }
                                    if (!bHeader)
                                    {
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                    }
                                    else if (bHeader)
                                    {
                                        sw.WriteLine(sHeader.Substring(0, (sHeader.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                        bHeader = false;
                                    }
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
                                if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                {
                                    File.Delete(CSV_File);
                                }
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
                        foreach (var record in CSV_InsertList)
                        {
                            foreach (PropertyInfo p in record.GetType().GetProperties()) //BindingFlags.Public | BindingFlags.Static
                            {
                                lstInsertHeader.Add(new Property { Column = p.Name.ToUpper() });
                            }
                            break;
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
                                    foreach (var record in CSV_InsertList)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        foreach (PropertyInfo p in record.GetType().GetProperties()) //BindingFlags.Public | BindingFlags.Static
                                        {
                                            sLine = sLine + (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        }
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
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
                                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                    {
                                        File.Delete(CSV_File);
                                    }
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
                                    foreach (var record in CSV_InsertList)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        foreach (PropertyInfo p in record.GetType().GetProperties())
                                        {
                                            lstProperty.Add(new Property { Column = p.Name.ToUpper(), Value = (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int x = 0; x < iPropertyLength; x++)
                                        {
                                            for (int i = 0; i < iHeaderRowLength; i++)
                                            {
                                                if (sarHeaderRow[i] == lstProperty[x].Column)
                                                {
                                                    sLine = sLine + lstProperty[x].Value + CSV_Delimiter;
                                                }
                                            }
                                        }
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
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
                                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                    {
                                        File.Delete(CSV_File);
                                    }
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
                                        string sLine = (sr.ReadLine() + "").Trim();
                                        if (sLine != "")
                                        {
                                            if (!bHeader)
                                            {
                                                sw.WriteLine((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                            }
                                            else
                                            { //First Row AKA Header Row
                                                sw.WriteLine(sNewHeaderRow.Replace("\r", "").Replace("\n", ""));
                                                bHeader = false;
                                            }
                                        }
                                    }
                                    string[] sarNewHeaderRow = sNewHeaderRow.Split(CSV_Delimiter);
                                    int iNewHeaderRow = sarNewHeaderRow.Length;
                                    foreach (var record in CSV_InsertList)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        foreach (PropertyInfo p in record.GetType().GetProperties())
                                        {
                                            lstProperty.Add(new Property { Column = p.Name.ToUpper(), Value = (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int i = 0; i < iNewHeaderRow; i++)
                                        {
                                            for (int x = 0; x < iPropertyLength; x++)
                                            {
                                                if (sarNewHeaderRow[i] == lstProperty[x].Column)
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
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
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

        /// <summary>
        /// Creates CSV if File Does NOT Exist Async. Bulk Insert into CSV. CSV_InsertList can match CSV Header property names. None matching CSV_InsertList Properties will be created as New Header Columns to Insert into CSV File.
        /// </summary>
        /// <param name="CSV_InsertList">The IEnumerable Class List to Bulk Insert Into CSV File. IEnumerable Class List can contain Property names that match CSV Header. None matching CSV_InsertList Properties will be created as New Header Columns to Insert into CSV File.</param>
        /// <param name="CSV_File">The CSV File to Bulk Insert into.</param>
        /// <param name="CSV_TimeOut">Time to Check to Create New CSV File</param> 
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Multiple Delimiters will try to be found if no match.</param>
        public static async Task InsertAsync(IEnumerable CSV_InsertList, string CSV_File, char CSV_Delimiter = '|')
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
                        string sLine = (await srTempFile1.ReadLineAsync() + "").Trim();
                        if (sLine != "")
                        {
                            if (bNoHeaderRow)
                            {
                                sHeaderRow = sLine;
                                FindDelimiter(sHeaderRow, ref CSV_Delimiter);
                                bNoHeaderRow = false;
                            }
                            await sw.WriteLineAsync(sLine.Replace("\r", "").Replace("\n", ""));
                        }
                    }
                    await sw.FlushAsync();
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
                                foreach (var record in CSV_InsertList)
                                {
                                    string sLine = "";
                                    foreach (PropertyInfo p in record.GetType().GetProperties()) //BindingFlags.Public | BindingFlags.Static
                                    {
                                        sLine = sLine + (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        if (bHeader)
                                        {
                                            sHeader = sHeader + p.Name.ToUpper() + CSV_Delimiter;
                                        }
                                    }
                                    if (!bHeader)
                                    {
                                        await sw.WriteLineAsync(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                    }
                                    else if (bHeader)
                                    {
                                        await sw.WriteLineAsync(sHeader.Substring(0, (sHeader.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                        await sw.WriteLineAsync(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                        bHeader = false;
                                    }
                                }
                                await sw.FlushAsync();
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
                        while (true)
                        {
                            try
                            {
                                if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                {
                                    File.Delete(CSV_File);
                                }
                                if (File.Exists(CSV_TempFile2))
                                {
                                    File.Move(CSV_TempFile2, CSV_File);
                                }
                                break;
                            }
                            catch (Exception ex)
                            {
                            }
                        }
                    }
                    else
                    { //CSV File Not Empty
                        List<Property> lstInsertHeader = new List<Property>();
                        foreach (var record in CSV_InsertList)
                        {
                            foreach (PropertyInfo p in record.GetType().GetProperties()) //BindingFlags.Public | BindingFlags.Static
                            {
                                lstInsertHeader.Add(new Property { Column = p.Name.ToUpper() });
                            }
                            break;
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
                                    foreach (var record in CSV_InsertList)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        foreach (PropertyInfo p in record.GetType().GetProperties()) //BindingFlags.Public | BindingFlags.Static
                                        {
                                            sLine = sLine + (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        }
                                        await sw.WriteLineAsync(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                    }
                                    await sw.FlushAsync();
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
                            while (true)
                            {
                                try
                                {
                                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                    {
                                        File.Delete(CSV_File);
                                    }
                                    if (File.Exists(CSV_TempFile2))
                                    {
                                        File.Move(CSV_TempFile2, CSV_File);
                                    }
                                    break;
                                }
                                catch (Exception ex)
                                {
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
                                    foreach (var record in CSV_InsertList)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        foreach (PropertyInfo p in record.GetType().GetProperties())
                                        {
                                            lstProperty.Add(new Property { Column = p.Name.ToUpper(), Value = (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int x = 0; x < iPropertyLength; x++)
                                        {
                                            for (int i = 0; i < iHeaderRowLength; i++)
                                            {
                                                if (sarHeaderRow[i] == lstProperty[x].Column)
                                                {
                                                    sLine = sLine + lstProperty[x].Value + CSV_Delimiter;
                                                }
                                            }
                                        }
                                        await sw.WriteLineAsync(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                    }
                                    await sw.FlushAsync();
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
                            while (true)
                            {
                                try
                                {
                                    if (File.Exists(CSV_File) == true && File.Exists(CSV_TempFile2) == true)
                                    {
                                        File.Delete(CSV_File);
                                    }
                                    if (File.Exists(CSV_TempFile2))
                                    {
                                        File.Move(CSV_TempFile2, CSV_File);
                                    }
                                    break;
                                }
                                catch (Exception ex)
                                {
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
                                        string sLine = (await sr.ReadLineAsync() + "").Trim();
                                        if (sLine != "")
                                        {
                                            if (!bHeader)
                                            {
                                                await sw.WriteLineAsync((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                            }
                                            else
                                            { //First Row AKA Header Row
                                                await sw.WriteLineAsync(sNewHeaderRow.Replace("\r", "").Replace("\n", ""));
                                                bHeader = false;
                                            }
                                        }
                                    }
                                    string[] sarNewHeaderRow = sNewHeaderRow.Split(CSV_Delimiter);
                                    int iNewHeaderRow = sarNewHeaderRow.Length;
                                    foreach (var record in CSV_InsertList)
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        foreach (PropertyInfo p in record.GetType().GetProperties())
                                        {
                                            lstProperty.Add(new Property { Column = p.Name.ToUpper(), Value = (p.GetValue(record) + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int i = 0; i < iNewHeaderRow; i++)
                                        {
                                            for (int x = 0; x < iPropertyLength; x++)
                                            {
                                                if (sarNewHeaderRow[i] == lstProperty[x].Column)
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
                                        await sw.WriteLineAsync(sLine.Substring(0, (sLine.Length - 1)).Replace("\r", "").Replace("\n", ""));
                                    }
                                    await sw.FlushAsync();
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
                            while (true)
                            {
                                try
                                {
                                    if (File.Exists(CSV_TempFile2))
                                    {
                                        File.Delete(CSV_TempFile2);
                                    }
                                    break;
                                }
                                catch (Exception ex)
                                {
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
            while (true)
            {
                try
                {
                    if (File.Exists(CSV_TempFile) == true)
                    {
                        File.Delete(CSV_TempFile);
                    }
                    break;
                }
                catch (Exception ex)
                {
                }
            }
        }


        /// <summary>
        /// Writes DataReader to CSV File. CsvDataReaderAsync does not work (Do not use Async DataReaders). Note Appends to CSV File if CSV File already exists. 
        /// </summary>
        /// <param name="DataReader">The DataReader</param>
        /// <param name="CSV_File">CSV File to write to. Creates CSV File if it does not exist and merges data if it does exist.</param>
        /// <param name="CSV_TimeOut">Seconds allowed to start creating CSV File</param>
        /// <param name="CSV_Delimiter">Delimiter to be used</param>
        public static void ToCSV(this IDataReader DataReader, string CSV_File, int CSV_TimeOut = 5, char CSV_Delimiter = '|')
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

            int iFieldCount = DataReader.FieldCount;
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
                                while (DataReader.Read())
                                {
                                    string sLine = "";
                                    for (int i = 0; i < iFieldCount; i++)
                                    {
                                        sLine = sLine + (DataReader.GetValue(i) + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        if (bHeader)
                                        {
                                            sHeader = sHeader + DataReader.GetName(i).ToUpper() + CSV_Delimiter;
                                        }
                                    }
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
                                }
                                DataReader.Close();
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
                        for (int i = 0; i < iFieldCount; i++)
                        {
                            lstInsertHeader.Add(new Property { Column = DataReader.GetName(i).ToUpper() });
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
                                    while (DataReader.Read())
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        for (int i = 0; i < iFieldCount; i++)
                                        {
                                            sLine = sLine + (DataReader.GetValue(i) + "").Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        }
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));
                                    }
                                    DataReader.Close();
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
                                    while (DataReader.Read())
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        for (int i = 0; i < iFieldCount; i++)
                                        {
                                            lstProperty.Add(new Property { Column = DataReader.GetName(i).ToUpper(), Value = (DataReader.GetValue(i) + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int x = 0; x < iPropertyLength; x++)
                                        {
                                            for (int i = 0; i < iHeaderRowLength; i++)
                                            {
                                                if (sarHeaderRow[i] == lstProperty[x].Column)
                                                {
                                                    sLine = sLine + lstProperty[x].Value + CSV_Delimiter;
                                                }
                                            }
                                        }
                                        sw.WriteLine(sLine.Substring(0, (sLine.Length - 1)));
                                    }
                                    DataReader.Close();
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
                                        string sLine = (sr.ReadLine() + "").Trim();
                                        if (sLine != "")
                                        {
                                            if (!bHeader)
                                            {
                                                sw.WriteLine(sLine + sToAppendToExistingRows);
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
                                    while (DataReader.Read())
                                    {
                                        string sLine = "";
                                        List<Property> lstProperty = new List<Property>();
                                        for (int i = 0; i < iFieldCount; i++)
                                        {
                                            lstProperty.Add(new Property { Column = DataReader.GetName(i).ToUpper(), Value = (DataReader.GetValue(i) + "").Replace(CSV_Delimiter.ToString(), "") });
                                        }
                                        int iPropertyLength = lstProperty.Count();
                                        for (int i = 0; i < iNewHeaderRow; i++)
                                        {
                                            for (int x = 0; x < iPropertyLength; x++)
                                            {
                                                if (sarNewHeaderRow[i] == lstProperty[x].Column)
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
                                    }
                                    DataReader.Close();
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