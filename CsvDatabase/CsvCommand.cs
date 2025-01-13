using System;
using System.IO;
using System.Text;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace CsvDatabase
{ 
    public class CsvCommand
    {
        private static bool _bFileNotFound = false;

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

        /// <summary>
        /// Checks the Header is Distinct
        /// </summary>
        /// <param name="sarHeader">Header string array</param>
        /// <returns></returns>
        private static bool Distinct(string[] sarHeader)
        {            
            int iHeaderLength = sarHeader.Length;
            int iIndex = 0;
            for (int i = 0; i < iHeaderLength; i++)
            {
                iIndex = 0;
                for (int a = 0; a < iHeaderLength; a++)
                {
                    if (sarHeader[i] == sarHeader[a])
                    {
                        iIndex = iIndex + 1;
                    }
                    if (iIndex == 2)
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        private class Property
        {
            public string Column { get; set; }
            public string Value { get; set; }
        }

        /// <summary>
        /// Insert Record/Row into CSV. CSV_File contains Path and Filename to Output to.
        /// </summary>
        /// <param name="CSV_Class_Insert">Class File with Parameters named after CSV Header names. Inserts Class into CSV.</param>
        /// <param name="CSV_File">Output path and FileName of CSV to Create or AppendTo</param>
        /// <param name="CSV_Delimiter">CSV Separator Value (Default |). Multiple Delimiters will try to be found if no match.</param>
        public static void Insert(object CSV_Class_Insert, string CSV_File, char CSV_Delimiter = '|')
        {
            List<Property> lstProperty = new List<Property>();
            foreach (PropertyInfo p in CSV_Class_Insert.GetType().GetProperties())
            {
                lstProperty.Add(new Property { Column = p.Name.ToUpper(), Value = (p.GetValue(CSV_Class_Insert) + "") });
            }
            int iPropertyLength = lstProperty.Count();
            string[] sarHeader = new string[iPropertyLength];
            string[] sarValues = new string[iPropertyLength];
            for (int i = 0; i < iPropertyLength; i++)
            {
                sarHeader[i] = lstProperty[i].Column;
                sarValues[i] = lstProperty[i].Value;
            }
            Insert(sarValues, sarHeader, CSV_File, CSV_Delimiter);
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
        /// Insert Record/Row into CSV. Takes String Array of each Field Value (DONT place Separator value in CSV_Values! CSV_File contains Path and Filename to Output to. Creates CSV TempFile if needed in CSV_File Directory.
        /// </summary>
        /// <param name="CSV_Values">String Array of Values to Insert Record. (DONT include Separator value!). CSV_Header Array Index Values must match CSV_Values Array Index</param>
        /// <param name="CSV_Header">String Array of Header Names to map to CSV_Values Index. CSV_Header Array Index Values must match CSV_Values Array Index</param>
        /// <param name="CSV_File">Output path and FileName of CSV to Create or AppendTo</param> 
        /// <param name="CSV_Delimiter">CSV Separator Value (Default |). Multiple Delimiters will try to be found if no match.</param>
        public static void Insert(string[] CSV_Values, string[] CSV_Header, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);            

            if (!Distinct(CSV_Header))
            {
                throw new Exception("CSV_Header must be Distinct.");
            }
            if (CSV_Values.Length != CSV_Header.Length)
            {
                throw new Exception("CSV_Header must match the number of CSV_Values.");
            }
            int iCSV_ValuesLength = CSV_Values.Length;
            StringBuilder sbrValues = new StringBuilder();
            FileStream fsTempFile2 = null;
            FileStream fs = null;
            FileStream fsTempFile1 = null;
            StreamWriter sw = null;
            StreamReader sr = null;
            StreamReader srTempFile1 = null;
            bool bFileLocked = true;
            string[] sarHeaderRow = null;
            bool bNoHeaderRow = true;
            string sHeaderRow = "";
            while (bFileLocked)
            {
                try
                {   //Find Header
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
                        string sLine = (srTempFile1.ReadLine() + "").Trim().Replace("\r", "").Replace("\n", ""); //System.Text.RegularExpressions.Regex.Replace((srTempFile1.ReadLine() + "").Trim(), @"\t|\n|\r", "");
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
                    if (bNoHeaderRow)
                    { //Empty CSV File                       
                        bFileLocked = true;
                        while (bFileLocked)
                        {
                            try
                            {
                                fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                sw = new StreamWriter(fs);
                                sw.WriteLine(string.Join(CSV_Delimiter.ToString(), CSV_Header).Replace("\r", "").Replace("\n", "")); //Write CSV Header
                                for (int i = 0; i < iCSV_ValuesLength; i++)
                                {
                                    if (i == (iCSV_ValuesLength - 1))
                                    {
                                        sbrValues.Append(CSV_Values[i].Replace(CSV_Delimiter.ToString(), ""));
                                    }
                                    else
                                    {
                                        sbrValues.Append(CSV_Values[i].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter);
                                    }
                                }
                                sw.WriteLine(sbrValues.ToString().Replace("\r", "").Replace("\n", "")); //Write CSV Value
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
                                if (File.Exists(CSV_TempFile2) == true)
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
                        int iCSV_HeaderLength = CSV_Header.Length;
                        int iHeaderRowLength = sarHeaderRow.Length;
                        string sInsert = ""; //The New Insert Row CSV Data
                        string sNewHeaderRow = sHeaderRow; //The New Header Row to Create
                        string sAppendHeader = "";
                        string sToAppendToExistingRows = ""; //The Empty Values to update old CSV File with New Added Headers
                        bool bFound = false;
                        bool bIsSimilarMatchingHeader = false;
                        for (int r = 0; r < iHeaderRowLength; r++)
                        {
                            bFound = false;
                            for (int h = 0; h < iCSV_HeaderLength; h++)
                            {
                                if (CSV_Header[h].ToUpper() == sarHeaderRow[r])
                                {
                                    bFound = true;
                                    sInsert = sInsert + CSV_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                    break;
                                }
                                else if (h == iCSV_HeaderLength - 1)
                                {
                                    sInsert = sInsert + CSV_Delimiter;
                                }
                            }
                            if (bFound)
                            {
                                bIsSimilarMatchingHeader = true;
                            }
                            else
                            {
                                bIsSimilarMatchingHeader = false;
                            }
                        }
                        if (iHeaderRowLength != iCSV_HeaderLength)
                        {
                            bIsSimilarMatchingHeader = false;
                        }
                        if (!bIsSimilarMatchingHeader)
                        {
                            for (int h = 0; h < iCSV_HeaderLength; h++)
                            {
                                for (int r = 0; r < iHeaderRowLength; r++)
                                {
                                    if (CSV_Header[h].ToUpper() == sarHeaderRow[r])
                                    {
                                        break;
                                    }
                                    else if (r == iHeaderRowLength - 1)
                                    {
                                        sInsert = sInsert + CSV_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;                                        
                                        sAppendHeader = sAppendHeader + CSV_Delimiter + CSV_Header[h];
                                        sToAppendToExistingRows = sToAppendToExistingRows + CSV_Delimiter;
                                    }
                                }
                            }
                            sNewHeaderRow = sNewHeaderRow + sAppendHeader;
                        }
                        sInsert = sInsert.Substring(0, sInsert.Length - 1);
                        if (sToAppendToExistingRows == "")
                        {//Append to End of CSV File Code here
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fs);
                                    sw.WriteLine(sInsert.Replace("\r", "").Replace("\n", ""));
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
                                    if (File.Exists(CSV_TempFile2) == true)
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
                        { //Update existing CSV file code here                           
                            FileStream fsCsvFile = null;
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fs = new FileStream(CSV_TempFile2, FileMode.Open, FileAccess.Read, FileShare.None);
                                    sr = new StreamReader(fs);
                                    fsCsvFile = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fsCsvFile);
                                    bool bHeader = true;
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
                                    sw.WriteLine(sInsert.Replace("\r", "").Replace("\n", ""));
                                    sw.Flush();
                                    bFileLocked = false;
                                }
                                catch
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
                                    if (fsCsvFile != null)
                                    {
                                        fsCsvFile.Dispose();
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
                                    if (File.Exists(CSV_TempFile2) == true)
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
        /// Delete Record(s)/Row(s) from CSV. Defaults CSV_TempFile to Directory of CSV_File.
        /// </summary>
        /// <param name="CSV_ID_Delete_Value">ID(s) Value that uniquely identifies a row(s)/record(s). Like Customer #</param>
        /// <param name="CSV_Header">The CSV Header Name to locate the ID to Delete from in CSV File.</param>
        /// <param name="CSV_File">The CSV FileName and Path that exists to Delete Record(s)</param>
        /// <param name="CSV_Delimiter">CSV Separator Value (Default |)</param>
        public static void Delete(string[] CSV_ID_Delete_Value, string CSV_Header, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);            
            FileStream fs = null;
            StreamReader sr = null;
            FileStream fsTemp = null;
            StreamWriter sw = null;
            bool bFileLocked = true;
            string[] sarHeader = null;
            int iHeaderIndex = 0;
            int iIDLength = CSV_ID_Delete_Value.Length;
            bool bFound = false;           
            while (bFileLocked)
            {
                try
                {
                    _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
                    if (_bFileNotFound)
                    {
                        CreateCsvFile(CSV_File, CSV_TempFile);
                    }
                    File.Move(CSV_File, CSV_TempFile);
                    bool bHeader = true;
                    fs = new FileStream(CSV_TempFile, FileMode.Open, FileAccess.Read, FileShare.None);
                    sr = new StreamReader(fs);
                    fsTemp = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fsTemp);
                    while (!sr.EndOfStream)
                    {
                        string sLine = (sr.ReadLine() + "").Trim();
                        if (sLine != "")
                        {
                            if (!bHeader)
                            {
                                string[] sarRow = sLine.Split(CSV_Delimiter);
                                if (iIDLength == 1)
                                {
                                    if (sarRow[iHeaderIndex].Trim().ToUpper() != CSV_ID_Delete_Value[0].Trim().ToUpper())
                                    {
                                        sw.WriteLine(sLine);
                                    }
                                }
                                else
                                {
                                    bFound = false;
                                    for (int i = 0; i < iIDLength; i++)
                                    {
                                        if (sarRow[iHeaderIndex].Trim().ToUpper() == CSV_ID_Delete_Value[i].Trim().ToUpper())
                                        {
                                            bFound = true;
                                            break;
                                        }
                                    }
                                    if (!bFound)
                                    {
                                        sw.WriteLine(sLine);
                                    }
                                }
                            }
                            else
                            {
                                FindDelimiter(sLine, ref CSV_Delimiter);
                                sarHeader = sLine.Split(CSV_Delimiter);
                                int iHeaderLength = sarHeader.Length;
                                for (int i = 0; i < iHeaderLength; i++)
                                {
                                    if (sarHeader[i].Trim().ToUpper() == CSV_Header.Trim().ToUpper())
                                    {
                                        iHeaderIndex = i;
                                        break;
                                    }
                                }
                                sw.WriteLine(sLine);
                                bHeader = false;
                            }
                        }
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
                    if (File.Exists(CSV_TempFile))
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
        /// Update Record(s)/Row(s) from CSV. Defaults CSV_TempFile to Directory of CSV_File.
        /// </summary>
        /// <param name="CSV_Update_ID">1 or Many ID Value that uniquely identifies a row(s)/record(s). Like Customer #</param>
        /// <param name="CSV_Update_ID_Header">The CSV Header that locates the Column in CSV of containing the ID.</param>        
        /// <param name="CSV_Update_Values">String Array of Headers to Update Row. (DONT include Separator value!). CSV_Update_Values String Array Index Values must match CSV_Update_Headers Index Field locations</param>
        /// <param name="CSV_Update_Headers">String Array of Values to Update Row. (DONT include Separator value!). CSV_Update_Values String Array Index Values must match CSV_Update_Headers Index Field locations</param>
        /// <param name="CSV_File">The CSV FileName and Path that exists to Delete Record(s)</param>
        /// <param name="CSV_Delimiter">CSV Delimiter Value (Default |)</param>
        public static void Update(string[] CSV_Update_ID, string CSV_Update_ID_Header, string[] CSV_Update_Values, string[] CSV_Update_Headers, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);            
            if (!Distinct(CSV_Update_Headers))
            {
                throw new Exception("CSV_Update_Headers must be Distinct.");
            }
            if (CSV_Update_Values.Length != CSV_Update_Headers.Length)
            {
                throw new Exception("CSV_Update_Headers must match the number of CSV_Update_Values.");
            }

            FileStream fs = null;
            StreamReader sr = null;
            FileStream fsTemp = null;
            StreamWriter sw = null;
            bool bFileLocked = true;
            int iHeaderIndex = 0;
            string[] sarHeaderRow = null;
            int iCSV_Update_IDLength = CSV_Update_ID.Length;
            bool bFound = false;
            bool bFoundUpdate = false;        
            while (bFileLocked)
    {
                try
                {
                    _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
                    if (_bFileNotFound)
                    {
                        CreateCsvFile(CSV_File, CSV_TempFile);
                    }
                    File.Move(CSV_File, CSV_TempFile);
                    string sUpdate = "";
                    bool bHeader = true;
                    string sToAppendToExistingRows = "";
                    string sAppendHeader = "";
                    string sNewHeaderRow = "";
                    int iCSV_Update_Values_Length = 0;
                    int iCSV_Update_Headers_Length = 0;
                    int iHeaderRowLength = 0;
                    string[] sarNewHeaderRow = null;
                    int iNewHeaderRowLength = 0;
                    fs = new FileStream(CSV_TempFile, FileMode.Open, FileAccess.Read, FileShare.None);                   
                    sr = new StreamReader(fs);
                    fsTemp = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fsTemp);
                    while (!sr.EndOfStream)
                    {
                        string sLine = (sr.ReadLine() + "").Trim();
                        if (sLine != "")
                        {
                            if (!bHeader)
                            {
                                sUpdate = "";
                                string[] sarRow = sLine.Split(CSV_Delimiter);
                                if (iCSV_Update_IDLength == 1)
                                {
                                    if (sarRow[iHeaderIndex].Trim().ToUpper() == CSV_Update_ID[0].Trim().ToUpper())
                                    {
                                        for (int i = 0; i < iNewHeaderRowLength; i++)
                                        {
                                            bFoundUpdate = false;
                                            for (int h = 0; h < iCSV_Update_Headers_Length; h++)
                                            {
                                                if (sarNewHeaderRow[i].Trim().ToUpper() == CSV_Update_Headers[h].Trim().ToUpper())
                                                {
                                                    sUpdate = sUpdate + CSV_Update_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                    bFoundUpdate = true;
                                                    break;
                                                }
                                            }
                                            if (!bFoundUpdate)
                                            {
                                                for (int h = 0; h < iHeaderRowLength; h++)
                                                {
                                                    if (sarNewHeaderRow[i].Trim().ToUpper() == sarHeaderRow[h].Trim().ToUpper())
                                                    {
                                                        sUpdate = sUpdate + sarRow[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                        bFoundUpdate = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (!bFoundUpdate)
                                            {
                                                sUpdate = sUpdate + CSV_Delimiter;
                                            }
                                        }
                                        sUpdate = sUpdate.Substring(0, sUpdate.Length - 1);
                                        sw.WriteLine(sUpdate.Replace("\r", "").Replace("\n", ""));
                                    }
                                    else
                                    {
                                        sw.WriteLine((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                    }
                                }
                                else
                                {
                                    bFound = false;
                                    for (int x = 0; x < iCSV_Update_IDLength; x++)
                                    {
                                        if (sarRow[iHeaderIndex].Trim().ToUpper() == CSV_Update_ID[x].Trim().ToUpper())
                                        {
                                            bFound = true;
                                            for (int i = 0; i < iNewHeaderRowLength; i++)
                                            {
                                                bFoundUpdate = false;
                                                for (int h = 0; h < iCSV_Update_Headers_Length; h++)
                                                {
                                                    if (sarNewHeaderRow[i].Trim().ToUpper() == CSV_Update_Headers[h].Trim().ToUpper())
                                                    {
                                                        sUpdate = sUpdate + CSV_Update_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                        bFoundUpdate = true;
                                                        break;
                                                    }
                                                }
                                                if (!bFoundUpdate)
                                                {
                                                    for (int h = 0; h < iHeaderRowLength; h++)
                                                    {
                                                        if (sarNewHeaderRow[i].Trim().ToUpper() == sarHeaderRow[h].Trim().ToUpper())
                                                        {
                                                            sUpdate = sUpdate + sarRow[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                            bFoundUpdate = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (!bFoundUpdate)
                                                {
                                                    sUpdate = sUpdate + CSV_Delimiter;
                                                }
                                            }
                                            sUpdate = sUpdate.Substring(0, sUpdate.Length - 1);
                                            sw.WriteLine(sUpdate.Replace("\r", "").Replace("\n", ""));
                                            break;
                                        }
                                    }
                                    if (!bFound)
                                    {
                                        sw.WriteLine((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                    }
                                }
                            }
                            else if (bHeader)
                            {
                                FindDelimiter(sLine, ref CSV_Delimiter);
                                bHeader = false;
                                iCSV_Update_Values_Length = CSV_Update_Values.Length;
                                iCSV_Update_Headers_Length = CSV_Update_Headers.Length;
                                sNewHeaderRow = sLine;
                                sarHeaderRow = sLine.ToUpper().Split(CSV_Delimiter);
                                iHeaderRowLength = sarHeaderRow.Length;
                                for (int i = 0; i < iHeaderRowLength; i++)
                                {
                                    if (CSV_Update_ID_Header.Trim().ToUpper() == sarHeaderRow[i].Trim().ToUpper())
                                    {
                                        iHeaderIndex = i;
                                        break;
                                    }
                                }
                                for (int h = 0; h < iCSV_Update_Headers_Length; h++)
                                {
                                    for (int r = 0; r < iHeaderRowLength; r++)
                                    {
                                        if (CSV_Update_Headers[h].Trim().ToUpper() == sarHeaderRow[r].Trim().ToUpper())
                                        {
                                            break;
                                        }
                                        else if (r == (iHeaderRowLength - 1))
                                        {
                                            sAppendHeader = sAppendHeader + CSV_Delimiter + CSV_Update_Headers[h];
                                            sToAppendToExistingRows = sToAppendToExistingRows + CSV_Delimiter;
                                        }
                                    }
                                }
                                sNewHeaderRow = sNewHeaderRow + sAppendHeader;
                                sarNewHeaderRow = sNewHeaderRow.Split(CSV_Delimiter);
                                iNewHeaderRowLength = sarNewHeaderRow.Length;
                                sw.WriteLine(sNewHeaderRow.Replace("\r", "").Replace("\n", ""));
                            }
                        }
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
                    if (File.Exists(CSV_TempFile))
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
        /// Insert Record/Row into CSV Async. CSV_File contains Path and Filename to Output to.
        /// </summary>
        /// <param name="CSV_Class_Insert">Class File with Parameters named after CSV Header names. Inserts Class into CSV.</param>
        /// <param name="CSV_File">Output path and FileName of CSV to Create or AppendTo</param> 
        /// <param name="CSV_Delimiter">CSV Separator Value (Default |). Multiple Delimiters will try to be found if no match.</param>
        public static Task InsertAsync(object CSV_Class_Insert, string CSV_File, char CSV_Delimiter = '|')
        {
            List<Property> lstProperty = new List<Property>();
            foreach (PropertyInfo p in CSV_Class_Insert.GetType().GetProperties())
            {
                lstProperty.Add(new Property { Column = p.Name.ToUpper(), Value = (p.GetValue(CSV_Class_Insert) + "") });
            }
            int iPropertyLength = lstProperty.Count();
            string[] sarHeader = new string[iPropertyLength];
            string[] sarValues = new string[iPropertyLength];
            for (int i = 0; i < iPropertyLength; i++)
            {
                sarHeader[i] = lstProperty[i].Column;
                sarValues[i] = lstProperty[i].Value;
            }
            return InsertAsync(sarValues, sarHeader, CSV_File, CSV_Delimiter);
        }

        /// <summary>
        /// Insert Record/Row into CSV Async. Takes String Array of each Field Value (DONT place Separator value in CSV_Values! CSV_File contains Path and Filename to Output to. Creates CSV TempFile if needed in CSV_File Directory.
        /// </summary>
        /// <param name="CSV_Values">String Array of Values to Insert Record. (DONT include Separator value!). CSV_Header Array Index Values must match CSV_Values Array Index</param>
        /// <param name="CSV_Header">String Array of Header Names to map to CSV_Values Index. CSV_Header Array Index Values must match CSV_Values Array Index</param>
        /// <param name="CSV_File">Output path and FileName of CSV to Create or AppendTo</param>
        /// <param name="CSV_Delimiter">CSV Separator Value (Default |). Multiple Delimiters will try to be found if no match.</param>
        public static async Task InsertAsync(string[] CSV_Values, string[] CSV_Header, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);
            if (!Distinct(CSV_Header))
            {
                throw new Exception("CSV_Header must be Distinct.");
            }
            if (CSV_Values.Length != CSV_Header.Length)
            {
                throw new Exception("CSV_Header must match the number of CSV_Values.");
            }
            int iCSV_ValuesLength = CSV_Values.Length;
            StringBuilder sbrValues = new StringBuilder();
            FileStream fsTempFile2 = null;
            FileStream fs = null;
            FileStream fsTempFile1 = null;
            StreamWriter sw = null;
            StreamReader sr = null;
            StreamReader srTempFile1 = null;
            bool bFileLocked = true;
            string[] sarHeaderRow = null;
            bool bNoHeaderRow = true;
            string sHeaderRow = "";
            while (bFileLocked)
            {
                try
                {   //Find Header
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
                    if (bNoHeaderRow)
                    { //Empty CSV File                       
                        bFileLocked = true;
                        while (bFileLocked)
                        {
                            try
                            {
                                fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                sw = new StreamWriter(fs);
                                await sw.WriteLineAsync(string.Join(CSV_Delimiter.ToString(), CSV_Header).Replace("\r", "").Replace("\n", "")); //Write CSV Header
                                for (int i = 0; i < iCSV_ValuesLength; i++)
                                {
                                    if (i == (iCSV_ValuesLength - 1))
                                    {
                                        sbrValues.Append(CSV_Values[i].Replace(CSV_Delimiter.ToString(), ""));
                                    }
                                    else
                                    {
                                        sbrValues.Append(CSV_Values[i].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter);
                                    }
                                }
                                await sw.WriteLineAsync(sbrValues.ToString().Replace("\r", "").Replace("\n", "")); //Write CSV Value
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
                                if (File.Exists(CSV_TempFile2) == true)
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
                        int iCSV_HeaderLength = CSV_Header.Length;
                        int iHeaderRowLength = sarHeaderRow.Length;
                        string sInsert = ""; //The New Insert Row CSV Data
                        string sNewHeaderRow = sHeaderRow; //The New Header Row to Create
                        string sAppendHeader = "";
                        string sToAppendToExistingRows = ""; //The Empty Values to update old CSV File with New Added Headers
                        bool bFound = false;
                        bool bIsSimilarMatchingHeader = false;
                        for (int r = 0; r < iHeaderRowLength; r++)
                        {
                            bFound = false;
                            for (int h = 0; h < iCSV_HeaderLength; h++)
                            {
                                if (CSV_Header[h].ToUpper() == sarHeaderRow[r])
                                {
                                    bFound = true;
                                    sInsert = sInsert + CSV_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                    break;
                                }
                                else if (h == iCSV_HeaderLength - 1)
                                {
                                    sInsert = sInsert + CSV_Delimiter;
                                }
                            }
                            if (bFound)
                            {
                                bIsSimilarMatchingHeader = true;
                            }
                            else
                            {
                                bIsSimilarMatchingHeader = false;
                            }
                        }
                        if (iHeaderRowLength != iCSV_HeaderLength)
                        {
                            bIsSimilarMatchingHeader = false;
                        }
                        if (!bIsSimilarMatchingHeader)
                        {
                            for (int h = 0; h < iCSV_HeaderLength; h++)
                            {
                                for (int r = 0; r < iHeaderRowLength; r++)
                                {
                                    if (CSV_Header[h].ToUpper() == sarHeaderRow[r])
                                    {
                                        break;
                                    }
                                    else if (r == iHeaderRowLength - 1)
                                    {
                                        sInsert = sInsert + CSV_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                        sAppendHeader = sAppendHeader + CSV_Delimiter + CSV_Header[h];
                                        sToAppendToExistingRows = sToAppendToExistingRows + CSV_Delimiter;
                                    }
                                }
                            }
                            sNewHeaderRow = sNewHeaderRow + sAppendHeader;
                        }
                        sInsert = sInsert.Substring(0, sInsert.Length - 1);
                        if (sToAppendToExistingRows == "")
                        {//Append to End of CSV File Code here
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fs = new FileStream(CSV_TempFile2, FileMode.Append, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fs);
                                    await sw.WriteLineAsync(sInsert.Replace("\r", "").Replace("\n", ""));
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
                                    if (File.Exists(CSV_TempFile2) == true)
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
                        { //Update existing CSV file code here                           
                            FileStream fsCsvFile = null;
                            bFileLocked = true;
                            while (bFileLocked)
                            {
                                try
                                {
                                    fs = new FileStream(CSV_TempFile2, FileMode.Open, FileAccess.Read, FileShare.None);
                                    sr = new StreamReader(fs);
                                    fsCsvFile = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                                    sw = new StreamWriter(fsCsvFile);
                                    bool bHeader = true;
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
                                    await sw.WriteLineAsync(sInsert.Replace("\r", "").Replace("\n", ""));
                                    await sw.FlushAsync();
                                    bFileLocked = false;
                                }
                                catch
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
                                    if (fsCsvFile != null)
                                    {
                                        fsCsvFile.Dispose();
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
                                    if (File.Exists(CSV_TempFile2) == true)
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
        /// Delete Record(s)/Row(s) from CSV Async. Defaults CSV_TempFile to Directory of CSV_File.
        /// </summary>
        /// <param name="CSV_ID_Delete_Value">ID(s) Value that uniquely identifies a row(s)/record(s). Like Customer #</param>
        /// <param name="CSV_Header">The CSV Header Name to locate the ID to Delete from in CSV File.</param>
        /// <param name="CSV_File">The CSV FileName and Path that exists to Delete Record(s)</param> 
        /// <param name="CSV_Delimiter">CSV Separator Value (Default |)</param>
        public static async Task DeleteAsync(string[] CSV_ID_Delete_Value, string CSV_Header, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            FileStream fs = null;
            StreamReader sr = null;
            FileStream fsTemp = null;
            StreamWriter sw = null;
            bool bFileLocked = true;
            string[] sarHeader = null;
            int iHeaderIndex = 0;
            int iIDLength = CSV_ID_Delete_Value.Length;
            bool bFound = false;
            while (bFileLocked)
            {
                try
                {
                    _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
                    if (_bFileNotFound)
                    {
                        CreateCsvFile(CSV_File, CSV_TempFile);
                    }
                    File.Move(CSV_File, CSV_TempFile);
                    bool bHeader = true;
                    fs = new FileStream(CSV_TempFile, FileMode.Open, FileAccess.Read, FileShare.None);
                    sr = new StreamReader(fs);
                    fsTemp = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fsTemp);
                    while (!sr.EndOfStream)
                    {
                        string sLine = (await sr.ReadLineAsync() + "").Trim();
                        if (sLine != "")
                        {
                            if (!bHeader)
                            {
                                string[] sarRow = sLine.Split(CSV_Delimiter);
                                if (iIDLength == 1)
                                {
                                    if (sarRow[iHeaderIndex].Trim().ToUpper() != CSV_ID_Delete_Value[0].Trim().ToUpper())
                                    {
                                        await sw.WriteLineAsync(sLine);
                                    }
                                }
                                else
                                {
                                    bFound = false;
                                    for (int i = 0; i < iIDLength; i++)
                                    {
                                        if (sarRow[iHeaderIndex].Trim().ToUpper() == CSV_ID_Delete_Value[i].Trim().ToUpper())
                                        {
                                            bFound = true;
                                            break;
                                        }
                                    }
                                    if (!bFound)
                                    {
                                        await sw.WriteLineAsync(sLine);
                                    }
                                }
                            }
                            else
                            {
                                FindDelimiter(sLine, ref CSV_Delimiter);
                                sarHeader = sLine.Split(CSV_Delimiter);
                                int iHeaderLength = sarHeader.Length;
                                for (int i = 0; i < iHeaderLength; i++)
                                {
                                    if (sarHeader[i].Trim().ToUpper() == CSV_Header.Trim().ToUpper())
                                    {
                                        iHeaderIndex = i;
                                        break;
                                    }
                                }
                                await sw.WriteLineAsync(sLine);
                                bHeader = false;
                            }
                        }
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
                    if (File.Exists(CSV_TempFile))
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
        /// Update Record(s)/Row(s) from CSV Async. Defaults CSV_TempFile to Directory of CSV_File.
        /// </summary>
        /// <param name="CSV_Update_ID">1 or Many ID Value that uniquely identifies a row(s)/record(s). Like Customer #</param>
        /// <param name="CSV_Update_ID_Header">The CSV Header that locates the Column in CSV of containing the ID.</param>        
        /// <param name="CSV_Update_Values">String Array of Headers to Update Row. (DONT include Separator value!). CSV_Update_Values String Array Index Values must match CSV_Update_Headers Index Field locations</param>
        /// <param name="CSV_Update_Headers">String Array of Values to Update Row. (DONT include Separator value!). CSV_Update_Values String Array Index Values must match CSV_Update_Headers Index Field locations</param>
        /// <param name="CSV_File">The CSV FileName and Path that exists to Delete Record(s)</param>
        /// <param name="CSV_Delimiter">CSV Delimiter Value (Default |)</param>
        public static async Task UpdateAsync(string[] CSV_Update_ID, string CSV_Update_ID_Header, string[] CSV_Update_Values, string[] CSV_Update_Headers, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            if (!Distinct(CSV_Update_Headers))
            {
                throw new Exception("CSV_Update_Headers must be Distinct.");
            }
            if (CSV_Update_Values.Length != CSV_Update_Headers.Length)
            {
                throw new Exception("CSV_Update_Headers must match the number of CSV_Update_Values.");
            }

            FileStream fs = null;
            StreamReader sr = null;
            FileStream fsTemp = null;
            StreamWriter sw = null;
            bool bFileLocked = true;
            int iHeaderIndex = 0;
            string[] sarHeaderRow = null;
            int iCSV_Update_IDLength = CSV_Update_ID.Length;
            bool bFound = false;
            bool bFoundUpdate = false;
            while (bFileLocked)
            {
                try
                {
                    _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
                    if (_bFileNotFound)
                    {
                        CreateCsvFile(CSV_File, CSV_TempFile);
                    }
                    File.Move(CSV_File, CSV_TempFile);
                    string sUpdate = "";
                    bool bHeader = true;
                    string sToAppendToExistingRows = "";
                    string sAppendHeader = "";
                    string sNewHeaderRow = "";
                    int iCSV_Update_Values_Length = 0;
                    int iCSV_Update_Headers_Length = 0;
                    int iHeaderRowLength = 0;
                    string[] sarNewHeaderRow = null;
                    int iNewHeaderRowLength = 0;
                    fs = new FileStream(CSV_TempFile, FileMode.Open, FileAccess.Read, FileShare.None);
                    sr = new StreamReader(fs);
                    fsTemp = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fsTemp);
                    while (!sr.EndOfStream)
                    {
                        string sLine = (await sr.ReadLineAsync() + "").Trim();
                        if (sLine != "")
                        {
                            if (!bHeader)
                            {
                                sUpdate = "";
                                string[] sarRow = sLine.Split(CSV_Delimiter);
                                if (iCSV_Update_IDLength == 1)
                                {
                                    if (sarRow[iHeaderIndex].Trim().ToUpper() == CSV_Update_ID[0].Trim().ToUpper())
                                    {
                                        for (int i = 0; i < iNewHeaderRowLength; i++)
                                        {
                                            bFoundUpdate = false;
                                            for (int h = 0; h < iCSV_Update_Headers_Length; h++)
                                            {
                                                if (sarNewHeaderRow[i].Trim().ToUpper() == CSV_Update_Headers[h].Trim().ToUpper())
                                                {
                                                    sUpdate = sUpdate + CSV_Update_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                    bFoundUpdate = true;
                                                    break;
                                                }
                                            }
                                            if (!bFoundUpdate)
                                            {
                                                for (int h = 0; h < iHeaderRowLength; h++)
                                                {
                                                    if (sarNewHeaderRow[i].Trim().ToUpper() == sarHeaderRow[h].Trim().ToUpper())
                                                    {
                                                        sUpdate = sUpdate + sarRow[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                        bFoundUpdate = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (!bFoundUpdate)
                                            {
                                                sUpdate = sUpdate + CSV_Delimiter;
                                            }
                                        }
                                        sUpdate = sUpdate.Substring(0, sUpdate.Length - 1);
                                        await sw.WriteLineAsync(sUpdate.Replace("\r", "").Replace("\n", ""));
                                    }
                                    else
                                    {
                                        await sw.WriteLineAsync((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                    }
                                }
                                else
                                {
                                    bFound = false;
                                    for (int x = 0; x < iCSV_Update_IDLength; x++)
                                    {
                                        if (sarRow[iHeaderIndex] == CSV_Update_ID[x])
                                        {
                                            bFound = true;
                                            for (int i = 0; i < iNewHeaderRowLength; i++)
                                            {
                                                bFoundUpdate = false;
                                                for (int h = 0; h < iCSV_Update_Headers_Length; h++)
                                                {
                                                    if (sarNewHeaderRow[i].Trim().ToUpper() == CSV_Update_Headers[h].Trim().ToUpper())
                                                    {
                                                        sUpdate = sUpdate + CSV_Update_Values[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                        bFoundUpdate = true;
                                                        break;
                                                    }
                                                }
                                                if (!bFoundUpdate)
                                                {
                                                    for (int h = 0; h < iHeaderRowLength; h++)
                                                    {
                                                        if (sarNewHeaderRow[i].Trim().ToUpper() == sarHeaderRow[h].Trim().ToUpper())
                                                        {
                                                            sUpdate = sUpdate + sarRow[h].Replace(CSV_Delimiter.ToString(), "") + CSV_Delimiter;
                                                            bFoundUpdate = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (!bFoundUpdate)
                                                {
                                                    sUpdate = sUpdate + CSV_Delimiter;
                                                }
                                            }
                                            sUpdate = sUpdate.Substring(0, sUpdate.Length - 1);
                                            await sw.WriteLineAsync(sUpdate.Replace("\r", "").Replace("\n", ""));
                                            break;
                                        }
                                    }
                                    if (!bFound)
                                    {
                                        await sw.WriteLineAsync((sLine + sToAppendToExistingRows).Replace("\r", "").Replace("\n", ""));
                                    }
                                }
                            }
                            else if (bHeader)
                            {
                                FindDelimiter(sLine, ref CSV_Delimiter);
                                bHeader = false;
                                iCSV_Update_Values_Length = CSV_Update_Values.Length;
                                iCSV_Update_Headers_Length = CSV_Update_Headers.Length;
                                sNewHeaderRow = sLine;
                                sarHeaderRow = sLine.ToUpper().Split(CSV_Delimiter);
                                iHeaderRowLength = sarHeaderRow.Length;
                                for (int i = 0; i < iHeaderRowLength; i++)
                                {
                                    if (CSV_Update_ID_Header.Trim().ToUpper() == sarHeaderRow[i].Trim().ToUpper())
                                    {
                                        iHeaderIndex = i;
                                        break;
                                    }
                                }
                                for (int h = 0; h < iCSV_Update_Headers_Length; h++)
                                {
                                    for (int r = 0; r < iHeaderRowLength; r++)
                                    {
                                        if (CSV_Update_Headers[h].Trim().ToUpper() == sarHeaderRow[r].Trim().ToUpper())
                                        {
                                            break;
                                        }
                                        else if (r == (iHeaderRowLength - 1))
                                        {
                                            sAppendHeader = sAppendHeader + CSV_Delimiter + CSV_Update_Headers[h];
                                            sToAppendToExistingRows = sToAppendToExistingRows + CSV_Delimiter;
                                        }
                                    }
                                }
                                sNewHeaderRow = sNewHeaderRow + sAppendHeader;
                                sarNewHeaderRow = sNewHeaderRow.Split(CSV_Delimiter);
                                iNewHeaderRowLength = sarNewHeaderRow.Length;
                                await sw.WriteLineAsync(sNewHeaderRow.Replace("\r", "").Replace("\n", ""));
                            }
                        }
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
                    if (File.Exists(CSV_TempFile))
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
        /// Drop or Delete a CSV File
        /// </summary>
        /// <param name="CSV_File">CSV File to Drop</param>
        /// <param name="CSV_TimeOut">Seconds to Drop File</param>
        public static void Drop(string CSV_File, int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);

            DateTime dtStart = DateTime.Now;
            while (true)
            {
                try
                {
                    File.Delete(CSV_File);
                    try
                    {
                        File.Delete(CSV_TempFile);
                    }
                    catch
                    {
                    }
                    try
                    {
                        File.Delete(CSV_TempFile2);
                    }
                    catch
                    {
                    }
                    break;
                }
                catch (Exception ex)
                {
                    TimeSpan tsInterval = DateTime.Now - dtStart;
                    if (tsInterval.Seconds >= CSV_TimeOut)
                    {
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Rename file.
        /// </summary>
        /// <param name="CSV_FromFile">Original CSV File and Path to be renamed.</param>
        /// <param name="CSV_ToFile">CSV File New File Name and optional Path</param>
        /// <param name="CSV_TimeOut">Seconds to Drop File</param>
        public static void Rename(string CSV_FromFile, string CSV_ToFile, int CSV_TimeOut = 5)
        {
            if (CSV_ToFile.IndexOf(@"\") == -1 && CSV_ToFile.IndexOf(@"/") == -1)
            {
                CSV_ToFile = Path.GetDirectoryName(CSV_FromFile) + @"\" + CSV_ToFile;
            }
            DateTime dtStart = DateTime.Now;
            while (true)
            {
                try
                {
                    File.Move(CSV_FromFile, CSV_ToFile);
                    break;
                }
                catch (Exception ex)
                {
                    TimeSpan tsInterval = DateTime.Now - dtStart;
                    if (tsInterval.Seconds >= CSV_TimeOut)
                    {
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Creates Empty CSV File with just the Header
        /// </summary>
        /// <param name="CSV_Header">Header to create CSV File</param>
        /// <param name="CSV_File">File Path of CSV File</param>
        /// <param name="CSV_Delimiter">Delimiter to be used</param>
        /// <param name="CSV_TimeOut">Seconds to check to see if file already exists.</param>
        public static void CreateTable(string[] CSV_Header, string CSV_File, char CSV_Delimiter = '|')
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            //string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                FileStream fs = null;
                StreamWriter sw = null;
                DateTime dtStart = DateTime.Now;
                while (true)
                {
                    try
                    {
                        if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                        {
                            break;
                        }
                        fs = new FileStream(CSV_File, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                        sw = new StreamWriter(fs);
                        sw.WriteLine(string.Join(CSV_Delimiter.ToString(), CSV_Header));
                        sw.Flush();
                        break;
                    }
                    catch
                    {

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
            }
        }

        /// <summary>
        /// Creates Empty CSV File with just the Header Async
        /// </summary>
        /// <param name="CSV_Header">Header to create CSV File</param>
        /// <param name="CSV_File">File Path of CSV File</param>
        /// <param name="CSV_Delimiter">Delimiter to be used</param>
        /// <param name="CSV_TimeOut">Seconds to check to see if file already exists.</param>
        public static async Task CreateTableAsync(string[] CSV_Header, string CSV_File, char CSV_Delimiter = '|', int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            //string CSV_TempFile2 = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE2" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                FileStream fs = null;
                StreamWriter sw = null;
                DateTime dtStart = DateTime.Now;
                while (true)
                {
                    try
                    {
                        if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                        {
                            break;
                        }
                        TimeSpan tsInterval = DateTime.Now - dtStart;
                        if (tsInterval.Seconds >= CSV_TimeOut)
                        {
                            fs = new FileStream(CSV_File, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                            sw = new StreamWriter(fs);
                            await sw.WriteLineAsync(string.Join(CSV_Delimiter.ToString(), CSV_Header));
                            await sw.FlushAsync();
                            break;
                        }
                    }
                    catch
                    {

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
            }
        }

        /// <summary>
        /// Truncates the CSV File, but leaves the Header.
        /// </summary>
        /// <param name="CSV_File">CSV File to Truncate</param>
        /// <param name="CSV_TimeOut">Seconds allowed try to Truncate the File. Default 5 seconds</param>
        public static async Task TruncateAsync(string CSV_File, int CSV_TimeOut = 5)
        {
            DateTime dtStart = DateTime.Now;
            FileStream fs = null;
            StreamWriter sw = null;
            StreamReader sr = null;
            while (true)
            {
                try
                {
                    fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.None);
                    sr = new StreamReader(fs);
                    string sHeaderLine = "";
                    while (!sr.EndOfStream)
                    {
                        sHeaderLine = await sr.ReadLineAsync();
                        break;
                    }
                    if (sr != null)
                    {
                        sr.Dispose();
                        sr = null;
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    fs = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fs);
                    await sw.WriteLineAsync(sHeaderLine);
                    await sw.FlushAsync();
                    if (sw != null)
                    {
                        sw.Dispose();
                        sw = null;
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    break;
                    //Old truncate
                    //fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Write, FileShare.None);
                    //fs.SetLength(0);
                    //break;
                }
                catch
                {
                    TimeSpan tsInterval = DateTime.Now - dtStart;
                    if (tsInterval.Seconds >= CSV_TimeOut)
                    {
                        break;
                    }
                }
                finally
                {
                    if (fs != null)
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    if (sw != null)
                    {
                        sw.Dispose();
                        sw = null;
                    }
                    if (sr != null)
                    {
                        sr.Dispose();
                        sr = null;
                    }
                }
            }
        }

        /// <summary>
        /// Truncates the CSV File, but leaves the Header.
        /// </summary>
        /// <param name="CSV_File">CSV File to Truncate</param>
        /// <param name="CSV_TimeOut">Seconds allowed try to Truncate the File. Default 5 seconds</param>
        public static void Truncate(string CSV_File, int CSV_TimeOut = 5)
        {
            DateTime dtStart = DateTime.Now;
            FileStream fs = null;
            StreamWriter sw = null;
            StreamReader sr = null;
            while (true)
            {
                try
                {
                    fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.None);
                    sr = new StreamReader(fs);
                    string sHeaderLine = "";
                    while (!sr.EndOfStream)
                    {
                        sHeaderLine = sr.ReadLine();
                        break;
                    }
                    if (sr != null)
                    {
                        sr.Dispose();
                        sr = null;
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    fs = new FileStream(CSV_File, FileMode.Create, FileAccess.Write, FileShare.None);
                    sw = new StreamWriter(fs);
                    sw.WriteLine(sHeaderLine);
                    sw.Flush();
                    if (sw != null)
                    {
                        sw.Dispose();
                        sw = null;
                    }
                    if (fs != null)
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    break;
                    //Old truncate
                    //fs = new FileStream(CSV_File, FileMode.Open, FileAccess.Write, FileShare.None);
                    //fs.SetLength(0);
                    //break;
                }
                catch
                {
                    TimeSpan tsInterval = DateTime.Now - dtStart;
                    if (tsInterval.Seconds >= CSV_TimeOut)
                    {
                        break;
                    }
                }
                finally
                {
                    if (fs != null)
                    {
                        fs.Dispose();
                        fs = null;
                    }
                    if (sw != null)
                    {
                        sw.Dispose();
                        sw = null;
                    }
                    if (sr != null)
                    {
                        sr.Dispose();
                        sr = null;
                    }
                }
            }
        }
    }
}
