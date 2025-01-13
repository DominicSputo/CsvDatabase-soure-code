using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CsvDatabase
{
    public class CsvPager
    {
        private static bool _bFileNotFound = false;
        
        /// <summary>
        /// CSV Property Name and DataType of class.
        /// </summary>
        private class Data
        {
            /// <summary>
            /// Property Name
            /// </summary>
            public string Column { get; set; }
            /// <summary>
            /// DataType
            /// </summary>
            public Type DataType { get; set; }
        }

        /// <summary>
        /// Get Sorted Page Size and Page Number of IEnumerable Class using CSV File. If Sort is greater than 4GB a non-sorted GetPage will return.
        /// </summary>
        /// <typeparam name="T">The Class with Properties to represent the CSV File Headers</typeparam>
        /// <param name="Page_Size">Set the Page Size of pager data returned.</param>
        /// <param name="Page_Number">Set the Page Number for pager data returned. Zero based indexed.</param>
        /// <param name="iTotalPages">Get the total pages found using page size.</param>
        /// <param name="iResultCount">Get the result count of records found.</param>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>
        /// <param name="CSV_Filter">Filter the CSV File using HEADER = VALUE, HEADER IN VALUE1 | VALUE2 | Etc, or HEADER LIKE VALUE</param>
        /// <param name="CSV_Sort">The CSV Sorted Column(s)</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="bFirstRowHeader">Specify the csv got a header in first row or not. Default is true and if argument is false then auto header 'ROW_xx will be used as per the order of columns.</param>
        /// <param name="CSV_TimeOut">Number of seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>Returns Page Size and Page Number of IEnumerable Class Type using CSV File</returns>
        public static IEnumerable<T> GetPage<T>(Int32 Page_Size, Int64 Page_Number, out Int64 iTotalPages, out Int64 iResultCount, string CSV_File, string CSV_Filter = "", string CSV_Sort = "", char CSV_Delimiter = '|', bool bFirstRowHeader = true, int CSV_TimeOut = 5)
        {
            if (CSV_Sort != "")
            {
                string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
                _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
                if (_bFileNotFound)
                {
                    iTotalPages = 0;
                    iResultCount = 0;
                    return new List<T>();
                }
                var lstCSV = new List<T>();
                DataTable dtbl = new DataTable();
                try
                {
                    iResultCount = 0;
                    CsvDataReader dtrCount = new CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
                    while (dtrCount.Read())
                    {
                        
                    }
                    dtrCount.Close();
                    iResultCount = dtrCount.RecordCount;
                    List<Data> lstData = new List<Data>();
                    DataRow dr = null;
                    foreach (var property in typeof(T).GetProperties())
                    {
                        Type tDataType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                        lstData.Add(new Data() { Column = property.Name.ToUpper(), DataType = tDataType });
                        dtbl.Columns.Add(property.Name.ToUpper(), tDataType);
                    }
                    int iDataTableColumnCount = dtbl.Columns.Count;
                    iTotalPages = (Int64)Math.Ceiling((iResultCount / (double)Page_Size));
                    Int64 iStartIndex = Page_Number * Page_Size;
                    Int64 iEndIndex = iStartIndex + Page_Size;
                    
                    int iHeaderRowLength = 0;
                    
                    Int64 iIndex = 0;
                    CsvDataReader dtrPage = new CsvDatabase.CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
                    iHeaderRowLength = dtrPage.FieldCount;
                    bool bFirstRecord = false;
                    bool bEndOfRecords = false;
                    while (dtrPage.Read())
                    {                        
                        for (int z = 0; z < iDataTableColumnCount; z++)
                        {
                            for (int i = 0; i < iHeaderRowLength; i++)
                            {
                                if (dtbl.Columns[z].ColumnName.ToUpper() == dtrPage.GetName(i).ToUpper())
                                {
                                    if (z == 0)
                                    {
                                        if (bFirstRecord == true)
                                        {
                                            if (bEndOfRecords == false)
                                            {
                                                dtbl.Rows.Add(dr);
                                            }
                                        }
                                        bFirstRecord = true;
                                        dr = dtbl.NewRow();
                                        dr[z] = ConvertTo(dtrPage[i] + "", lstData[z].DataType);

                                    }
                                    else if (z == (iDataTableColumnCount - 1))
                                    {
                                        dr[z] = ConvertTo(dtrPage[i] + "", lstData[z].DataType);
                                        dtbl.Rows.Add(dr);
                                        bEndOfRecords = true;
                                    }
                                    else
                                    {
                                        dr[z] = ConvertTo(dtrPage[i] + "", lstData[z].DataType);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    if (bFirstRecord == true)
                    {
                        if (bEndOfRecords == false)
                        {
                            dtbl.Rows.Add(dr);
                        }
                    }

                    dtrPage.Close();
                    dtbl.DefaultView.Sort = CSV_Sort;
                    DataView dv = dtbl.DefaultView;

                    foreach (DataRowView drData in dv)
                    {
                        if (iIndex >= iStartIndex && iIndex < iEndIndex)
                        {                            
                            var item = Activator.CreateInstance<T>();
                            foreach (var property in typeof(T).GetProperties())
                            {
                                for (int i = 0; i < iDataTableColumnCount; i++)
                                {
                                    if (property.Name.ToUpper() == dtbl.Columns[i].ColumnName.ToUpper())
                                    {
                                        Type convertTo = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                                        try
                                        {
                                            property.SetValue(item, ConvertTo(drData[i] + "", convertTo), null);
                                        }
                                        catch
                                        {
                                        }
                                        break;
                                    }
                                }
                            }
                            lstCSV.Add(item);
                        }
                        if (iIndex > iEndIndex)
                        {
                            break;
                        }
                        iIndex = iIndex + 1;
                    }
                    dtbl.Clear();
                    dv.Dispose();
                    dv = null;
                    return lstCSV;
                }
                catch
                {
                }
                lstCSV.Clear();
                dtbl.Clear(); 
                return GetPageNoSort<T>(Page_Size, Page_Number, out iTotalPages, out iResultCount, CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
            }
            else
            {
                return GetPageNoSort<T>(Page_Size, Page_Number, out iTotalPages, out iResultCount, CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
            }
        }

        /// <summary>
        /// Get Page Size and Page Number of IEnumerable Class using CSV File.
        /// </summary>
        /// <typeparam name="T">The Class with Properties to represent the CSV File Headers</typeparam>
        /// <param name="Page_Size">Set the Page Size of pager data returned.</param>
        /// <param name="Page_Number">Set the Page Number for pager data returned.</param>
        /// <param name="iTotalPages">Get the total pages found using page size.</param>
        /// <param name="iResultCount">Get the result count of records found.</param>
        /// <param name="CSV_File">The CSV FileName and Path that exists</param>
        /// <param name="CSV_Filter">Filter the CSV File using HEADER = VALUE, HEADER IN VALUE1 | VALUE2 | Etc, or HEADER LIKE VALUE</param>
        /// <param name="CSV_Delimiter">The CSV Delimiter used. Default | , but the if Delimiter NOT Found it will search for '\t', '|', ',', '^', ';', ':', '~', '\\', '/', '*', '-' as new Delimiter</param>
        /// <param name="bFirstRowHeader">Specify the csv got a header in first row or not. Default is true and if argument is false then auto header 'ROW_xx will be used as per the order of columns.</param>
        /// <param name="CSV_TimeOut">Number of seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <returns>Returns Page Size and Page Number of IEnumerable Class Type using CSV File</returns>
        private static IEnumerable<T> GetPageNoSort<T>(Int32 Page_Size, Int64 Page_Number, out Int64 iTotalPages, out Int64 iResultCount, string CSV_File, string CSV_Filter = "", char CSV_Delimiter = '|', bool bFirstRowHeader = true, int CSV_TimeOut = 5)
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                iTotalPages = 0;
                iResultCount = 0;
                return new List<T>();
            }
            iResultCount = 0;
            CsvDataReader dtrCount = new CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
            while (dtrCount.Read())
            {

            }
            dtrCount.Close();
            iResultCount = dtrCount.RecordCount;
            iTotalPages = (Int64)Math.Ceiling((iResultCount / (double)Page_Size));
            Int64 iStartIndex = Page_Number * Page_Size;
            Int64 iEndIndex = iStartIndex + Page_Size;
            var lstCSV = new List<T>();
            int iHeaderRowLength = 0;            
            Int64 iIndex = 0;
            CsvDataReader dtrPage = new CsvDatabase.CsvDataReader(CSV_File, CSV_Filter, CSV_Delimiter, bFirstRowHeader, CSV_TimeOut);
            iHeaderRowLength = dtrPage.FieldCount;
            while (dtrPage.Read())
            {
                if (iIndex >= iStartIndex && iIndex < iEndIndex)
                {                    
                    var item = Activator.CreateInstance<T>();
                    foreach (var property in typeof(T).GetProperties())
                    {
                        for (int i = 0; i < iHeaderRowLength; i++)
                        {
                            if (property.Name.ToUpper() == dtrPage.GetName(i).ToUpper())
                            {
                                Type convertTo = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                                property.SetValue(item, ConvertTo(dtrPage[i] + "", convertTo), null);
                                break;
                            }
                        }
                    }
                    lstCSV.Add(item);
                }
                if (iIndex > iEndIndex)
                {
                    break;
                }
                iIndex = iIndex + 1;
            }
            dtrPage.Close();
            return lstCSV;
        }

        /// <summary>
        /// Checks value and Type to return a safe value.
        /// </summary>
        /// <param name="sValue">Value of property</param>
        /// <param name="tType">Value Data Type</param>
        /// <returns>Returns safe value</returns>
        private static object ConvertTo(string sValue, Type tType)
        {
            if (tType.FullName != "System.String")
            {
                if (sValue != "")
                {
                    return Convert.ChangeType(sValue, tType);
                }
                else if (tType.FullName == "System.Decimal")
                {
                    return Decimal.Zero;
                }
                else
                {
                    return 0;
                }
            }
            return sValue;
        }
    }
}