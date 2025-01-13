using System;
using System.Linq;
using System.IO;
using System.Data;
using System.Threading.Tasks;

namespace CsvDatabase
{
    public class CsvDataReaderAsync : IDataReader, IDisposable
    {
        public bool NextResult()
        {
            throw new NotImplementedException();
        }
        public bool Read()
        {
            throw new NotImplementedException();
        }
        private FileStream _fileStream = null;
        private StreamReader _file = null;
        private char _delimiter;
        /* stores the header and values of csv and also virtual*/
        private string _virtualHeaderString = "", _csvHeaderstring = "", _csvlinestring = "", _virtuallineString = "";
        
        private string[] _header;

        private string _sEquals_Filter = null;        
        private string[] _sarIN_Filter = null;
        private int _iHeader_Index_Filter = 0;
        //private int _iHeader2_Index_Filter = 0;
        private bool _bFoundHeader = false;
        private bool _bIN_Operator = false;
        private bool _bEquals_Operator = false;
        private bool _bNotEquals_Operator = false; //New
        private bool _bLike_Operator = false;
        //private bool _bLikeOrLike_Operator = false;
        private bool _bPipeLike_Operator = false;
        private bool _bFoundDelimiter = false;
        private bool _bFileNotFound = false;
        /// <summary>
        /// Returns an array of header names as string in the order of columns 
        /// from left to right of csv file. This can be manually renamed calling 
        /// 'RenameCSVHeader'
        /// </summary>
        public string[] Header
        {
            get { return _header; }
        }

        /*
         * The values of header and values must be in same order. So using this collection.
         * This collection stores header key as header name and its related value as value. 
         * When the value of a specific 
         * header is updated the specific key value will be updated. 
         * For Original Header values from the csv file the values will be null. 
         * This is used as a check and identify this is a csv value or not.
         */
        private System.Collections.Specialized.OrderedDictionary headercollection = new System.Collections.Specialized.OrderedDictionary();

        private string[] _line;

        /// <summary>
        /// Returns an array of strings from the current line of csv file. 
        /// Call Read() method to read the next line/record of csv file. 
        /// </summary>
        public string[] Line
        {
            get
            {
                return _line;
            }
        }

        private long recordsaffected;
        private bool _iscolumnlocked = false;
        private string _CSV_Filter;
        private bool _bHeaderRead = false;
        private bool _bFirstRowHeader = true;

        private string __CSV_File;
        private string __CSV_Filter = "";
        private char __CSV_Delimiter = '|';
        private bool __bFirstRowHeader = true;
        private int __CSV_TimeOut = 5;

        /// <summary>
        /// Create Instance of CSV DataReader Async.
        /// </summary>
        /// <param name="CSV_FilePath">Path to the csv file.</param>
        /// <param name="CSV_Filter">Filter the CSV File using HEADER = VALUE, HEADER IN (VALUE1, VALUE2, etc), or HEADER LIKE VALUE.</param>
        /// <param name="CSV_Delimiter">Delimiter character used in CSV file.</param>
        /// <param name="CSV_TimeOut">Number of Seconds before searching for CSV File will end with File Not Found. Default 5 seconds.</param>
        /// <param name="bFirstRowHeader">Specify the csv got a header in first row or not. Default is true and if argument is false then auto header 'ROW_xx will be used as per the order of columns.</param>
        public CsvDataReaderAsync(string CSV_File, string CSV_Filter = "", char CSV_Delimiter = '|', bool bFirstRowHeader = true, int CSV_TimeOut = 5)
        {
            __CSV_File = CSV_File;
            __CSV_Filter = CSV_Filter;
            __CSV_Delimiter = CSV_Delimiter;
            __bFirstRowHeader = bFirstRowHeader;
            __CSV_TimeOut = CSV_TimeOut;
        }

        /// <summary>
        /// Executes CSV DataReader Async.
        /// </summary>       
        public async Task ExecuteReaderAsync()
        {
            string CSV_TempFile = System.IO.Path.GetDirectoryName(__CSV_File) + "\\" + System.IO.Path.GetFileNameWithoutExtension(__CSV_File).ToUpper().Trim() + "_#_TEMPFILE" + System.IO.Path.GetExtension(__CSV_File);
            _bFileNotFound = Helper.CheckAbandonedTempFiles(__CSV_File, CSV_TempFile);
            if (_bFileNotFound)
            {
                return;
            }
            string CSV_File = __CSV_File;
            string CSV_Filter = __CSV_Filter;
            char CSV_Delimiter = __CSV_Delimiter;
            bool bFirstRowHeader = __bFirstRowHeader;
            int CSV_TimeOut = __CSV_TimeOut;
            _bFirstRowHeader = bFirstRowHeader;
            _CSV_Filter = CSV_Filter;
            bool bFileLocked = true;
            DateTime dtStart = DateTime.Now;
            bool bCSVFound = false;
            while (bFileLocked)
            {
                try
                {
                    if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                    {
                        bCSVFound = true;
                    }
                    _fileStream = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.Read);
                    _file = new StreamReader(_fileStream);
                    bFileLocked = false;
                }
                catch
                {
                    if (bCSVFound == false) //Checks if CSV Not Found for 10 seconds
                    {
                        TimeSpan tsInterval = DateTime.Now - dtStart;
                        if (tsInterval.Seconds >= CSV_TimeOut)
                        {
                            throw new Exception("CSV File Not Found");
                        }
                    }
                    Close();
                    bFileLocked = true;
                }
            }
            _delimiter = CSV_Delimiter;
            if (bFirstRowHeader == true)
            {
                _bHeaderRead = true;
                await ReadAsync();
                _csvHeaderstring = _csvlinestring;
                _header = ReadRow(_csvHeaderstring);
                int iHeaderLength = _header.Length;
                for (int i = 0; i < iHeaderLength; i++) //check for duplicate headers and create a header record.
                {
                    if (headercollection.Contains(_header[i].ToUpper()) == true)
                        throw new Exception("Duplicate found in CSV header. Cannot create a CSV reader instance with duplicate header");
                    headercollection.Add(_header[i].ToUpper(), null);
                }
            }
            else
            {
                //just open and close the file with read of first line to determine how many 
                //rows are there and then add default rows as  row1,row2 etc to collection.
                _bHeaderRead = true;
                await ReadAsync();
                _csvHeaderstring = _csvlinestring;
                _header = ReadRow(_csvHeaderstring);
                _csvHeaderstring = "";
                int iHeaderLength = _header.Length;
                for (int i = 0; i < iHeaderLength; i++)//read each column and create a dummy header.
                {
                    headercollection.Add("COL_" + i.ToString(), null);
                    _csvHeaderstring = _csvHeaderstring + "COL_" + i.ToString() + _delimiter;
                }
                _csvHeaderstring.TrimEnd(_delimiter);
                _header = ReadRow(_csvHeaderstring);
                Close(); //close and repoen to get the record position to beginning.
                         //_file = File.OpenText(filePath);
                bFileLocked = true;
                while (bFileLocked)
                {
                    try
                    {
                        if (File.Exists(CSV_File) == true || File.Exists(CSV_TempFile) == true)
                        {
                            bCSVFound = true;
                        }
                        _fileStream = new FileStream(CSV_File, FileMode.Open, FileAccess.Read, FileShare.Read);
                        _file = new StreamReader(_fileStream);
                        bFileLocked = false;
                    }
                    catch
                    {
                        if (bCSVFound == false) //Checks if CSV Not Found for 10 seconds
                        {
                            TimeSpan tsInterval = DateTime.Now - dtStart;
                            if (tsInterval.Seconds >= CSV_TimeOut)
                            {
                                throw new Exception("CSV File Not Found");
                            }
                        }
                        Close();
                        bFileLocked = true;
                    }
                }               
            }
            _iscolumnlocked = false; //setting this to false since above read is called 
                                     //internally during constructor and actual user read() didnot start.
            _csvlinestring = "";
            _line = null;
            recordsaffected = 0;
        }

        public async Task<bool> ReadAsync()
        {
            if (_bFileNotFound)
            {
                return false;
            }
            if (_file == null)
            {
                return false;
            }
            var result = !_file.EndOfStream;
            if (result == false)
            {
                if (_iscolumnlocked == false)
                    _iscolumnlocked = true;
                Close();
                return result;
            }

            if (_bHeaderRead)
            {
                _bHeaderRead = false;
                _csvlinestring = await _file.ReadLineAsync();
                _bFoundDelimiter = true;
                FindDelimiter(_csvlinestring, ref _delimiter);
                if (_virtuallineString == "")
                    _line = ReadRow(_csvlinestring);
                else
                    _line = ReadRow(_csvlinestring + _delimiter + _virtuallineString);                
                if (_iscolumnlocked == false)
                    _iscolumnlocked = true;
                return result;
            }

            if (_CSV_Filter == "")
            {
                _csvlinestring = await _file.ReadLineAsync();
                if (!_bFoundDelimiter)
                {
                    FindDelimiter(_csvlinestring, ref _delimiter);
                    _bFoundDelimiter = true;
                }
                if (_virtuallineString == "")
                    _line = ReadRow(_csvlinestring);
                else
                    _line = ReadRow(_csvlinestring + _delimiter + _virtuallineString);
                recordsaffected++;
                if (_iscolumnlocked == false)
                    _iscolumnlocked = true;
                return result;
            }
            else
            { //Filter Data We have a Filter
                if (_bEquals_Operator == false && _bIN_Operator == false && _bLike_Operator == false && _bPipeLike_Operator == false && _bNotEquals_Operator == false) //New
                {
                    _CSV_Filter = _CSV_Filter.Trim().ToUpper();
                    string sFilter = _CSV_Filter;
                    int iIN = sFilter.IndexOf(" IN ");
                    int iEqual = sFilter.IndexOf("=");
                    int iLike = sFilter.IndexOf(" LIKE ");
                    int iNotEqual = sFilter.IndexOf(" <> "); //new
                    int iPipe = sFilter.IndexOf("|");
                    if (iIN == -1)
                    {
                        iIN = 99999;
                    }
                    if (iEqual == -1)
                    {
                        iEqual = 99999;
                    }
                    if (iLike == -1)
                    {
                        iLike = 99999;
                    }
                    if (iIN < iEqual && iIN < iLike)
                    {
                        _bIN_Operator = true;
                        sFilter = sFilter.Substring(0, iIN).Trim();
                    }
                    else if (iEqual < iIN && iEqual < iLike)
                    {
                        _bEquals_Operator = true;
                        sFilter = sFilter.Substring(0, iEqual).Trim();
                    }
                    else if (iLike < iIN && iLike < iEqual)
                    {
                        _bLike_Operator = true;
                        sFilter = sFilter.Substring(0, iLike).Trim();
                    }
                    string sHeader = _CSV_Filter.Substring(0, _CSV_Filter.IndexOf(" "));
                    foreach (System.Collections.DictionaryEntry de in headercollection)
                    {
                        if (de.Key.ToString() == sHeader)
                        {
                            _bFoundHeader = true;
                            break;
                        }
                        _iHeader_Index_Filter = _iHeader_Index_Filter + 1;
                    }               
                    if (_bLike_Operator && iPipe != -1) //new
                    {
                        _bPipeLike_Operator = true;
                        _bLike_Operator = false;
                    }
                    if (iNotEqual != -1 && _bIN_Operator == false && _bEquals_Operator == false && _bLike_Operator == false && _bPipeLike_Operator == false) //NEW
                    {
                        _bNotEquals_Operator = true;
                    }
                }
                if (_bFoundHeader)
                {
                    if (_bEquals_Operator)
                    { //Filter out the =  
                        if (_file != null)
                        {
                            if (!_file.EndOfStream)
                            {
                                bool bFound = false;
                                while (!bFound)
                                {
                                    if (_file == null)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false; //result = !_file.EndOfStream;
                                    }
                                    if (_file.EndOfStream == true)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false;
                                    }
                                    if (_sEquals_Filter == null)
                                    {
                                        _sEquals_Filter = _CSV_Filter.Substring(_CSV_Filter.IndexOf('=') + 1, _CSV_Filter.Length - (_CSV_Filter.IndexOf('=') + 1)).Trim(); //.Replace("'", "").Replace("\"", "").Trim();
                                    }
                                    string sCSV_Row = await _file.ReadLineAsync();
                                    if (_virtuallineString != "")
                                    {
                                        sCSV_Row = sCSV_Row + _delimiter + _virtuallineString;
                                    }
                                    string[] sarCSV_Row = ReadRow(sCSV_Row);
                                    if (sarCSV_Row != null)
                                    {
                                        if (_sEquals_Filter == sarCSV_Row[_iHeader_Index_Filter].Trim().ToUpper()) //.Replace("'", "").Replace("\"", "").Trim().ToUpper())
                                        {
                                            _line = sarCSV_Row;
                                            recordsaffected++;
                                            return result;
                                        }
                                    }                              
                                }                     
                            }
                            else
                            {
                                Close();
                                if (_iscolumnlocked == false)
                                    _iscolumnlocked = true;
                                return false;
                            }
                        }
                        Close();
                        if (_iscolumnlocked == false)
                            _iscolumnlocked = true;
                        return false;
                    }
                    else if (_bNotEquals_Operator) //New
                    { //Filter out the <>  
                        if (_file != null)
                        {
                            if (!_file.EndOfStream)
                            {
                                bool bFound = false;
                                while (!bFound)
                                {
                                    if (_file == null)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false; //result = !_file.EndOfStream;
                                    }
                                    if (_file.EndOfStream == true)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false;
                                    }
                                    if (_sEquals_Filter == null)
                                    {
                                        _sEquals_Filter = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" <> ") + 4, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" <> ") + 4)).Trim(); //.Replace("'", "").Replace("\"", "").Trim();
                                    }
                                    string sCSV_Row = await _file.ReadLineAsync();
                                    if (_virtuallineString != "")
                                    {
                                        sCSV_Row = sCSV_Row + _delimiter + _virtuallineString;
                                    }
                                    string[] sarCSV_Row = ReadRow(sCSV_Row);
                                    if (sarCSV_Row != null)
                                    {
                                        if (_sEquals_Filter != sarCSV_Row[_iHeader_Index_Filter].Trim().ToUpper()) //NEw //.Replace("'", "").Replace("\"", "").Trim().ToUpper())
                                        {
                                            _line = sarCSV_Row;
                                            recordsaffected++;
                                            return result;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                Close();
                                if (_iscolumnlocked == false)
                                    _iscolumnlocked = true;
                                return false;
                            }
                        }
                        Close();
                        if (_iscolumnlocked == false)
                            _iscolumnlocked = true;
                        return false;
                    }
                    else if (_bIN_Operator)
                    { //Filter out the IN
                        if (_file != null)
                        {
                            if (!_file.EndOfStream)
                            {
                                bool bFound = false;
                                while (!bFound)
                                {
                                    if (_file == null)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false; 
                                    }
                                    if (_file.EndOfStream == true)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false;
                                    }

                                    if (_sarIN_Filter == null)
                                    {  
                                        _CSV_Filter = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" IN ") + 4, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" IN ") + 4)).Trim().ToUpper();
                                        if (_CSV_Filter.Substring(0, 1) == "(")
                                        {
                                            _CSV_Filter = _CSV_Filter.Substring(1, _CSV_Filter.Length - 1);
                                        }
                                        if (_CSV_Filter.Substring(_CSV_Filter.Length - 1, 1) == ")")
                                        {
                                            _CSV_Filter = _CSV_Filter.Substring(0, _CSV_Filter.Length - 1);
                                        }
                                        _sarIN_Filter = _CSV_Filter.Split('|').Select(p => p.Trim()).ToArray();
                                    }
                                    int iINLength = _sarIN_Filter.Length;

                                    string sCSV_Row = await _file.ReadLineAsync();

                                    if (_virtuallineString != "")
                                    {
                                        sCSV_Row = sCSV_Row + _delimiter + _virtuallineString;
                                    }
                                    string[] sarCSV_Row = ReadRow(sCSV_Row);
                                    if (sarCSV_Row != null)
                                    {
                                        for (int i = 0; i < iINLength; i++)
                                        {
                                            if (_sarIN_Filter[i].Trim() == sarCSV_Row[_iHeader_Index_Filter].ToUpper().Trim()) //.ToUpper().Replace("'", "").Replace("\"", "").Replace("(", "").Replace(")", ""))
                                            {
                                                _line = sarCSV_Row;
                                                recordsaffected++;
                                                return result;
                                            }
                                        }
                                    }
                                }
                            }
                            else
                            {
                                Close();
                                if (_iscolumnlocked == false)
                                    _iscolumnlocked = true;
                                return false;
                            }
                        }
                        Close();
                        if (_iscolumnlocked == false)
                            _iscolumnlocked = true;
                        return false;
                    }
                    else if (_bLike_Operator)
                    { //Filter out the LIKE  
                        if (_file != null)
                        {
                            if (!_file.EndOfStream)
                            {
                                bool bFound = false;
                                while (!bFound)
                                {
                                    if (_file == null)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false; //result = !_file.EndOfStream;
                                    }
                                    if (_file.EndOfStream == true)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false;
                                    }
                                    if (_sEquals_Filter == null)
                                    {
                                        _sEquals_Filter = _CSV_Filter.Substring(_CSV_Filter.IndexOf(" LIKE ") + 6, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" LIKE ") + 6)).Trim(); //.Replace("'", "").Replace("\"", "").Trim();
                                    }
                                    string sCSV_Row = await _file.ReadLineAsync();
                                    if (_virtuallineString != "")
                                    {
                                        sCSV_Row = sCSV_Row + _delimiter + _virtuallineString;
                                    }
                                    string[] sarCSV_Row = ReadRow(sCSV_Row);
                                    if (sarCSV_Row != null)
                                    {
                                        if (sarCSV_Row[_iHeader_Index_Filter].Trim().ToUpper().Contains(_sEquals_Filter)) //.Replace("'", "").Replace("\"", "").Trim().ToUpper())
                                        {
                                            _line = sarCSV_Row;
                                            recordsaffected++;
                                            return result;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                Close();
                                if (_iscolumnlocked == false)
                                    _iscolumnlocked = true;
                                return false;
                            }
                        }
                        Close();
                        if (_iscolumnlocked == false)
                            _iscolumnlocked = true;
                        return false;
                    }
                    else if (_bPipeLike_Operator)
                    {
                        if (_file != null)
                        {
                            if (!_file.EndOfStream)
                            {
                                bool bFound = false;
                                while (!bFound)
                                {
                                    if (_file == null)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false;
                                    }
                                    if (_file.EndOfStream == true)
                                    {
                                        Close();
                                        if (_iscolumnlocked == false)
                                            _iscolumnlocked = true;
                                        return false;
                                    }
                                    string[] sarEqualsFilter = _CSV_Filter.Split('|');
                                    int iEqualsFilterLength = sarEqualsFilter.Length;

                                    string sCSV_Row = await _file.ReadLineAsync();
                                    for (int i = 0; i < iEqualsFilterLength; i++)
                                    {
                                        _iHeader_Index_Filter = 0;
                                        string sHeader = sarEqualsFilter[i].Trim().Substring(0, sarEqualsFilter[i].Trim().IndexOf(" "));
                                        foreach (System.Collections.DictionaryEntry de in headercollection)
                                        {
                                            if (de.Key.ToString() == sHeader)
                                            {
                                                _bFoundHeader = true;
                                                break;
                                            }
                                            _iHeader_Index_Filter = _iHeader_Index_Filter + 1;
                                        }
                                        _sEquals_Filter = sarEqualsFilter[i].Substring(sarEqualsFilter[i].IndexOf(" LIKE ") + 6, sarEqualsFilter[i].Length - (sarEqualsFilter[i].IndexOf(" LIKE ") + 6)).Trim(); //.Replace("'", "").Replace("\"", "").Trim();
                                        //if (_sEquals_Filter == null)
                                        //{
                                        //    _sEquals_Filter = sarEqualsFilter[0].Substring(_CSV_Filter.IndexOf(" LIKE ") + 6, _CSV_Filter.Length - (_CSV_Filter.IndexOf(" LIKE ") + 6)).Trim(); //.Replace("'", "").Replace("\"", "").Trim();
                                        //}
                                        //string sCSV_Row = _file.ReadLine();

                                        if (_virtuallineString != "")
                                        {
                                            sCSV_Row = sCSV_Row + _delimiter + _virtuallineString;
                                        }
                                        string[] sarCSV_Row = ReadRow(sCSV_Row);
                                        if (sarCSV_Row != null)
                                        {
                                            if (sarCSV_Row[_iHeader_Index_Filter].Trim().ToUpper().Contains(_sEquals_Filter)) //.Replace("'", "").Replace("\"", "").Trim().ToUpper())
                                            {
                                                _line = sarCSV_Row;
                                                recordsaffected++;
                                                return result;
                                            }
                                        }
                                    }
                                }
                            }
                            else
                            {
                                Close();
                                if (_iscolumnlocked == false)
                                    _iscolumnlocked = true;
                                return false;
                            }
                        }
                        Close();
                        if (_iscolumnlocked == false)
                            _iscolumnlocked = true;
                        return false;
                    }
                }
                else
                { //Header Not Found Dont Return Data
                    return result;
                }
            }
            return result;
        }

        /// <summary>
        /// Adds a new virtual column at the beginning of each row. 
        /// If a virtual column exists then the new one is placed left of the first one. 
        /// Adding virtual column is possible only before read is made.
        /// </summary>
        /// <param name="columnName">Name of the header of column</param>
        /// <param name="value">Value for this column. This will be returned for every row 
        /// for this column until the value for this column is changed through method 
        /// 'UpdateVirtualcolumnValues'</param>
        /// <returns>Success status</returns>
        public bool AddVirtualColumn(string columnName, string value)
        {
            columnName = columnName.ToUpper();
            if (value == null)
                return false;
            if (_iscolumnlocked == true)
                throw new Exception("Cannot add new records after Read() is called.");
            if (headercollection.Contains(columnName) == true)
                throw new Exception("Duplicate found in CSV header. Cannot create a CSV readerinstance with duplicate header");
            headercollection.Add(columnName, value); //add this to main collection so that 
                                                     //we can check for duplicates next time col is added.

            if (_virtualHeaderString == "")
                _virtualHeaderString = columnName;
            else
                _virtualHeaderString = columnName + _delimiter + _virtualHeaderString;
            _header = ReadRow(_csvHeaderstring + _delimiter + _virtualHeaderString);

            if (_virtuallineString == "")
                _virtuallineString = value;
            else
                _virtuallineString = value + _delimiter + _virtuallineString;
            _line = ReadRow(_csvlinestring + _delimiter + _virtuallineString);
            return true;
        }

        /// <summary>
        /// Update the column header. This method must be called before Read() method is called. 
        /// Otherwise it will throw an exception.
        /// </summary>
        /// <param name="columnName">Name of the header of column</param>
        /// <param name="value">Value for this column. This will be returned for every row 
        /// for this column until the value for this column is changed through method 
        /// 'UpdateVirtualcolumnValues'</param>
        /// <returns>Success status</returns>
        public bool RenameCSVHeader(string oldColumnName, string newColumnName)
        {
            oldColumnName = oldColumnName.ToUpper();
            newColumnName = newColumnName.ToUpper();
            if (_iscolumnlocked == true)
                throw new Exception("Cannot update header after Read() is called.");
            if (headercollection.Contains(oldColumnName) == false)
                throw new Exception("CSV header not found. Cannot update.");
            string value = headercollection[oldColumnName] == null ?
            null : headercollection[oldColumnName].ToString();
            int i = 0;
            foreach (var item in headercollection.Keys) //this collection does no have a position 
            {                                           //location property so using this way assuming the key is ordered
                if (item.ToString() == oldColumnName)
                    break;
                i++;
            }
            headercollection.RemoveAt(i);
            headercollection.Insert(i, newColumnName, value);
            if (value == null) //csv header update.
            {
                _csvHeaderstring = _csvHeaderstring.Replace(oldColumnName, newColumnName);
                _header = ReadRow(_csvHeaderstring + _delimiter + _virtualHeaderString);
            }
            else //virtual header update
            {
                _virtualHeaderString = _virtualHeaderString.Replace(oldColumnName, newColumnName);
                _header = ReadRow(_csvHeaderstring + _delimiter + _virtualHeaderString);
            }
            return true;
        }

        /// <summary>
        /// Updates the value of the virtual column if it exists. Else throws exception.
        /// </summary>
        /// <param name="columnName">Name of the header of column</param>
        /// <param name="value">Value for this column. 
        /// This new value will be returned for every row for this column until 
        /// the value for this column is changed again</param>
        /// <returns>Success status</returns>
        public bool UpdateVirtualColumnValue(string columnName, string value)
        {
            columnName = columnName.ToUpper();
            if (value == null)
                return false;
            if (headercollection.Contains(columnName) == false)
                throw new Exception("Unable to find the csv header. Cannot update value.");
            if (headercollection.Contains(columnName) == true && headercollection[columnName] == null)
                throw new Exception("Cannot update values for default csv based columns.");
            headercollection[columnName] = value; //add this to main collection so that 
                                                  //we can check for duplicates next time col is added.
            _virtuallineString = "";
            foreach (var item in headercollection.Values)   //cannot use string.replace since 
            {                                               //values may be duplicated and can update wrong column. So rebuilding the string.
                if (item != null)
                {
                    _virtuallineString = (string)item + _delimiter + _virtuallineString;
                }
            }
            _virtuallineString = _virtuallineString.TrimEnd(','); //!!!!!!!!!!!!!!!!!!!!<<<<<<<<<<<<<<<<<<<NOT REALLY SURE IF THIS IS SUPPOSE TO BE _delimiter
            _line = ReadRow(_csvlinestring + _delimiter + _virtuallineString);
            return true;
        }

        /// <summary>
        /// Reads a row of data from a CSV file
        /// </summary>
        /// <returns>array of strings from csv line</returns>
        private string[] ReadRow(string line)
        {
            if (String.IsNullOrEmpty(line) == true)
                return null;

            return line.Split(_delimiter);
        }

        public void Close()
        {
            if (_file != null)
            {
                _file.Dispose();
                _file = null;
            }
            if (_fileStream != null)
            {
                _fileStream.Dispose();
                _fileStream = null;
            }            
        }

        /// <summary>
        /// Gets a value that indicates the depth of nesting for the current row.
        /// </summary>
        public int Depth
        {
            get { return 1; }
        }

        public DataTable GetSchemaTable()
        {
            DataTable t = new DataTable();
            foreach (string s in Header)
            {
                t.Columns.Add(s);
            }
            //t.Rows.Add(Header);
            return t;
        }

        public bool IsClosed
        {
            get { return _file == null; }
        }

        public async Task<bool> NextResultAsnyc()
        {
            return await ReadAsync();
        }

        /// <summary>
        /// Returns how many records read so far.
        /// </summary>
        public int RecordsAffected
        {
            get { return int.Parse(recordsaffected.ToString()); }
        }

        /// <summary>
        /// Returns how many records read so far.
        /// </summary>
        public long RecordCount
        {
            get { return recordsaffected; }
        }

        public void Dispose()
        {
            if (_file != null)
            {
                _file.Dispose();
                _file = null;
            }
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public int FieldCount
        {
            get { return Header.Length; }
        }

        public bool GetBoolean(int i)
        {
            return Boolean.Parse(Line[i]);
        }

        public byte GetByte(int i)
        {
            return Byte.Parse(Line[i]);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return Char.Parse(Line[i]);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            return (IDataReader)this;
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            return DateTime.Parse(Line[i]);
        }

        public decimal GetDecimal(int i)
        {
            return Decimal.Parse(Line[i]);
        }

        public double GetDouble(int i)
        {
            return Double.Parse(Line[i]);
        }

        public Type GetFieldType(int i)
        {
            return typeof(String);
        }

        public float GetFloat(int i)
        {
            return float.Parse(Line[i]);
        }

        public Guid GetGuid(int i)
        {
            return Guid.Parse(Line[i]);
        }

        public short GetInt16(int i)
        {
            return Int16.Parse(Line[i]);
        }

        public int GetInt32(int i)
        {
            return Int32.Parse(Line[i]);
        }

        public long GetInt64(int i)
        {
            return Int64.Parse(Line[i]);
        }

        public string GetName(int i)
        {
            return Header[i];
        }

        public int GetOrdinal(string name)
        {
            int result = -1;
            for (int i = 0; i < Header.Length; i++)
                if (Header[i].ToUpper().Trim() == name.ToUpper().Trim())
                {
                    result = i;
                    break;
                }
            return result;
        }

        public string GetString(int i)
        {
            return Line[i];
        }

        public object GetValue(int i)
        {
            return Line[i];
        }

        public int GetValues(object[] values)
        {
            values = Line;
            return 1;
        }

        public bool IsDBNull(int i)
        {
            return string.IsNullOrWhiteSpace(Line[i]);
        }

        public object this[string name]
        {
            get { return Line[GetOrdinal(name)]; }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
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
    }
}