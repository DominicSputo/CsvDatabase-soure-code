using CsvDatabase;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using TestWebMVC.Models;

namespace TestWebMVC.Controllers
{
    public class HomeController : Controller
    {
        public async Task<ActionResult> Index()
        {
            //for (long i = 0; i < 20000000; i++)
            //{
            //    if (CsvDatabase.Helper.CheckCsvFileExist("C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt") == false)
            //    {
            //        await CsvCommand.UpdateAsync(new string[] { "TEST" }, "Header", new string[] { "TEST" }, new string[] { "Header" }, "C:\\Users\\Dominic Sputo\\Documents\\Visual Studio 2017\\Projects\\CSVDatabase\\TestCsvDatabase\\bin\\Debug\\TEST_223416.txt");
            //    }
            //}
            //List<Customer> lstCustomer = new List<Customer>();
            //for (int i = 0; i < 100; i++)
            //{
            //    lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            //    lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "SkY", LASTNAME = "Peter", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            //    lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Kelly", LASTNAME = "Ice", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            //}
            ////Create Customer CSV File if does Not Exist and insert Customer List into CSV File Customer.txt
            //await CsvBulk.InsertAsync(lstCustomer, Server.MapPath("~/App_Data/Customer.txt"), 0);
            //await CsvCommand.TruncateAsync(Server.MapPath("~/App_Data/Customer.txt"));
            await CsvCommand.TruncateAsync(Server.MapPath("~/App_Data/Customer.txt"));
            for (int i = 0; i < 10; i++)
            {
                //await CsvCommand.CreateTableAsync(new string[] { "ID", "FirstName", "LastName", "MiddleName", "PhoneNumber" }, Server.MapPath("~/App_Data/Customer.txt"), CSV_TimeOut: 0);
                await CsvCommand.InsertAsync(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" }, Server.MapPath("~/App_Data/Customer.txt"));
            }
            //for (int i = 0; i < 100; i++)
            //{
            //    await CsvCommand.InsertAsync(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" }, Server.MapPath("~/App_Data/Customer.txt"), 0);
            //}
            List<Customer> lstCustomerNewChanges = new List<Customer>();
            CsvDataReaderAsync dtr2 = new CsvDatabase.CsvDataReaderAsync(Server.MapPath("~/App_Data/Customer.txt"), "FIRSTNAME LIKE domini | firstname LiKe SK | lastnaMe LiKe Ic");
            //CsvDataReaderAsync dtr2 = new CsvDatabase.CsvDataReaderAsync(Server.MapPath("~/App_Data/Customer.txt"), "FIRSTNAME IN dominic|dominic");
            await dtr2.ExecuteReaderAsync();
            while (await dtr2.ReadAsync())
            {
                lstCustomerNewChanges.Add(new Customer() { ID = dtr2[0] + "", FIRSTNAME = dtr2[1] + "", LASTNAME = dtr2[2] + "", MIDDLENAME = dtr2[3] + "", PHONENUMBER = dtr2[4] + "", ROWCOUNT = dtr2.RecordCount.ToString() });
            }
            dtr2.Close();
            return View(lstCustomerNewChanges);
        }

        //public async Task<ActionResult> Index()
        //{
        //    CsvDatabase.CsvCommand.Rename(Server.MapPath("~/App_Data/Customer.txt"), "Customer2.txt");
        //    CsvDatabase.CsvCommand.Drop(Server.MapPath("~/App_Data/Customer.txt"));

        //    List<Customer> lstCustomer = new List<Customer>();
        //    lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" });
        //    lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "SkY", LASTNAME = "Peter", MIDDLENAME = "", PHONENUMBER = "8137505459" });
        //    lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Kelly", LASTNAME = "Ice", MIDDLENAME = "", PHONENUMBER = "8137505459" });
        //    //Create Customer CSV File if does Not Exist and insert Customer List into CSV File Customer.txt
        //    await CsvBulk.InsertAsync(lstCustomer, Server.MapPath("~/App_Data/Customer.txt"), 0);

        //    //Update Async Where Firstname = Dominic Set Firstname = Dominic2, LastName = Sputo2 for Customer.txt CSV File. Not case sensitive
        //    await CsvCommand.UpdateAsync(new string[] { "DoMINic" }, "FIRSTNAME", new string[] { "DOMINIc2", "Sputo2" }, new string[] { "FIRSTNAME", "LASTNAME" }, Server.MapPath("~/App_Data/Customer.txt"));

        //    //Delete Async Where Firstname = Sky. Not case sensitive
        //    await CsvCommand.DeleteAsync(new string[] { "sky" }, "FIRSTNAME", Server.MapPath("~/App_Data/Customer.txt"));

        //    Customer cCustomer3 = new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" };
        //    //Insert Async Customer Class cCustomer3 
        //    await CsvCommand.InsertAsync(cCustomer3, Server.MapPath("~/App_Data/Customer.txt"), 0);
        //    //Insert Async ID = 1, Firstname = Tod, Lastname = Yellow
        //    await CsvCommand.InsertAsync(new string[] { "1", "ToD", "YeLLoW" }, new string[] { "ID", "FIRSTNAME", "LASTNAME" }, Server.MapPath("~/App_Data/Customer.txt"));

        //    List<Customer> lstCustomerNewChanges = new List<Customer>();

        //    //Create DataReader Async Where FIRSTNAME IN dominic2 OR sky OR kelly OR tod
        //    CsvDataReaderAsync dtr = new CsvDatabase.CsvDataReaderAsync(Server.MapPath("~/App_Data/Customer.txt"), "FIRSTNAME IN dominic2 | sky | kelly | tod");
        //    await dtr.ExecuteReaderAsync();
        //    while (await dtr.ReadAsync())
        //    {
        //        lstCustomerNewChanges.Add(new Customer() { ID = dtr[0] + "", FIRSTNAME = dtr[1] + "", LASTNAME = dtr[2] + "", MIDDLENAME = dtr[3] + "", PHONENUMBER = dtr[4] + "" });
        //    }
        //    dtr.Close();

        //    //Create Async Table Customer8 CSV File.
        //    await CsvCommand.CreateTableAsync(new string[] { "ID", "FIRSTNAME", "LASTNAME", "MIDDLENAME", "PHONENUMBER" }, Server.MapPath("~/App_Data/Customer8.txt"));

        //    //Create DataReader Async Where FIRSTNAME LIKE domini OR firstname LiKe SK OR lastnaMe LiKe Ic
        //    CsvDataReaderAsync dtr2 = new CsvDatabase.CsvDataReaderAsync(Server.MapPath("~/App_Data/Customer.txt"), "FIRSTNAME LIKE domini | firstname LiKe SK | lastnaMe LiKe Ic");
        //    await dtr2.ExecuteReaderAsync();
        //    while (await dtr2.ReadAsync())
        //    {
        //        lstCustomerNewChanges.Add(new Customer() { ID = dtr2[0] + "", FIRSTNAME = dtr2[1] + "", LASTNAME = dtr2[2] + "", MIDDLENAME = dtr2[3] + "", PHONENUMBER = dtr2[4] + "" });
        //    }
        //    dtr2.Close();
        //    return View(lstCustomerNewChanges);
        //}

        //public ActionResult Index()
        //{            
        //    //CsvDatabase.CsvCommand.Truncate(Server.MapPath("~/App_Data/Customer.txt"));
        //    List<Customer> lstCustomer = new List<Customer>();
        //    for (int i = 0; i < 1000; i++)
        //    {
        //        lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" });
        //        lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "SkY", LASTNAME = "Peter", MIDDLENAME = "", PHONENUMBER = "8137505459" });
        //        lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Kelly", LASTNAME = "Ice", MIDDLENAME = "", PHONENUMBER = "8137505459" });
        //    }
        //    ////Create Customer CSV File if does Not Exist and insert Customer List into CSV File Customer.txt
        //    CsvBulk.Insert(lstCustomer, Server.MapPath("~/App_Data/Customer.txt"), 0);
        //    for (int i = 0; i < 100; i++)
        //    {
        //        CsvCommand.Insert(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" }, Server.MapPath("~/App_Data/Customer.txt"), 0);
        //    }
        //    for (int i = 0; i < 100; i++)
        //    {
        //        CsvCommand.Insert(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "SkY", LASTNAME = "Peter", MIDDLENAME = "", PHONENUMBER = "8137505459" }, Server.MapPath("~/App_Data/Customer.txt"), 0);
        //    }
        //    int iROWCOUNT = 0;
        //    List<Customer> lstCustomerNewChanges = new List<Customer>();
        //    CsvDataReader dtr2 = new CsvDatabase.CsvDataReader(Server.MapPath("~/App_Data/Customer.txt"), "FIRSTNAME LIKE domini | firstname LiKe SK | lastnaMe LiKe Ic");
        //    while (dtr2.Read())
        //    {
        //        iROWCOUNT = iROWCOUNT + 1;
        //        lstCustomerNewChanges.Add(new Customer() { ID = dtr2[0] + "", FIRSTNAME = dtr2[1] + "", LASTNAME = dtr2[2] + "", MIDDLENAME = dtr2[3] + "", PHONENUMBER = dtr2[4] + "", ROWCOUNT = iROWCOUNT.ToString() });
        //    }
        //    dtr2.Close();
        //    return View(lstCustomerNewChanges);
        //}


        //public ActionResult Index()
        //{
        //    CsvCommand.Truncate(Server.MapPath("~/App_Data/Customer.txt"));
        //    for (int i = 0; i < 100; i++)
        //    {
        //        //await CsvCommand.CreateTableAsync(new string[] { "ID", "FirstName", "LastName", "MiddleName", "PhoneNumber" }, Server.MapPath("~/App_Data/Customer.txt"), CSV_TimeOut: 0);
        //        CsvCommand.Insert(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" }, Server.MapPath("~/App_Data/Customer.txt"));
        //    }
        //    List<Customer> lstCustomerNewChanges = new List<Customer>();
        //    //CsvDataReader dtr2 = new CsvDatabase.CsvDataReader(Server.MapPath("~/App_Data/Customer.txt"), "firstName liKE DominiC | lastname Like sputo");
        //    //CsvDataReader dtr2 = new CsvDatabase.CsvDataReader(Server.MapPath("~/App_Data/Customer.txt"), "firstName IN Domini | DominiC");
        //    //CsvDataReader dtr2 = new CsvDatabase.CsvDataReader(Server.MapPath("~/App_Data/Customer.txt"), "firstName <> Domini");
        //    CsvDataReader dtr2 = new CsvDatabase.CsvDataReader(Server.MapPath("~/App_Data/Customer.txt"), "firstName = DominiC");
        //    while (dtr2.Read())
        //    {
        //        lstCustomerNewChanges.Add(new Customer() { ID = dtr2[0] + "", FIRSTNAME = dtr2[1] + "", LASTNAME = dtr2[2] + "", MIDDLENAME = dtr2[3] + "", PHONENUMBER = dtr2[4] + "", ROWCOUNT = dtr2.RecordCount.ToString() });
        //    }
        //    dtr2.Close();
        //    return View(lstCustomerNewChanges);
        //}
    }
}