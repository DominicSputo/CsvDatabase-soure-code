using CsvDatabase;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace TestWebFormAspNet
{
    public partial class Index : System.Web.UI.Page
    {

        public class Customer
        {
            public string ID { get; set; }
            public string FIRSTNAME { get; set; }
            public string LASTNAME { get; set; }
            public string MIDDLENAME { get; set; }
            public string PHONENUMBER { get; set; }
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            //string sCSV_File = Server.MapPath("~/App_Data/Customerfff.txt");
            //dgrCustomer.DataSource = CsvReader.GetRecords<Customer>(sCSV_File, "firstname Like DOMInic | lastName Like PEter");
            //dgrCustomer.DataBind();


            //string sCSV_File = Server.MapPath("~/App_Data/Customerfff.txt");
            //dgrCustomer.DataSource = CsvDataTable.GetDataTable(sCSV_File, "firstname Like DOMInic | lastName Like PEter");
            //dgrCustomer.DataBind();


            string sCSV_File = Server.MapPath("~/App_Data/Customer.txt");
            List<Customer> lstCustomer = new List<Customer>();
            lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Dominic", LASTNAME = "Sputo", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "SkY", LASTNAME = "Peter", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            lstCustomer.Add(new Customer() { ID = Guid.NewGuid().ToString(), FIRSTNAME = "Kelly", LASTNAME = "Ice", MIDDLENAME = "", PHONENUMBER = "8137505459" });
            //Create Customer CSV File if does Not Exist and insert Customer List into CSV File Customer.txt
            CsvBulk.Insert(lstCustomer, sCSV_File);
            dgrCustomer.DataSource = CsvReader.GetRecords<Customer>(sCSV_File, "firstname Like DOMInic | lastName Like PEter");
            dgrCustomer.DataBind();
        }
    }
}