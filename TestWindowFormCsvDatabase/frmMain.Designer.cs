namespace TestWindowFormCsvDatabase
{
    partial class frmMain
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.dgvCsv = new System.Windows.Forms.DataGridView();
            this.lblCsvDatabase = new System.Windows.Forms.Label();
            this.lsbCsv = new System.Windows.Forms.ListBox();
            this.lblStatus = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.btnRecordCount = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.dgvCsv)).BeginInit();
            this.SuspendLayout();
            // 
            // dgvCsv
            // 
            this.dgvCsv.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvCsv.Location = new System.Drawing.Point(442, 27);
            this.dgvCsv.Name = "dgvCsv";
            this.dgvCsv.Size = new System.Drawing.Size(442, 565);
            this.dgvCsv.TabIndex = 3;
            // 
            // lblCsvDatabase
            // 
            this.lblCsvDatabase.AutoSize = true;
            this.lblCsvDatabase.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblCsvDatabase.Location = new System.Drawing.Point(26, 1);
            this.lblCsvDatabase.Name = "lblCsvDatabase";
            this.lblCsvDatabase.Size = new System.Drawing.Size(105, 20);
            this.lblCsvDatabase.TabIndex = 4;
            this.lblCsvDatabase.Text = "CsvDatabase";
            // 
            // lsbCsv
            // 
            this.lsbCsv.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lsbCsv.FormattingEnabled = true;
            this.lsbCsv.ItemHeight = 20;
            this.lsbCsv.Location = new System.Drawing.Point(12, 27);
            this.lsbCsv.Name = "lsbCsv";
            this.lsbCsv.Size = new System.Drawing.Size(424, 224);
            this.lsbCsv.TabIndex = 5;
            this.lsbCsv.SelectedIndexChanged += new System.EventHandler(this.lsbCsv_SelectedIndexChanged);
            // 
            // lblStatus
            // 
            this.lblStatus.AutoSize = true;
            this.lblStatus.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblStatus.Location = new System.Drawing.Point(12, 259);
            this.lblStatus.Name = "lblStatus";
            this.lblStatus.Size = new System.Drawing.Size(0, 20);
            this.lblStatus.TabIndex = 6;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(455, 1);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(258, 20);
            this.label1.TabIndex = 7;
            this.label1.Text = "CsvDatabase Performance Results";
            // 
            // btnRecordCount
            // 
            this.btnRecordCount.Location = new System.Drawing.Point(204, 1);
            this.btnRecordCount.Name = "btnRecordCount";
            this.btnRecordCount.Size = new System.Drawing.Size(185, 23);
            this.btnRecordCount.TabIndex = 8;
            this.btnRecordCount.Text = "Record Count";
            this.btnRecordCount.UseVisualStyleBackColor = true;
            this.btnRecordCount.Click += new System.EventHandler(this.btnRecordCount_Click);
            // 
            // frmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(887, 604);
            this.Controls.Add(this.btnRecordCount);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.lblStatus);
            this.Controls.Add(this.lsbCsv);
            this.Controls.Add(this.lblCsvDatabase);
            this.Controls.Add(this.dgvCsv);
            this.Name = "frmMain";
            this.Text = "CsvDatabase Performance";
            this.Load += new System.EventHandler(this.frmMain_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dgvCsv)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        private System.Windows.Forms.DataGridView dgvCsv;
        private System.Windows.Forms.Label lblCsvDatabase;
        private System.Windows.Forms.ListBox lsbCsv;
        private System.Windows.Forms.Label lblStatus;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnRecordCount;
    }
}

