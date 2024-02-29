const { Sequelize } = require("sequelize");
require("dotenv").config();
const _ = require("lodash");
const fs = require("fs");
const fsExtra = require("fs.extra");
const spawn = require("child_process").spawn;
const express = require("express");
const app = express();
const port = 3002;
const archiver = require('archiver');
const ExcelJS = require("exceljs"); 

const sequelize = new Sequelize(
  process.env.DB_DATABASE_ATOME,
  process.env.DB_USERNAME_ATOME,
  process.env.DB_PASSWORD_ATOME,
  {
    host: process.env.DB_HOST_ATOME,
    port: process.env.DB_PORT_ATOME,
    dialect: process.env.DB_CONNECTION_ATOME,
  }
);

const sequelize_spl = new Sequelize(
  process.env.DB_DATABASE_SPL,
  process.env.DB_USERNAME_SPL,
  process.env.DB_PASSWORD_SPL,
  {
    host: process.env.DB_HOST_SPL,
    port: process.env.DB_PORT_SPL,
    dialect: process.env.DB_CONNECTION_SPL,
  }
);

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Metode untuk generate Excel
const generateExcel = async (filename, data, dirpath, resolve) => {
  const keys = desiredColumns.map((item) => ({
    header: item,
    key: item,
  }));

  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Data");
  worksheet.columns = keys;

  data.forEach((row) => {
    worksheet.addRow(row);
  });

  workbook.xlsx
    .writeFile(dirpath + "/" + filename)
    .then(() => {
      console.log(`Data telah diekspor ke ${filename}`);
      resolve();
    })
    .catch((err) => {
      console.error("Terjadi kesalahan saat menulis ke file:", err);
    });
};

// Fungsi untuk generate Excel dari SPL
const generateExcelSPL = async (filename, dirpath, yearmonth, sequelizeInstance, databaseName, tableName, columns, batchSize) => {
  try {
    let startOffset = 0;
    let batchNumber = 1;

    while (true) {
      const query = `
        SELECT ${columns.join(', ')} FROM dashboard.${tableName}
        WHERE yearmonth = '${yearmonth}'
        LIMIT ${batchSize} OFFSET ${startOffset};
      `;


      const [rawData] = await sequelizeInstance.query(query);
      if (rawData.length === 0) {
        break;  // Jika tidak ada data lagi, keluar dari loop
      }

      const filenameWithTable = `${tableName}_${batchNumber}_${filename}`;

      const keys = columns.map((item) => ({
        header: item,
        key: item,
      }));

      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet(filenameWithTable);
      worksheet.columns = keys;

      rawData.forEach((row) => {
        worksheet.addRow(row);
      });

      await new Promise((resolve, reject) => {
        workbook.xlsx
          .writeFile(`${dirpath}/${filenameWithTable}.xlsx`)
          .then(() => {
            console.log(`Data telah diekspor ke ${filenameWithTable}`);
            resolve();
          })
          .catch((err) => {
            console.error("Terjadi kesalahan saat menulis ke file:", err);
            reject(err);
          });
      });

      // Increment startOffset and batchNumber for the next batch
      startOffset += rawData.length;
      batchNumber++;
    }
  } catch (error) {
    console.error("Terjadi kesalahan saat mengambil data :", error);
    throw error;
  }
};






const exportToZip = async (filename, dirpath) => {
  return new Promise((resolve, reject) => {
    const archive = archiver("zip", { zlib: { level: 9 } });
    const output = fs.createWriteStream(`${dirpath}.zip`);

    archive.on("error", (err) => reject(err));
    output.on("close", () => resolve());

    archive.directory(dirpath, false);
    archive.finalize();
    archive.pipe(output);
  });
};



const generateExcelSplAndExportToZip = async (filename, dirpath, yearmonth, sequelizeInstance, databaseName, tableName, columns, batchSize) => {
  try {
    // Generate Excel dari SPL menggunakan objek Sequelize yang sesuai
    const filenameWithTable = await generateExcelSPL(filename, dirpath, yearmonth, sequelizeInstance, databaseName, tableName, columns, batchSize);

    console.log(`Data berhasil diekspor ke ${filename}`);

    // Menunggu hingga proses menulis file Excel selesai
    await delay(2000); // Sesuaikan dengan kebutuhan

    // Setelah file Excel dibuat, buat zip
    console.log("Membuat file zip...");
    await exportToZip(filenameWithTable, dirpath);  // Menambahkan nama tabel ke folder zip

    console.log("File zip berhasil dibuat.");
  } catch (error) {
    console.error("Terjadi kesalahan:", error);
    throw error;
  }
};


const getFiles = async (yearmonth, product, res) => {
  let [data] = await sequelize.query(
    `select origina_fie_name  from dashboard.upload u 
        where  exists  (
            select load_id, * from dashboard."policy" p  where  p.yearmonth  = '${yearmonth}'  
            and u.id  = p.load_id 
            and p.product  like '${product}%'
        )`
  );

  let rootPath = "feedback/";
  let dirpath = rootPath + product + "/" + yearmonth;
  await fs.promises.mkdir(dirpath, {
    recursive: true,
  });

  let promise = Promise.resolve();
  data.map(async (item) => {
    promise = promise.then(() => {
      return new Promise(async (resolve) => {
        let filename = item.origina_fie_name;
        let filenameFeedback = [
          filename.slice(0, 17),
          "Feedback_",
          filename.slice(17),
        ].join("");

        let targetFile = rootPath + filenameFeedback;
        let destiFile = dirpath + "/" + filenameFeedback;
        console.log("Move " + filenameFeedback + " To " + destiFile);

        await fsExtra.move(targetFile, destiFile);
        resolve();
      }, 0);
    });
  });
  await promise;
  await delay(2000);
  let newZip = spawn("zip", ["-r", "-P", "zip#123", dirpath + ".zip", dirpath]);

  newZip.on("exit", async function (code) {
    res.json({
      status: true,
      message: "Process zip done!",
    });
  });
};

const getFilesGenerate = async (yearmonth, product, app, res) => {
  console.log(yearmonth, product, app);
  let query = `
    with a as (
      select distinct  load_id from dashboard."policy" p 
      where  p.yearmonth  = '${yearmonth}'  
      and product  like '${product}%'
      union all 
      select distinct  load_id from dashboard."cancel" c
      where  c.yearmonth  = '${yearmonth}'  
      and product  like '${product}%'
    )        
    select  distinct (load_id), u.origina_fie_name  from  a
    inner join dashboard.upload u on u.id  = a.load_id
    order by load_id  asc`;
  let [data] = await sequelize.query(query);

  let rootPath = "feedback/";
  let dirpath = rootPath + product + "/" + yearmonth;
  await fs.promises.mkdir(dirpath, {
    recursive: true,
  });

  let promise = Promise.resolve();
  data.map(async (item) => {
    promise = promise.then(() => {
      return new Promise(async (resolve) => {
        let filename = item.origina_fie_name;
        let id = item.load_id;
        console.log(filename, id);
        filename = [
          filename.slice(0, 17),
          "Feedback_",
          filename.slice(17),
        ].join("");
        await generateExcelSPL(filename, dirpath, yearmonth, sequelizeInstance, databaseName, tableName, columns, batchSize); // Panggil fungsi generateExcelSPL dengan benar
        
        // Menunggu hingga proses generate Excel selesai
        await delay(2000); // Sesuaikan dengan kebutuhan
        
        generateExcel(
          filename,
          await readExcelFile(dirpath + "/" + filename),
          dirpath,
          resolve
        );
      });
    });
  });
  await promise;
  console.log("Waiting for Excel write to finish...");

  // Menunggu hingga proses menulis file Excel selesai
  await delay(2000); // Sesuaikan dengan kebutuhan
  
  console.log("Exporting to zip...");
  let newZip = spawn("zip", ["-r", "-P", "zip#123", dirpath + ".zip", dirpath]);

  newZip.on("exit", async function (code) {
    if (code === 0) {
      res.json({
        status: true,
        message: "Process zip done!",
      });
    } else {
      console.error(`Error creating zip. Exit code: ${code}`);
      res.status(500).json({
        status: false,
        message: "Error creating zip.",
      });
    }
  });
};

const getFilesGenerateMerge = async (yearmonth, product, app, res) => {
  console.log(yearmonth, product, app);
  let query = `
    select policy_number, borrower, contract_number, submit_date, loan_start_date, loan_amount, rate, tenor, premium_amount, link_certificate,
    status
    from dashboard."policy"
    where  yearmonth  = '${yearmonth}'  
    and product like '${product}%'
    and status = 'Approved'`;
  
  try {
    let [rawData] = await sequelize.query(query);
    
    let rootPath = "feedback/";
    let dirpath = rootPath + product + "/" + yearmonth;
    await fs.promises.mkdir(dirpath, { recursive: true });

    if (rawData.length) {
      let dataExcel = rawData;
      let key = Object.keys(dataExcel[0]);
      let keys = [];
      key.map((item) => {
        keys.push({
          header: item,
          key: item,
        });
      });

      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet("Data");
      worksheet.columns = keys;

      dataExcel.forEach((row) => {
        worksheet.addRow(row);
      });

      let productName = "KPI";
      if (product === "AF") productName = "AFI";
      let filename = `${productName}_Registration_${yearmonth}.xlsx`;

      await new Promise((resolve, reject) => {
        workbook.xlsx
          .writeFile(dirpath + "/" + filename)
          .then(() => {
            console.log(`Data telah diekspor ke ${filename}`);
            resolve();
          })
          .catch((err) => {
            console.error("Terjadi kesalahan saat menulis ke file:", err);
            reject(err);
          });
      });

      // Panggil fungsi generateExcelSPL dan export ke zip
      await generateExcelSplAndExportToZip(filename, dirpath, yearmonth, sequelize, "dashboard", "policy", key, 1000);

      res.json({
        status: true,
        message: "Process zip done!",
      });
    } else {
      console.log("Tidak ada data yang memenuhi kriteria.");
      res.json({
        status: false,
        message: "No data found for the given criteria.",
      });
    }
  } catch (error) {
    console.error("Terjadi kesalahan:", error);
    res.status(500).json({
      status: false,
      message: "Terjadi kesalahan saat mengekspor data.",
    });
  }
};


// Endpoint untuk mengunduh file zip
app.get("/download-excel/:database/:yearmonth/:table", async (req, res) => {
  const { database, yearmonth, table } = req.params;
  const { batchSize }  = req.query;
  const rootPath = "feedback/";
  const dirpath = `${rootPath}${batchSize}_${database}_${yearmonth}`;
  res.setHeader("Content-type", "application/zip");
  res.download(__dirname + "/" + dirpath + ".zip");
});


// Panggil fungsi generateExcelSplAndExportToZip
app.get("/generate-zip/:database/:yearmonth/:table", async (req, res) => {
  try {
    // Kirim respons sukses terlebih dahulu
    res.status(200).json({
      status: true,
      message: "Proses dimulai dengan sukses!",
    });

    const { database, yearmonth, table } = req.params;
    const { batchSize }  = req.query;
    const rootPath = "feedback/";
    let databaseName;

    const dirpath = `${rootPath}${batchSize}_${database}_${yearmonth}`;
    let filename = `data_atome_${yearmonth}.xlsx`;

    await fs.promises.mkdir(dirpath, { recursive: true });

    let sequelizeInstance;
    let desiredColumns;

    if (table === "claim") {
      if (database === "sps_api") {
        // Kolom-kolom untuk tabel "claim" pada database "sps_api"
        desiredColumns = [
          'claim_id',
          'no_rekening',
          'no_polis',
          'nama',
          'usia',
          'jenis_kelamin',
          'kategori',
          'jangka',
          'kantor_cabang',
          'tgl_pengajuan',
          'no_surat',
          'tgl_kolektibility_3',
          'penyebab_klaim',
          'nilai_pengajuan',
          'hutang_pokok',
          'tunggakan_bunga',
          'tunggakan_biaya',
          'tunggakan_denda',
          'nominal_disetujui',
          'rekening_koran',
          'bukti_dokumen',
          'data_nasabah',
          'pembayaran_klaim',
          'remark',
          'status',
          'claim_settlement',
          'yearmonth',
          'created_at',
          'updated_at',
          'bukti_pembayaran',
          'batch_claim',
          'hak_klaim_80',
          'hak_hutang_pokok'
        ];
      } 
      else if ( database === "afi" || database === "kpi" ){
        desiredColumns = [
        'claim_id',
        'loan_id',
        'contract_number',
        'normal_outstanding',
        'dpd',
        'funding_partner',
        'product',
        'tenor',
        'premium_amount',
        'status',
        'remark',
        'yearmonth',
        'batch_policy',
        'load_id',
        'created_at',
        'loan_amount'
      ];
      }
       else {
        // Kolom-kolom untuk tabel "claim" pada database selain "sps_api"
        desiredColumns = [
          'no_rekening',
          'no_perjanjian_kredit',
          'nama',
          'no_ktp',
          'tgl_lahir',
          'nilai_pokok_kredit',
          'nilai_klaim',
          'tenor',
          'tgl_mulai',
          'tgl_akhir'
      ];
    }
    } else if (table === "policy") {
      if (database === "sps_api") {
        // Tentukan kolom-kolom untuk tabel "policy" pada database "sps_api"
        desiredColumns = [
          'policy_number',
          'periode',
          'kantor_cabang',
          'no_rekening',
          'no_ktp',
          'cif',
          'nama_debitur',
          'tanggal_lahir',
          'jenis_kelamin',
          'produk',
          'kode_produk',
          'sub_produk',
          'produk_fintech',
          'kategori',
          'nama_perusahaan',
          'mulai_asuransi',
          'selesai_asuransi',
          'jangka_waktu',
          'limit_plafond',
          'nilai_pertanggungan',
          'rate_premi',
          'premi',
          'tgl_pencairan',
          'tgl_pk',
          'no_pk',
          'nama_program',
          'is_cbc',
          'coverage',
          'nomor_polis',
          'url_sertifikat',
          'yearmonth',
          'created_at',
          'risk',
          'status',
          'psjt',
          'sisa_bulan',
          'premi_refund',
          'remark_refund'
        ];
      }
       else if (database === "afi" || database === "kpi" ){
        desiredColumns = [
          'policy_id',
          'batch_id',
          'seq_number',
          'policy_number',
          'funding_partner_loan_id',
          'borrower',
          'contract_number',
          'funding_partner',
          'product',
          'submit_date',
          'loan_start_date',
          'loan_end_date',
          'loan_amount',
          'rate',
          'tenor',
          'premium_amount',
          'link_certificate',
          'status',
          'remark',
          'yearmonth',
          'load_id',
          'main_product'
        ];
     } 
     else {
        // Kolom-kolom untuk tabel "policy" pada database selain "sps_api"
        desiredColumns = [
          'nomor_rekening',
          'nomor_aplikasi_pk',
          'tanggal_perjanjian_kredit',
          'nama',
          'alamat',
          'no_ktp',
          'tanggal_lahir',
          'tanggal_mulai',
          'tanggal_akhir',
          'usia',
          'jml_bulan_kredit',
          'harga_pertanggungan',
          'limit_plafond',
          'kategori_debitur',
          'tempat_kerja',
          'kategori',
          'paket_coverage',
          'premium',
          'url_sertifikat'
        ];
      }
    }

    if (database === "spl") {
      sequelizeInstance = sequelize_spl;
      databaseName = "spl"; // Sesuaikan dengan nama database SPL Anda
      filename = `data_${databaseName}_${yearmonth}.xlsx`; // Nama file yang diperbarui untuk spj

    } else if (database === "spj") {
      // Ganti konfigurasi koneksi untuk database spj
      sequelizeInstance = new Sequelize(
        process.env.DB_DATABASE_SPJ,
        process.env.DB_USERNAME_SPJ,
        process.env.DB_PASSWORD_SPJ,
        {
          host: process.env.DB_HOST_SPJ,
          port: process.env.DB_PORT_SPJ,
          dialect: process.env.DB_CONNECTION_SPJ,
        }
      );
      databaseName = "spj"; // Set nama database
      filename = `data_${databaseName}_${yearmonth}.xlsx`; // Nama file yang diperbarui untuk spj
    } 
    else if (database === "sps_api") {
      // Ganti konfigurasi koneksi untuk database sps_api
      sequelizeInstance = new Sequelize(
        process.env.DB_DATABASE_FLEXI,
        process.env.DB_USERNAME_FLEXI,
        process.env.DB_PASSWORD_FLEXI,
        {
          host: process.env.DB_HOST_FLEXI,
          port: process.env.DB_PORT_FLEXI,
          dialect: process.env.DB_CONNECTION_FLEXI,
        }
      );
      databaseName = "sps_api"; // Set nama database
      filename = `data_${databaseName}_${yearmonth}.xlsx`; // Nama file yang diperbarui untuk sps_api
    }
    else if (database === "kpi") {
      sequelizeInstance = sequelize;
      databaseName = "kpi"; // Set nama database
      filename = `data_${databaseName}_${yearmonth}.xlsx`; // Nama file yang diperbarui untuk kpi
    }
     else {
      sequelizeInstance = sequelize;
      databaseName = "default"; 
    }

    // Panggil fungsi generateExcelSplAndExportToZip dengan yearmonth, columns, dan table dari parameter
    await generateExcelSplAndExportToZip(filename, dirpath, yearmonth, sequelizeInstance, databaseName, table, desiredColumns, batchSize);

    // Set is_process to false after exporting data
    const updateQuery = `
      UPDATE dashboard.files_redines
      SET is_process = false
      WHERE yearmonth = '${yearmonth}';
    `;

    await sequelizeInstance.query(updateQuery);
    
  } catch (error) {
    console.error("Terjadi kesalahan:", error);
    // Kirim respons error
    res.status(500).json({
      status: false,
      message: "Terjadi kesalahan saat membuat file Excel dan zip.",
    });
  }
});




app.get("/policy/:yearmonth/:type", (req, res) => {
  const { yearmonth, type } = req.params;
  getFiles(yearmonth, type, res);
});


app.get("/:app/:yearmonth/:type", (req, res) => {
  const { yearmonth, type, app } = req.params;
  // getFilesGenerate(yearmonth, type, app, res);
  getFilesGenerateMerge(yearmonth, type, app, res);
}); 

app.get("/download/policy/:yearmonth/:type", (req, res) => {
  const { yearmonth, type } = req.params;
  let rootPath = "feedback/";
  let dirpath = rootPath + type + "/" + yearmonth;
  console.log("download " + dirpath);
  res.setHeader("Content-type", "application/zip");
  res.download(
    __dirname + "/" + dirpath + ".zip",
    type + "_" + yearmonth + ".zip"
  );
});

app.get("/download/:app/:yearmonth/:type", (req, res) => {
  const { yearmonth, type } = req.params;
  let rootPath = "feedback/";
  let dirpath = rootPath + type + "/" + yearmonth;
  console.log("download " + dirpath);
  res.setHeader("Content-type", "application/zip");
  res.download(
    __dirname + "/" + dirpath + ".zip",
    type + "_" + yearmonth + ".zip"
  );
});



app.listen(port, () => {
  console.log(`Example app listening on port http://localhost:${port}`);
});

