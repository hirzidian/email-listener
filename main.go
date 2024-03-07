package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/alexmullins/zip"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/joho/godotenv"
	"github.com/tealeg/xlsx"
)

var db *gorm.DB

func main() {
	// Memuat variabel lingkungan dari file .env
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}

	r := gin.Default()
	port := 3002

	db, err := gorm.Open("postgres", "host=127.0.0.1 port=5435 user=ciu_user dbname=spl sslmode=disable password=ciu_db_2023!")
	if err != nil {
		panic("Failed to connect to the database: " + err.Error())
	}
	defer db.Close()

	r.GET("/generate-zip/:database/:yearmonth/:table/:status", handleGenerateExcelZip)

	r.GET("/download-excel/:database/:yearmonth/:table/:status", handleDownloadExcelZip)

	r.Run(fmt.Sprintf(":%d", port))
}



func handleDownloadExcelZip(c *gin.Context) {
	database := c.Param("database")
	yearmonth := c.Param("yearmonth")
	table := c.Param("table")
	status := c.Param("status")
	batchSize := c.Query("batchSize")

	if database == "flexi" {
		database = "fle"
	}

	capsLockDB := strings.ToUpper(database)

	dirPath := batchSize + "_" + database + "_" + yearmonth

	var fileName string
	if table == "summary_production" {
		fileName = capsLockDB + "_policy_Registration_" + yearmonth + ".zip"
	} else if table == "summary_claim" {
		fileName = capsLockDB + "_claim_Registration_" + yearmonth + ".zip"
	} else if table == "summary_explore" {
		fileName = table + "_" + database + "_" + yearmonth + ".zip"
	} else {
		fileName = status + "_" + table + "_" + database + "_" + yearmonth + ".zip"
	}

	filePath := "feedback/" + capsLockDB + "/" + dirPath + "/" + fileName

	c.Writer.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))
	c.Writer.Header().Add("Content-Type", "application/zip")
	c.File(filePath)

}

func handleGenerateExcelZip(c *gin.Context) {
	database := c.Param("database")
	yearmonth := c.Param("yearmonth")
	table := c.Param("table")
	status := c.Param("status")
	batchSize := c.Query("batchSize")
	capsLockDB := strings.ToUpper(c.Param("database"))

	dirpath := fmt.Sprintf("feedback/%s/%s_%s_%s", capsLockDB, batchSize, database, yearmonth)

	var filename string
	var tbsummary string
	if table == "summary_production" || table == "summary_claim" {
		if table == "summary_production" {
			tbsummary = "policy"
		} else {
			tbsummary = "claim"
		}
		filename = fmt.Sprintf("%s_%s_Registration_%s", capsLockDB, tbsummary, yearmonth)
	} else if table == "summary_explore"{
		filename = fmt.Sprintf("%s_%s_%s", table, database, yearmonth)
	} else {
		filename = fmt.Sprintf("%s_%s_%s_%s", status, table, database, yearmonth)
	}

	// Create a channel to receive the result of the background process
	resultCh := make(chan error)

	var sequelizeInstance *gorm.DB

	switch database {
	case "afi":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
	case "kpi":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
	case "spl":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_SPL")
	case "spj":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_SPJ")
	case "fle":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_FLEXI")
	default:
		// Fallback ke koneksi default jika database tidak dikenali
		sequelizeInstance = db
	}

	var desiredColumns []string
	desiredColumns = getDesiredColumns(database, table)

	// Periksa apakah koneksi ke database telah berhasil
	if sequelizeInstance == nil {
		fmt.Println("Failed to initialize database connection.")
		c.String(http.StatusInternalServerError, "Internal Server Error")
		return
	}

	batchSizeInt, err := strconv.Atoi(batchSize)
	if err != nil {
		fmt.Println("Error converting batchSize to integer:", err)
		c.String(http.StatusInternalServerError, "Internal Server Error")
		return
	}

	// Run tasks concurrently in the background
	go func() {
		// Inisialisasi variabel koneksi database di dalam goroutine
		var sequelizeInstance *gorm.DB

		switch database {
		case "afi":
			sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
		case "kpi":
			sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
		case "spl":
			sequelizeInstance = initializeDatabase("DB_CONNECTION_SPL")
		case "spj":
			sequelizeInstance = initializeDatabase("DB_CONNECTION_SPJ")
		case "fle":
			sequelizeInstance = initializeDatabase("DB_CONNECTION_FLEXI")
		default:
			// Fallback ke koneksi default jika database tidak dikenali
			sequelizeInstance = db
		}

		// Periksa apakah koneksi ke database telah berhasil di dalam goroutine
		if sequelizeInstance == nil {
			handleError("Gagal menginisialisasi koneksi database.", nil, c, nil, "")
			resultCh <- errors.New("Gagal menginisialisasi koneksi database.")
			return
		}

		startOffset := 0
		batchNumber := 1
		var shouldExit bool
		fmt.Printf("Sedang Mengekspor Data %s Ke EXCEL...\n", filename)

		// Buat file ZIP dalam memori di dalam goroutine
		zipBuffer := new(bytes.Buffer)
		zipWriter := zip.NewWriter(zipBuffer)

		var mutex sync.Mutex

		for !shouldExit {
			// Pastikan hanya satu batch diproses pada satu waktu
			mutex.Lock()

			go func(batchNumber int, localStatus string) {
				defer func() {

					// Lepaskan kunci untuk memungkinkan batch berikutnya diproses
					mutex.Unlock()
				}()

				columns := strings.Join(desiredColumns, ", ")
				var query string

				allStatus := fmt.Sprintf(`
					SELECT %s FROM dashboard.%s
					WHERE yearmonth = '%s'
					LIMIT %d OFFSET %d;
				`, columns, table, yearmonth, batchSizeInt, startOffset)

				var product string

				if capsLockDB == "AFI" {
					product = "AF"
				} else if capsLockDB == "KPI" {
					product = "KP"
				}
				// query jika database adalah spl atau spj
				if database == "spl" || database == "spj" {
					if localStatus == "All_Status" {
						query = allStatus
					} else if table == "summary_explore"{
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE policy_yearmonth = '%s'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, batchSizeInt, startOffset)
					} else {
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND status_%s = '%s'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, table, status, batchSizeInt, startOffset)
					}
					// query jika database adalah flexi
				} else if database == "fle" {
					if localStatus == "All_Status" {
						query = allStatus
					} else if localStatus == "Not_Refunded" {
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND status = 'Not Refunded'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, batchSizeInt, startOffset)
					} else if localStatus == "In_Process" {
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND status = 'In Process'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, batchSizeInt, startOffset)
					} else {
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND status = '%s'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, status, batchSizeInt, startOffset)
					}
					// query jika database adalah kpi dan afi
				} else {
					if localStatus == "All_Status" {
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND product LIKE '%s%%'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, product, batchSizeInt, startOffset)
					} else if table == "summary_production" ||  table == "summary_claim" {
						var statusAtome string
						if table == "summary_production" {
							statusAtome = "Approved"
						} else {
							statusAtome = "Proceed"
						}
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND product LIKE '%s%%' AND status  = '%s'
							LIMIT %d OFFSET %d;
						`, columns, tbsummary, yearmonth, product, statusAtome, batchSizeInt, startOffset)
					} else {
						query = fmt.Sprintf(`
							SELECT %s FROM dashboard.%s
							WHERE yearmonth = '%s' AND product LIKE '%s%%' AND status  = '%s'
							LIMIT %d OFFSET %d;
						`, columns, table, yearmonth, product, status, batchSizeInt, startOffset)
					}

				}
				// Inisialisasi variabel rows di dalam goroutine
				rows, err := sequelizeInstance.Raw(query).Rows()

				if err != nil {
					handleError("Error saat mengambil data dari database:", nil, c, nil, "")
					return
				}
				defer rows.Close() // Tutup rows pada akhir loop

				// Keluar dari loop jika tidak ada data baru
				if !rows.Next() {
					shouldExit = true
					return
				}

				fileNameNew := fmt.Sprintf("%d_%s", batchNumber, filename)

				keys := desiredColumns

				var valuesFirstRow []interface{}
				for range keys {
					var value interface{}
					valuesFirstRow = append(valuesFirstRow, &value)
				}

				if err := rows.Scan(valuesFirstRow...); err != nil {
					handleError("Error saat memindai baris pertama:", err, c, nil, "")
					return
				}

				file := xlsx.NewFile()
				sheet, err := file.AddSheet("Data")
				if err != nil {
					handleError("Error saat menambahkan lembar ke file Excel:", err, c, nil, "")
					return
				}

				// Tambahkan header
				headerRow := sheet.AddRow()
				for _, col := range keys {
					cell := headerRow.AddCell()
					cell.Value = col
				}
				// Tambahkan data dari baris pertama ke Excel di luar goroutine
				dataRowFirst := sheet.AddRow()
				for _, value := range valuesFirstRow {
					cell := dataRowFirst.AddCell()
					cell.Value = fmt.Sprintf("%v", *value.(*interface{}))
				}

				rowIndex := 1
				for rows.Next() {
					var values []interface{}
					for range keys {
						var value interface{}
						values = append(values, &value)
					}

					if err := rows.Scan(values...); err != nil {
						handleError("Error saat memindai baris:", err, c, nil, "")
						return
					}

					dataRow := sheet.AddRow()
					for _, value := range values {
						cell := dataRow.AddCell()
						cell.Value = fmt.Sprintf("%v", *value.(*interface{}))
					}

					rowIndex++
				}

				// Simpan file Excel ke dalam zip archive di memori di dalam goroutine
				excelBuffer := new(bytes.Buffer)
				if err := file.Write(excelBuffer); err != nil {
					handleError("Error saat menyimpan file Excel ke buffer:", err, c, nil, "")
					return
				}

				// Tambahkan file Excel ke dalam zip archive di memori di dalam goroutine
				fileHeader := &zip.FileHeader{
					Name: fileNameNew + ".xlsx",
				}
				writer, err := zipWriter.CreateHeader(fileHeader)
				if err != nil {
					handleError("Error saat membuat header zip:", err, c, nil, "")
					return
				}
				_, err = writer.Write(excelBuffer.Bytes())
				if err != nil {
					handleError("Error saat menulis file Excel ke dalam zip archive:", err, c, nil, "")
					return
				}

				fmt.Printf("Data telah diekspor ke %s\n", fileNameNew)

				// Tambahkan startOffset untuk batch berikutnya
				startOffset += batchSizeInt
			}(batchNumber, status)

			batchNumber++
		}
		fmt.Printf("Sedang Mengekspor %s Ke ZIP...\n", filename)

		if _, err := os.Stat(dirpath); os.IsNotExist(err) {
			err := os.MkdirAll(dirpath, 0755)
			if err != nil {
				handleError("Error creating directory:", err, c, nil, "")
				resultCh <- err
				return
			}
		}
		// Selesaikan penulisan ZIP
		if err := zipWriter.Close(); err != nil {
			handleError("Error closing ZIP writer:", err, c, nil, dirpath)
			resultCh <- err
			return
		}
		// Save the ZIP file to the local file system
		zipFilePath := fmt.Sprintf("%s/%s.zip", dirpath, filename)
		if err := os.WriteFile(zipFilePath, zipBuffer.Bytes(), 0644); err != nil {
			handleError("Error saving ZIP file to local file system:", err, c, nil, "")
			resultCh <- err
			return
		}

		updateQuery := fmt.Sprintf(`
			UPDATE dashboard.files_redines
			SET is_process = false
			WHERE yearmonth = '%s';
		`, yearmonth)

		// Eksekusi query dan tangani kesalahan jika ada
		if err := sequelizeInstance.Exec(updateQuery).Error; err != nil {
			handleError("Error saat mengeksekusi query update:", err, c, nil, "")
			resultCh <- err
			return
		}

		fmt.Printf("File ZIP %s Berhasil Dibuat\n", filename)

		// Tandai bahwa proses di latar belakang telah selesai di dalam goroutine
		resultCh <- nil
		// Return success response to the frontend
		c.JSON(http.StatusOK, gin.H{
			"status":  true,
			"message": "Proses pembuatan file Excel dan ZIP telah dimulai di latar belakang.",
		})

	}()

}

func handleError(message string, err error, c *gin.Context, zipBuffer *bytes.Buffer, dirpath string) {
	fmt.Println(message, err)
	c.String(http.StatusInternalServerError, "Internal Server Error")
	_ = dirpath
	_ = zipBuffer
}

func initializeDatabase(envKey string) *gorm.DB {
	dbConnection := os.Getenv(envKey)
	dbHost := os.Getenv("DB_HOST_" + strings.ToUpper(strings.Split(envKey, "_")[2]))
	dbPort := os.Getenv("DB_PORT_" + strings.ToUpper(strings.Split(envKey, "_")[2]))
	dbDatabase := os.Getenv("DB_DATABASE_" + strings.ToUpper(strings.Split(envKey, "_")[2]))
	dbUsername := os.Getenv("DB_USERNAME_" + strings.ToUpper(strings.Split(envKey, "_")[2]))
	dbPassword := os.Getenv("DB_PASSWORD_" + strings.ToUpper(strings.Split(envKey, "_")[2]))

	connectionString := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s", dbHost, dbPort, dbUsername, dbDatabase, dbPassword)
	sequelizeInstance, _ := gorm.Open(dbConnection, connectionString)

	return sequelizeInstance
}

func getDesiredColumns(database, table string) []string {
	var desiredColumns []string

	switch database {
	case "afi":
		desiredColumns = getColumnsForAtome(table)
	case "kpi":
		desiredColumns = getColumnsForAtome(table)
	case "spl":
		desiredColumns = getColumnsForSPL(table)
	case "spj":
		desiredColumns = getColumnsForSPJ(table)
	case "fle":
		desiredColumns = getColumnsForSPSAPI(table)
	default:
		// Fallback ke koneksi default jika database tidak dikenali
	}

	return desiredColumns
}

func getColumnsForAtome(table string) []string {
	var columns []string

	if table == "claim" {
		columns = []string{
			"claim_id",
			"loan_id",
			"contract_number",
			"normal_outstanding",
			"dpd",
			"funding_partner",
			"product",
			"tenor",
			"premium_amount",
			"status",
			"remark",
			"yearmonth",
			"batch_policy",
			"load_id",
			"created_at",
			"loan_amount",
		}
	} else if table == "policy" {
		columns = []string{
			"policy_id",
			"batch_id",
			"seq_number",
			"policy_number",
			"funding_partner_loan_id",
			"borrower",
			"contract_number",
			"funding_partner",
			"product",
			"submit_date",
			"loan_start_date",
			"loan_end_date",
			"loan_amount",
			"rate",
			"tenor",
			"premium_amount",
			"link_certificate",
			"status",
			"remark",
			"yearmonth",
			"load_id",
			"main_product",
		}
	} else if table == "summary_production" {
		columns = []string{
			"policy_number",
			"borrower",
			"contract_number",
			"submit_date",
			"loan_start_date",
			"loan_amount",
			"rate",
			"tenor",
			"premium_amount",
			"link_certificate",
			"status",
		}
	} else if table == "summary_claim" {
		columns = []string{
			"loan_id",
			"loan_amount",
			"tenor",
			"premium_amount",
			"status",

		}
	}

	return columns
}

func getColumnsForSPL(table string) []string {
	var columns []string

	if table == "claim" {
		columns = []string{
			"claim_id",
			"policy_number",
			"reference_id",
			"product_id",
			"packed_code",
			"premium",
			"phone_no",
			"email",
			"application_number",
			"benefit",
			"product_key",
			"package_name",
			"policy_start_date",
			"status_claim",
			"no_rekening",
			"no_perjanjian_kredit",
			"nama",
			"tgl_lahir",
			"no_ktp",
			"nilai_kredit_dasar",
			"nilai_klaim",
			"nilai_pokok_kredit",
			"tgl_mulai",
			"tgl_akhir",
			"tenor",
			"tanggal_pengajuan_klaim_bni",
			"upload_id",
			"error",
			"created_at",
			"updated_at",
			"total",
			"filename",
			"yearmonth",
			"remark",
			"batch_policy",
			"category",
		}
	} else if table == "policy" {
		columns = []string{
			"nomor_rekening",
			"nomor_aplikasi_pk",
			"tanggal_perjanjian_kredit",
			"nama",
			"alamat",
			"no_ktp",
			"tanggal_lahir",
			"tanggal_mulai",
			"tanggal_akhir",
			"usia",
			"jml_bulan_kredit",
			"harga_pertanggungan",
			"limit_plafond",
			"kategori_debitur",
			"tempat_kerja",
			"kategori",
			"paket_coverage",
			"premium",
			"url_sertifikat",
		}
	} else if table == "summary_explore"{
		columns = []string{
			"no_pk",
			"nik",
			"name",
			"policy",
			"claim",
			"policy_yearmonth",
			"claim_yearmonth",
			"ltc_by_nik",
		  }
		  
	}

	return columns
}

func getColumnsForSPJ(table string) []string {
	// ... (implementasi serupa untuk SPJ)
	var columns []string
	if table == "claim" {
		columns = []string{
			"claim_id",
			"policy_number",
			"reference_id",
			"product_id",
			"packed_code",
			"premium",
			"phone_no",
			"email",
			"application_number",
			"benefit",
			"product_key",
			"package_name",
			"policy_start_date",
			"status_claim",
			"no_rekening",
			"no_perjanjian_kredit",
			"nama",
			"no_ktp",
			"nilai_kredit_dasar",
			"tgl_lahir",
			"nilai_pokok_kredit",
			"nilai_klaim",
			"tenor",
			"tgl_mulai",
			"tgl_akhir",
			"tanggal_pengajuan_klaim_bni",
			"upload_id",
			"error",
			"created_at",
			"updated_at",
			"total",
			"filename",
			"yearmonth",
			"remark",
			"batch_policy",
			"category",
		}
	} else if table == "policy" {
		columns = []string{
			"nomor_rekening",
			"nomor_aplikasi_pk",
			"tanggal_perjanjian_kredit",
			"nama",
			"alamat",
			"no_ktp",
			"tanggal_lahir",
			"tanggal_mulai",
			"tanggal_akhir",
			"usia",
			"jml_bulan_kredit",
			"harga_pertanggungan",
			"limit_plafond",
			"kategori_debitur",
			"tempat_kerja",
			"kategori",
			"paket_coverage",
			"premium",
			"url_sertifikat",
		}
	} else if table == "summary_explore"{
		columns = []string{
			"no_pk",
			"nik",
			"name",
			"policy",
			"claim",
			"policy_yearmonth",
			"claim_yearmonth",
			"ltc_by_nik",
		}
	}
	return columns
}

func getColumnsForSPSAPI(table string) []string {
	// ... (implementasi serupa untuk fle)
	var columns []string
	if table == "claim" {
		columns = []string{
			"claim_id",
			"no_rekening",
			"no_polis",
			"nama",
			"usia",
			"jenis_kelamin",
			"kategori",
			"jangka",
			"kantor_cabang",
			"tgl_pengajuan",
			"no_surat",
			"tgl_kolektibility_3",
			"penyebab_klaim",
			"nilai_pengajuan",
			"hutang_pokok",
			"tunggakan_bunga",
			"tunggakan_biaya",
			"tunggakan_denda",
			"nominal_disetujui",
			"rekening_koran",
			"bukti_dokumen",
			"data_nasabah",
			"pembayaran_klaim",
			"remark",
			"status",
			"claim_settlement",
			"yearmonth",
			"created_at",
			"updated_at",
			"bukti_pembayaran",
			"batch_claim",
			"hak_klaim_80",
			"hak_hutang_pokok",
		}
	} else if table == "policy" {
		columns = []string{
			"policy_number",
			"periode",
			"kantor_cabang",
			"no_rekening",
			"no_ktp",
			"cif",
			"nama_debitur",
			"tanggal_lahir",
			"jenis_kelamin",
			"produk",
			"kode_produk",
			"sub_produk",
			"produk_fintech",
			"kategori",
			"nama_perusahaan",
			"mulai_asuransi",
			"selesai_asuransi",
			"jangka_waktu",
			"limit_plafond",
			"nilai_pertanggungan",
			"rate_premi",
			"premi",
			"tgl_pencairan",
			"tgl_pk",
			"no_pk",
			"nama_program",
			"is_cbc",
			"coverage",
			"nomor_polis",
			"url_sertifikat",
			"yearmonth",
			"created_at",
			"risk",
			"status",
			"psjt",
			"sisa_bulan",
			"premi_refund",
			"remark_refund",
		}
	}

	return columns
}

func createZip(sourceDir, destinationPath string) error {
	archive := zip.NewWriter(io.Writer(nil)) // Buat arsip zip di dalam memori

	if err := filepath.Walk(sourceDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Method = zip.Deflate

		// Sesuaikan nama header untuk mencakup jalur tujuan yang diinginkan dalam bucket
		header.Name, err = filepath.Rel(sourceDir, filePath)
		if err != nil {
			return err
		}
		header.Name = filepath.Join(destinationPath, header.Name)

		if info.IsDir() {
			header.Name += "/"
		}

		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}

		if !info.IsDir() {
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(writer, file)
		}

		return err
	}); err != nil {
		return err
	}

	return nil
}
