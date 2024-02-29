package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/alexmullins/zip"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/joho/godotenv"
	"github.com/tealeg/xlsx"
	"io"
	"net/http"
	"os"
	"path/filepath"
	// "reflect"
	"strconv"
	"strings"
	"sync"
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

	r.GET("/generate-zip/:database/:yearmonth/:table", handleGenerateExcelZip)

	r.GET("/download-excel/:database/:yearmonth/:table", handleDownloadExcelZip)

	// r.GET("/:type/:yearmonth/:app", handleGetFilesGenerateMerge)

	r.Run(fmt.Sprintf(":%d", port))
}

func handleDownloadExcelZip(c *gin.Context) {
	database := c.Param("database")
	yearmonth := c.Param("yearmonth")
	batchSize := c.Query("batchSize")

	// Nama file zip yang akan diunduh
	zipFilename := fmt.Sprintf("%s_%s_%s.zip", batchSize, database, yearmonth)

	// Path tempat menyimpan file zip yang diunduh
	localPath := filepath.Join("/path/to/save", zipFilename)

	// Buka file untuk menyimpan hasil unduhan
	localFile, err := os.Open(localPath)
	if err != nil {
		handleError("Error saat membuka file lokal:", err, c)
		return
	}
	defer localFile.Close()

	// Berikan file yang diunduh kepada pengguna sebagai respons
	c.File(localPath)
}

func handleGenerateExcelZip(c *gin.Context) {
	database := c.Param("database")
	yearmonth := c.Param("yearmonth")
	table := c.Param("table")
	batchSize := c.Query("batchSize")
	dirpath := fmt.Sprintf("%s_%s_%s", batchSize, database, yearmonth)
	filename := fmt.Sprintf("data_%s_%s", database, yearmonth)
	zipFilename := fmt.Sprintf("%s_%s_%s.zip", batchSize, database, yearmonth)

	// Membuat folder baru jika belum ada
	err := createDirectoryIfNotExist("./feedback/" + dirpath)
	if err != nil {
		handleError("Error creating directory:", err, c)
		return
	}

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
	case "sps_api":
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

	// Create a channel to receive the result of the background process
	resultCh := make(chan error)

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
		case "sps_api":
			sequelizeInstance = initializeDatabase("DB_CONNECTION_FLEXI")
		default:
			// Fallback ke koneksi default jika database tidak dikenali
			sequelizeInstance = db
		}

		// Periksa apakah koneksi ke database telah berhasil di dalam goroutine
		if sequelizeInstance == nil {
			handleError("Gagal menginisialisasi koneksi database.", nil, c)
			resultCh <- errors.New("Gagal menginisialisasi koneksi database.")
			return
		}

		startOffset := 0
		batchNumber := 1
		var shouldExit bool

		// Buat file ZIP dalam memori di dalam goroutine
		zipBuffer := new(bytes.Buffer)
		zipWriter := zip.NewWriter(zipBuffer)

		var wg sync.WaitGroup
		var mutex sync.Mutex

		for !shouldExit {
			// Pastikan hanya satu batch diproses pada satu waktu
			mutex.Lock()

			// Tambahkan counter wait group
			wg.Add(1)

			go func(batchNumber int) {
				defer func() {
					// Kurangi counter wait group saat batch selesai diproses
					wg.Done()

					// Lepaskan kunci untuk memungkinkan batch berikutnya diproses
					mutex.Unlock()
				}()

				columns := strings.Join(desiredColumns, ", ")
				query := fmt.Sprintf(`
					SELECT %s FROM dashboard.%s
					WHERE yearmonth = '%s'
					LIMIT %d OFFSET %d;
				`, columns, table, yearmonth, batchSizeInt, startOffset)

				// Inisialisasi variabel rows di dalam goroutine
				rows, err := sequelizeInstance.Raw(query).Rows()
				if err != nil {
					handleError("Error saat mengambil data dari database:", nil, c)
					return
				}
				defer rows.Close() // Tutup rows pada akhir loop

				// Keluar dari loop jika tidak ada data baru
				if !rows.Next() {
					shouldExit = true
					return
				}

				filenameWithTable := fmt.Sprintf("%s_%d_%s", table, batchNumber, filename)

				keys := desiredColumns
				file := xlsx.NewFile()
				sheet, err := file.AddSheet("Data")
				if err != nil {
					handleError("Error saat menambahkan lembar ke file Excel:", err, c)
					return
				}

				// Tambahkan header
				headerRow := sheet.AddRow()
				for _, col := range keys {
					cell := headerRow.AddCell()
					cell.Value = col
				}

				rowIndex := 2
				for rows.Next() {
					var values []interface{}
					for range keys {
						var value interface{}
						values = append(values, &value)
					}

					if err := rows.Scan(values...); err != nil {
						handleError("Error saat memindai baris:", err, c)
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
					handleError("Error saat menyimpan file Excel ke buffer:", err, c)
					return
				}

				// Tambahkan file Excel ke dalam zip archive di memori di dalam goroutine
				fileHeader := &zip.FileHeader{
					Name: filenameWithTable + ".xlsx",
				}
				writer, err := zipWriter.CreateHeader(fileHeader)
				if err != nil {
					handleError("Error saat membuat header zip:", err, c)
					return
				}
				_, err = writer.Write(excelBuffer.Bytes())
				if err != nil {
					handleError("Error saat menulis file Excel ke dalam zip archive:", err, c)
					return
				}

				fmt.Printf("Data telah diekspor ke %s\n", filenameWithTable)

				// Tambahkan startOffset untuk batch berikutnya
				startOffset += batchSizeInt
			}(batchNumber)

			batchNumber++
		}

		// Tutup zip archive di memori ketika semua batch selesai diproses di dalam goroutine
		wg.Wait()
		if err := zipWriter.Close(); err != nil {
			handleError("Error saat menutup zip archive:", err, c)
			resultCh <- err
			return
		}

		// Create ZIP file locally
		dirpath := fmt.Sprintf("feedback/%s_%s_%s", batchSize, database, yearmonth)
		destinationPath := dirpath // Ganti dengan direktori penyimpanan lokal yang diinginkan

		if err := createZip(dirpath, destinationPath); err != nil {
			handleError("Error creating ZIP file locally:", err, c)
			resultCh <- err
			return
		}

		fmt.Printf("File ZIP %s berhasil dibuat di %s\n", zipFilename, destinationPath)

		// Berikan respons bahwa proses selesai
		resultCh <- nil
	}()

	// Return success response to the frontend
	c.JSON(http.StatusOK, gin.H{
		"status":  true,
		"message": "Proses pembuatan file Excel dan ZIP telah dimulai di latar belakang.",
	})
}

func createDirectoryIfNotExist(path string) error {
    // Check if the directory exists
    if _, err := os.Stat(path); os.IsNotExist(err) {
        // Create the directory if it does not exist
        if err := os.MkdirAll(path, os.ModePerm); err != nil {
            return err
        }
    }
    return nil
}


func handleError(message string, err error, c *gin.Context) {
    fmt.Println(message, err)
    c.String(http.StatusInternalServerError, "Internal Server Error")
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
	case "atome":
		desiredColumns = getColumnsForAtome(table)
	case "spl":
		desiredColumns = getColumnsForSPL(table)
	case "spj":
		desiredColumns = getColumnsForSPJ(table)
	case "sps_api":
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
	}

	return columns
}

func getColumnsForSPL(table string) []string {
	var columns []string

	if table == "claim" {
		columns = []string{
			"no_rekening",
			"no_perjanjian_kredit",
			"nama",
			"no_ktp",
			"tgl_lahir",
			"nilai_pokok_kredit",
			"nilai_klaim",
			"tenor",
			"tgl_mulai",
			"tgl_akhir",
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
	}

	return columns
}

func getColumnsForSPJ(table string) []string {
	// ... (implementasi serupa untuk SPJ)
	var columns []string
	if table == "claim" {
		columns = []string{
			"no_rekening",
			"no_perjanjian_kredit",
			"nama",
			"no_ktp",
			"tgl_lahir",
			"nilai_pokok_kredit",
			"nilai_klaim",
			"tenor",
			"tgl_mulai",
			"tgl_akhir",
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
	}
	return columns
}

func getColumnsForSPSAPI(table string) []string {
	// ... (implementasi serupa untuk SPS_API)
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

func createZip(dirpath, destinationPath string) error {
	// Buat file ZIP di lokal
	zipFilename := filepath.Join(destinationPath, ".zip")
	zipFile, err := os.Create(zipFilename)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	archive := zip.NewWriter(zipFile)
	defer archive.Close()

	// Fungsi untuk menambahkan file dan folder ke dalam ZIP
	var addFilesToZip func(folder, path string) error
	addFilesToZip = func(folder, path string) error {
		files, err := os.ReadDir(path)
		if err != nil {
			return err
		}

		for _, file := range files {
			filePath := filepath.Join(path, file.Name())

			if file.IsDir() {
				// Rekursif tambahkan folder
				if err := addFilesToZip(filepath.Join(folder, file.Name()), filePath); err != nil {
					return err
				}
				continue
			}

			fileToAdd, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer fileToAdd.Close()

			// Dapatkan informasi file
			fileInfo, err := fileToAdd.Stat()
			if err != nil {
				return err
			}

			// Buat header untuk file dalam ZIP
			fileHeader, err := zip.FileInfoHeader(fileInfo)
			if err != nil {
				return err
			}
			fileHeader.Name = filepath.Join(folder, file.Name())

			// Tambahkan file ke dalam ZIP
			fileWriter, err := archive.CreateHeader(fileHeader)
			if err != nil {
				return err
			}

			if _, err = io.Copy(fileWriter, fileToAdd); err != nil {
				return err
			}
		}

		return nil
	}

	// Panggil fungsi untuk menambahkan file dari direktori sumber
	if err := addFilesToZip("", dirpath); err != nil {
		return err
	}

	return nil
}


// func handleGetFilesGenerateMerge(c *gin.Context) {
// 	yearmonth := c.Param("yearmonth")
// 	app := c.Param("app")
// 	product := c.Param("app")[:2]
// 	table := c.Param("type")

// 	// Initialize the Sequelize instance based on the app parameter
// 	var sequelizeInstance *gorm.DB
// 	switch app {
// 	case "AFI":
// 		sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
// 	case "KPI":
// 		sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
// 	case "SPL":
// 		sequelizeInstance = initializeDatabase("DB_CONNECTION_SPL")
// 	case "SPJ":
// 		sequelizeInstance = initializeDatabase("DB_CONNECTION_SPJ")
// 	case "FLE":
// 		sequelizeInstance = initializeDatabase("DB_CONNECTION_FLEXI")
// 	default:
// 		sequelizeInstance = db
// 	}

// 	if sequelizeInstance == nil {
// 		fmt.Println("Failed to initialize database connection.")
// 		c.String(http.StatusInternalServerError, "Internal Server Error")
// 		return
// 	}
// 	defer func() {
// 		if err := sequelizeInstance.Close(); err != nil {
// 			fmt.Println("Error closing database connection:", err)
// 		}
// 	}()

// 	var desiredColumns []string
// 	switch app {
// 	case "AFI":
// 		desiredColumns = getColumnsForAtome(table)
// 	case "KPI":
// 		desiredColumns = getColumnsForAtome(table)
// 	case "SPL":
// 		desiredColumns = getColumnsForSPL(table)
// 	case "SPJ":
// 		desiredColumns = getColumnsForSPJ(table)
// 	case "FLE":
// 		desiredColumns = getColumnsForSPSAPI(table)
// 	default:
// 		// Fallback desired columns if the app is not recognized
// 		desiredColumns = getColumnsForSPL(table)
// 	}

// 	columns := strings.Join(desiredColumns, ", ")
// 	var query string

// 	if app == "SPL" || app == "SPJ" || app == "FLE" {
// 		query = fmt.Sprintf(`
//             SELECT %s
//             FROM dashboard.%s
//             WHERE yearmonth = '%s'
//         `, columns, table, yearmonth)
// 	} else {
// 		query = fmt.Sprintf(`
//             SELECT %s
//             FROM dashboard.%s
//             WHERE yearmonth = '%s'
//             AND product LIKE '%s%%'
//             AND status = 'Approved'
//         `, columns, table, yearmonth, product)
// 	}

// 	var rawData []map[string]interface{}
// 	if err := sequelizeInstance.Raw(query).Find(&rawData).Error; err != nil {
// 		fmt.Println("Error querying database:", err)
// 		c.String(http.StatusInternalServerError, "Internal Server Error")
// 		return
// 	}

// 	dirpath := fmt.Sprintf("%s/%s", app, yearmonth)

// 	if err := os.MkdirAll(dirpath, os.ModePerm); err != nil {
// 		fmt.Println("Error creating directory:", err)
// 		c.String(http.StatusInternalServerError, "Internal Server Error")
// 		return
// 	}

// 	if len(rawData) > 0 {
// 		batchSize := 1000000
// 		startOffset := 0
// 		batchNumber := 1

// 		var shouldExit bool

// 		for !shouldExit {
// 			filename := fmt.Sprintf("%s_%d_Registration_%s.xlsx", app, batchNumber, yearmonth)
// 			filenameWithTable := fmt.Sprintf("%s/%s", dirpath, filename)

// 			keys := reflect.ValueOf(rawData[0]).MapKeys()
// 			file := xlsx.NewFile()
// 			sheet, err := file.AddSheet("Data")
// 			if err != nil {
// 				fmt.Println("Error adding sheet to Excel file:", err)
// 				c.String(http.StatusInternalServerError, "Internal Server Error")
// 				return
// 			}

// 			// Add header
// 			headerRow := sheet.AddRow()
// 			for _, col := range keys {
// 				cell := headerRow.AddCell()
// 				cell.Value = col.String()
// 			}

// 			// Add data rows
// 			for i := startOffset; i < startOffset+batchSize && i < len(rawData); i++ {
// 				dataRow := sheet.AddRow()
// 				for _, col := range keys {
// 					cell := dataRow.AddCell()
// 					// Convert value to string before adding to Excel cell
// 					cell.Value = fmt.Sprintf("%v", rawData[i][col.String()])
// 				}
// 			}

// 			if err := file.Save(filenameWithTable); err != nil {
// 				fmt.Println("Error saving Excel file:", err)
// 				c.String(http.StatusInternalServerError, "Internal Server Error")
// 				return
// 			}

// 			fmt.Printf("Data telah diekspor ke %s\n", filename)

// 			// Increment startOffset and batchNumber for the next batch
// 			startOffset += batchSize
// 			batchNumber++

// 			if startOffset >= len(rawData) {
// 				shouldExit = true
// 			}
// 		}

// 		// Inisialisasi variabel ctx, b2c, dan bucket
// 		ctx := context.Background()
// 		b2c, err := b2.NewClient(ctx, "005e5a1b4a267b30000000001", "K005Vlb42qTganKSM2D9cvCqYZqas9A")
// 		if err != nil {
// 			fmt.Println("Error creating Backblaze B2 client:", err)
// 			c.String(http.StatusInternalServerError, "Internal Server Error")
// 			return
// 		}

// 		bucket, err := b2c.NewBucket(ctx, "files-management", nil)
// 		if err != nil {
// 			fmt.Println("Error creating Backblaze B2 bucket:", err)
// 			c.String(http.StatusInternalServerError, "Internal Server Error")
// 			return
// 		}

// 		destinationPath := "files-redines/"

// 		if err := createZip(ctx, b2c, bucket, dirpath, destinationPath); err != nil {
// 			fmt.Println("Error creating ZIP file:", err)
// 			c.String(http.StatusInternalServerError, "Internal Server Error")
// 			return
// 		}

// 		fmt.Println("File ZIP berhasil dibuat.")

// 		fmt.Println("Tidak ada data yang memenuhi kriteria.")
// 		c.JSON(http.StatusOK, gin.H{
// 			"status":  false,
// 			"message": "No data found for the given criteria.",
// 		})
// 	}
// }
