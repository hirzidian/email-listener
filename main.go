package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/alexmullins/zip"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/joho/godotenv"
	"github.com/kurin/blazer/b2"
	"github.com/tealeg/xlsx"
	"io"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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

	r.GET("/:type/:yearmonth/:app", handleGetFilesGenerateMerge)

	r.Run(fmt.Sprintf(":%d", port))
}

func handleDownloadExcelZip(c *gin.Context) {
    database := c.Param("database")
    yearmonth := c.Param("yearmonth")
    batchSize := c.Query("batchSize")

    // Inisialisasi koneksi ke Backblaze B2
    ctx := context.Background()
    b2c, err := b2.NewClient(ctx, "YOUR_ACCOUNT_ID", "YOUR_APPLICATION_KEY")
    if err != nil {
        handleError("Error saat membuat klien Backblaze B2:", err, c, nil, "")
        return
    }

    // Buat objek klien Bucket
    bucket, err := b2c.NewBucket(ctx, "files-management", nil)
    if err != nil {
        handleError("Error saat membuat bucket Backblaze B2:", err, c, nil, "")
        return
    }

    // Nama file zip yang akan diunduh
    zipFilename := fmt.Sprintf("%s_%s_%s.zip", batchSize, database, yearmonth)

    // Path tempat menyimpan file zip yang diunduh
    localPath := filepath.Join("/path/to/save", zipFilename)

    // Dapatkan URL unduhan dengan menggunakan fungsi GetDownloadAuthorization
    downloadAuth, err := bucket.Download(ctx, zipFilename)
    if err != nil {
        handleError("Error saat mendapatkan otorisasi unduhan dari Backblaze B2:", err, c, nil, "")
        return
    }

    downloadURL := downloadAuth.URL

    // Lakukan unduhan menggunakan HTTP client biasa
    resp, err := http.Get(downloadURL)
    if err != nil {
        handleError("Error saat melakukan unduhan:", err, c, nil, "")
        return
    }
    defer resp.Body.Close()

    // Buka file untuk menyimpan hasil unduhan
    localFile, err := os.Create(localPath)
    if err != nil {
        handleError("Error saat membuka file lokal:", err, c, nil, "")
        return
    }
    defer localFile.Close()

    // Salin isi unduhan ke file lokal
    _, err = io.Copy(localFile, resp.Body)
    if err != nil {
        handleError("Error saat menyalin isi file:", err, c, nil, "")
        return
    }

    fmt.Printf("File ZIP %s berhasil diunduh ke %s\n", zipFilename, localPath)

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
			handleError("Gagal menginisialisasi koneksi database.", nil, c, nil, "")
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
					handleError("Error saat mengambil data dari database:", nil, c, nil, "")
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
					handleError("Error saat menambahkan lembar ke file Excel:", err, c, nil, "")
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
					Name: filenameWithTable + ".xlsx",
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

				fmt.Printf("Data telah diekspor ke %s\n", filenameWithTable)

				// Tambahkan startOffset untuk batch berikutnya
				startOffset += batchSizeInt
			}(batchNumber)

			batchNumber++
		}

		// Tutup zip archive di memori ketika semua batch selesai diproses di dalam goroutine
		wg.Wait()
		if err := zipWriter.Close(); err != nil {
			handleError("Error saat menutup zip archive:", err, c, nil, "")
			resultCh <- err
			return
		}

		// Unggah zip archive di memori ke Backblaze B2 di dalam goroutine
		ctx := context.Background()
		b2c, err := b2.NewClient(ctx, "005e5a1b4a267b30000000001", "K005Vlb42qTganKSM2D9cvCqYZqas9A")
		if err != nil {
			handleError("Error saat membuat klien Backblaze B2:", err, c, nil, "")
			resultCh <- err
			return
		}

		bucket, err := b2c.NewBucket(ctx, "files-management", nil)
		if err != nil {
			handleError("Error saat membuat bucket Backblaze B2:", err, c, nil, "")
			resultCh <- err
			return
		}

		obj := bucket.Object("files-redines/" + dirpath + ".zip")
		w := obj.NewWriter(ctx)
		_, err = io.Copy(w, zipBuffer)
		if err != nil {
			handleError("Error saat mengunggah file ZIP ke Backblaze B2:", err, c, nil, "")
			resultCh <- err
			return
		}

		if err := w.Close(); err != nil {
			handleError("Error saat menutup penulis Backblaze B2:", err, c, nil, "")
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

		fmt.Println("File ZIP berhasil dibuat dan diunggah di dalam goroutine.")

		// Hapus folder lokal setelah berhasil diunggah di dalam goroutine
		if err := os.RemoveAll(dirpath); err != nil {
			fmt.Println("Error saat menghapus folder lokal di dalam goroutine:", err)
			// Tangani kesalahan jika diperlukan, misalnya, log
		}

		// Tandai bahwa proses di latar belakang telah selesai di dalam goroutine
		resultCh <- nil
	}()

 

	// Return success response to the frontend
	c.JSON(http.StatusOK, gin.H{
		"status":  true,
		"message": "Proses pembuatan file Excel dan ZIP telah dimulai di latar belakang.",
	})
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

func createZip(ctx context.Context, b2c *b2.Client, bucket *b2.Bucket, sourceDir, destinationPath string) error {
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

    // Unggah arsip zip di dalam memori ke Backblaze B2
    obj := bucket.Object(filepath.Join(destinationPath, "example.zip"))
    w := obj.NewWriter(ctx)
    if err := archive.Flush(); err != nil {
        return err
    }

    if err := w.Close(); err != nil {
        return err
    }

    return nil
}

func handleGetFilesGenerateMerge(c *gin.Context) {
	yearmonth := c.Param("yearmonth")
	app := c.Param("app")
	product := c.Param("app")[:2]
	table := c.Param("type")

	// Initialize the Sequelize instance based on the app parameter
	var sequelizeInstance *gorm.DB
	switch app {
	case "AFI":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
	case "KPI":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_ATOME")
	case "SPL":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_SPL")
	case "SPJ":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_SPJ")
	case "FLE":
		sequelizeInstance = initializeDatabase("DB_CONNECTION_FLEXI")
	default:
		sequelizeInstance = db
	}

	if sequelizeInstance == nil {
		fmt.Println("Failed to initialize database connection.")
		c.String(http.StatusInternalServerError, "Internal Server Error")
		return
	}
	defer func() {
		if err := sequelizeInstance.Close(); err != nil {
			fmt.Println("Error closing database connection:", err)
		}
	}()

	var desiredColumns []string
	switch app {
	case "AFI":
		desiredColumns = getColumnsForAtome(table)
	case "KPI":
		desiredColumns = getColumnsForAtome(table)
	case "SPL":
		desiredColumns = getColumnsForSPL(table)
	case "SPJ":
		desiredColumns = getColumnsForSPJ(table)
	case "FLE":
		desiredColumns = getColumnsForSPSAPI(table)
	default:
		// Fallback desired columns if the app is not recognized
		desiredColumns = getColumnsForSPL(table)
	}

	columns := strings.Join(desiredColumns, ", ")
	var query string

	if app == "SPL" || app == "SPJ" || app == "FLE" {
		query = fmt.Sprintf(`
            SELECT %s
            FROM dashboard.%s
            WHERE yearmonth = '%s'
        `, columns, table, yearmonth)
	} else {
		query = fmt.Sprintf(`
            SELECT %s
            FROM dashboard.%s
            WHERE yearmonth = '%s'
            AND product LIKE '%s%%'
            AND status = 'Approved'
        `, columns, table, yearmonth, product)
	}

	var rawData []map[string]interface{}
	if err := sequelizeInstance.Raw(query).Find(&rawData).Error; err != nil {
		fmt.Println("Error querying database:", err)
		c.String(http.StatusInternalServerError, "Internal Server Error")
		return
	}

	dirpath := fmt.Sprintf("%s/%s", app, yearmonth)

	if err := os.MkdirAll(dirpath, os.ModePerm); err != nil {
		fmt.Println("Error creating directory:", err)
		c.String(http.StatusInternalServerError, "Internal Server Error")
		return
	}

	if len(rawData) > 0 {
		batchSize := 1000000
		startOffset := 0
		batchNumber := 1

		var shouldExit bool

		for !shouldExit {
			filename := fmt.Sprintf("%s_%d_Registration_%s.xlsx", app, batchNumber, yearmonth)
			filenameWithTable := fmt.Sprintf("%s/%s", dirpath, filename)

			keys := reflect.ValueOf(rawData[0]).MapKeys()
			file := xlsx.NewFile()
			sheet, err := file.AddSheet("Data")
			if err != nil {
				fmt.Println("Error adding sheet to Excel file:", err)
				c.String(http.StatusInternalServerError, "Internal Server Error")
				return
			}

			// Add header
			headerRow := sheet.AddRow()
			for _, col := range keys {
				cell := headerRow.AddCell()
				cell.Value = col.String()
			}

			// Add data rows
			for i := startOffset; i < startOffset+batchSize && i < len(rawData); i++ {
				dataRow := sheet.AddRow()
				for _, col := range keys {
					cell := dataRow.AddCell()
					// Convert value to string before adding to Excel cell
					cell.Value = fmt.Sprintf("%v", rawData[i][col.String()])
				}
			}

			if err := file.Save(filenameWithTable); err != nil {
				fmt.Println("Error saving Excel file:", err)
				c.String(http.StatusInternalServerError, "Internal Server Error")
				return
			}

			fmt.Printf("Data telah diekspor ke %s\n", filename)

			// Increment startOffset and batchNumber for the next batch
			startOffset += batchSize
			batchNumber++

			if startOffset >= len(rawData) {
				shouldExit = true
			}
		}

		// Inisialisasi variabel ctx, b2c, dan bucket
		ctx := context.Background()
		b2c, err := b2.NewClient(ctx, "005e5a1b4a267b30000000001", "K005Vlb42qTganKSM2D9cvCqYZqas9A")
		if err != nil {
			fmt.Println("Error creating Backblaze B2 client:", err)
			c.String(http.StatusInternalServerError, "Internal Server Error")
			return
		}

		bucket, err := b2c.NewBucket(ctx, "files-management", nil)
		if err != nil {
			fmt.Println("Error creating Backblaze B2 bucket:", err)
			c.String(http.StatusInternalServerError, "Internal Server Error")
			return
		}

		destinationPath := "files-redines/"

		if err := createZip(ctx, b2c, bucket, dirpath, destinationPath); err != nil {
			fmt.Println("Error creating ZIP file:", err)
			c.String(http.StatusInternalServerError, "Internal Server Error")
			return
		}

		fmt.Println("File ZIP berhasil dibuat.")

		fmt.Println("Tidak ada data yang memenuhi kriteria.")
		c.JSON(http.StatusOK, gin.H{
			"status":  false,
			"message": "No data found for the given criteria.",
		})
	}
}
