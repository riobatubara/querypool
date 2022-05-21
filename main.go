package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type AppConf struct {
	DbUser         string `json:"db_user"`
	DbPass         string `json:"db_pass"`
	DbAddress      string `json:"db_address"`
	DbPort         string `json:"db_port"`
	DbName         string `json:"db_name"`
	DbTable        string `json:"db_table"`
	DbMaxIdleConns int    `json:"db_max_idle_conns"`
	DbMaxOpenConns int    `json:"db_max_open_conns"`
	TotalWorker    int    `json:"total_worker"`
	Debug          bool   `json:"debug"`
}

var appConf AppConf = AppConf{}
var dbConnString = ""
var dbMaxIdleConns = 0
var dbMaxConns = 0
var totalWorker = 0
var dbTable = ""
var dataHeaders = make([]string, 0)
var debug = false
var totalData = 0
var csvFile = ""

func openCsvFile() (*csv.Reader, *os.File, error) {
	f, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

// On the first read, rows will be accommodated into the dataHeaders variable.
// Next, the data is sent to the worker via the job channel.
func csvDataToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		totalData += 1
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs) // After the data reading process is complete, the channel is closed. Because sending and receiving data on the channel is synchronous for unbuffered channels.
}

// Dispatches multiple goroutines number of totalWorkers.
// Each goroutine is a worker, whose task will later insert data into the database.
// When the application is run, a total of 1000 workers (based on config) will compete to complete a data insert job.
// 1 job carrying 1 data.
func dispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				execJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func setQuestionsMark(n int) []string {
	s := make([]string, 0)
	for i := 0; i < n; i++ {
		s = append(s, "?")
	}
	return s
}

func execJob(workerIndex, counter int, db *sql.DB, values []interface{}) {
	for {
		var errJob error

		// Here, a failover mechanism is applied where when the insert process fails it will be recovered and retry.
		func(errJob *error) {
			defer func() {
				if err := recover(); err != nil {
					*errJob = fmt.Errorf("%v", err)
				}
			}()

			conn, err := db.Conn(context.Background())

			if err != nil {
				log.Fatal(err.Error())
			}

			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				dbTable,
				strings.Join(dataHeaders, ","),
				strings.Join(setQuestionsMark(len(dataHeaders)), ","),
			)

			_, err = conn.ExecContext(context.Background(), query, values...)
			if err != nil {
				log.Fatal("err::", err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&errJob)
		if errJob == nil {
			break
		}
	}

	if counter%100 == 0 {
		if debug {
			log.Println("worker:", workerIndex, "inserted:", counter, "data")
		}
	}
}

func openDbConn() (*sql.DB, error) {
	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func main() {
	narg := len(os.Args)
	if narg < 3 {
		log.Println("err::missing config file or batch data")
		log.Println("inf::gofile/binary file.conf data.csv")
		return
	}

	cfile, err := os.Open(os.Args[1])
	if err != nil {
		log.Println("err:open config_file")
		return
	}

	byteValue, _ := ioutil.ReadAll(cfile)
	json.Unmarshal(byteValue, &appConf)
	cfile.Close()

	csvFile = os.Args[2]

	dbConnString = appConf.DbUser + ":" + appConf.DbPass + "@tcp(" + appConf.DbAddress + ":" + appConf.DbPort + ")/" + appConf.DbName
	dbMaxIdleConns = appConf.DbMaxIdleConns
	dbMaxConns = appConf.DbMaxOpenConns
	totalWorker = appConf.TotalWorker
	dbTable = appConf.DbTable
	debug = appConf.Debug

	log.Println("Config:")
	log.Println("  db_conn_string:", dbConnString)
	log.Println("  db_max_idle_conns:", dbMaxIdleConns)
	log.Println("  db_max_conns:", dbMaxConns)
	log.Println("  total_worker:", totalWorker)
	log.Println("  db_table:", dbTable)
	log.Println("  data_batch:", csvFile)
	log.Println("  debug", debug)
	log.Print("\n")

	start := time.Now()
	db, err := openDbConn()
	if err != nil {
		log.Fatal("err::", err.Error())
		return
	}

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		log.Fatal("err::", err.Error())
		return
	}
	defer csvFile.Close()

	jobs := make(chan []interface{})
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	csvDataToWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	log.Println("data_path:", csvFile, "total_data:", totalData, "proccess_time:", int(math.Ceil(duration.Seconds())), "seconds")
}
