package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	gr "github.com/dancannon/gorethink"
)

var bSess *gr.Session
var dbName string
var tableName string

func testSetup() {

	var err error

	dbName = "benchmark"
	tableName = "benchmarks"

	bSess, err = gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: dbName,
		MaxIdle:  50,
		MaxOpen:  50,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	gr.DbDrop(dbName).Exec(bSess)
	gr.DbCreate(dbName).Exec(bSess)

	gr.Db(dbName).TableDrop(tableName).Run(bSess)
	gr.Db(dbName).TableCreate(tableName).Run(bSess)

}

func testTeardown() {
	gr.Db(dbName).TableDrop(tableName).Run(bSess)
	bSess.Close()
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UTC().UnixNano())
	testSetup()
	res := m.Run()
	testTeardown()
	os.Exit(res)
}

func BenchmarkBatch200RandomWrites(b *testing.B) {

	var term gr.Term
	var data []map[string]interface{}

	for i := 0; i < b.N; i++ {

		for is := 0; is < 200; is++ {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			cid := map[string]interface{}{
				"customer_id": strconv.FormatInt(r.Int63(), 10),
			}
			data = append(data, cid)
		}

		// Insert the new item into the database
		term = gr.Table(tableName).Insert(data)

		// Insert the new item into the database
		_, err := term.RunWrite(bSess, gr.RunOpts{
			MinBatchRows: 200,
			MaxBatchRows: 200,
		})
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
		}
	}

}

func BenchmarkBatch200RandomWritesParallel10(b *testing.B) {

	var term gr.Term
	var data []map[string]interface{}

	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {

		for pb.Next() {
			for is := 0; is < 200; is++ {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				cid := map[string]interface{}{
					"customer_id": strconv.FormatInt(r.Int63(), 10),
				}
				data = append(data, cid)
			}

			// Insert the new item into the database
			term = gr.Table(tableName).Insert(data)

			// Insert the new item into the database
			_, err := term.RunWrite(bSess, gr.RunOpts{
				MinBatchRows: 200,
				MaxBatchRows: 200,
			})
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
			}
		}
	})

}

func BenchmarkBatch200SoftRandomWritesParallel10(b *testing.B) {

	var term gr.Term
	var data []map[string]interface{}

	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {

		for pb.Next() {

			opts := gr.InsertOpts{Durability: "soft"}

			for is := 0; is < 200; is++ {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				cid := map[string]interface{}{
					"customer_id": strconv.FormatInt(r.Int63(), 10),
				}
				data = append(data, cid)
			}

			// Insert the new item into the database
			term = gr.Table(tableName).Insert(data, opts)

			// Insert the new item into the database
			_, err := term.RunWrite(bSess, gr.RunOpts{
				MinBatchRows: 200,
				MaxBatchRows: 200,
			})
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
			}
		}
	})

}

func BenchmarkRandomWrites(b *testing.B) {

	for i := 0; i < b.N; i++ {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		data := map[string]interface{}{
			"customer_id": strconv.FormatInt(r.Int63(), 10),
		}
		// Insert the new item into the database
		_, err := gr.Table(tableName).Insert(data).RunWrite(bSess)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
		}
	}

}

func BenchmarkRandomWritesParallel10(b *testing.B) {

	// p*GOMAXPROCS
	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			data := map[string]interface{}{
				"customer_id": strconv.FormatInt(r.Int63(), 10),
			}
			// Insert the new item into the database
			_, err := gr.Table(tableName).Insert(data).RunWrite(bSess)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
			}
		}
	})

}

func BenchmarkRandomSoftWrites(b *testing.B) {

	for i := 0; i < b.N; i++ {
		data := map[string]interface{}{
			"customer_id": strconv.FormatInt(rand.Int63(), 10),
		}
		// Insert the new item into the database
		opts := gr.InsertOpts{Durability: "soft"}
		_, err := gr.Table(tableName).Insert(data, opts).RunWrite(bSess)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
		}
	}

}

func BenchmarkRandomSoftWritesParallel10(b *testing.B) {

	// p*GOMAXPROCS
	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			data := map[string]interface{}{
				"customer_id": strconv.FormatInt(r.Int63(), 10),
			}

			// Insert the new item into the database
			opts := gr.InsertOpts{Durability: "soft"}
			_, err := gr.Table(tableName).Insert(data, opts).RunWrite(bSess)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
			}
		}
	})

}

func BenchmarkSequentialWrites(b *testing.B) {

	si := 0
	for i := 0; i < b.N; i++ {
		si++
		data := map[string]interface{}{
			"customer_id": si,
		}

		// Insert the new item into the database
		_, err := gr.Table(tableName).Insert(data).RunWrite(bSess)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
			return
		}
	}
}

func BenchmarkSequentialWritesParallel10(b *testing.B) {

	var mu sync.Mutex
	si := 0

	// p*GOMAXPROCS
	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			si++
			mu.Unlock()

			data := map[string]interface{}{
				"customer_id": si,
			}

			// Insert the new item into the database
			_, err := gr.Table(tableName).Insert(data).RunWrite(bSess)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
				return
			}
		}
	})

}

func BenchmarkSequentialSoftWrites(b *testing.B) {

	opts := gr.InsertOpts{Durability: "soft"}
	si := 0

	for i := 0; i < b.N; i++ {
		si++
		data := map[string]interface{}{
			"customer_id": si,
		}

		// Insert the new item into the database
		_, err := gr.Table(tableName).Insert(data, opts).RunWrite(bSess)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
			return
		}
	}
}

func BenchmarkSequentialSoftWritesParallel10(b *testing.B) {

	var mu sync.Mutex
	si := 0

	// p*GOMAXPROCS
	b.SetParallelism(10)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			si++
			mu.Unlock()

			data := map[string]interface{}{
				"customer_id": si,
			}

			opts := gr.InsertOpts{Durability: "soft"}

			// Insert the new item into the database
			_, err := gr.Table(tableName).Insert(data, opts).RunWrite(bSess)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
				return
			}
		}
	})

}
