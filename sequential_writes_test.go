package main

import (
	"log"
	"sync"
	"testing"

	gr "github.com/dancannon/gorethink"
)

func BenchmarkSequentialWrites(b *testing.B) {

	var err error

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  1000,
		MaxOpen:  1000,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	si := 0
	for i := 0; i < b.N; i++ {
		si++
		data := map[string]interface{}{
			"customer_id": si,
		}

		// Insert the new item into the database
		_, err = gr.Table("benchmarks").Insert(data).RunWrite(session)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
			return
		}
	}
}

func BenchmarkSequentialWritesParallel(b *testing.B) {

	var err error
	var mu sync.Mutex
	si := 0

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  1000,
		MaxOpen:  1000,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			si++
			mu.Unlock()

			data := map[string]interface{}{
				"customer_id": si,
			}

			// Insert the new item into the database
			_, err = gr.Table("benchmarks").Insert(data).RunWrite(session)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
				return
			}
		}
	})

}
