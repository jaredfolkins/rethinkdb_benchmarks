package main

import (
	"log"
	"testing"

	gr "github.com/dancannon/gorethink"
)

func BenchmarkSequentialWrites(b *testing.B) {

	var err error

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  10,
		MaxOpen:  10,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	si := 0
	for i := 0; i < b.N; i++ {
		si++
		data := map[string]interface{}{
			"id": si,
		}

		// Insert the new item into the database
		_, err = gr.Table("benchmarks").Insert(data).RunWrite(session)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
			return
		}
	}
}
