package main

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	gr "github.com/dancannon/gorethink"
)

func BenchmarkRandomWrites(b *testing.B) {

	rand.Seed(time.Now().UTC().UnixNano())

	var err error

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  100,
		MaxOpen:  100,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	for i := 0; i < b.N; i++ {
		data := map[string]interface{}{
			"customer_id": strconv.FormatInt(rand.Int63(), 10),
		}
		// Insert the new item into the database
		_, err = gr.Table("benchmarks").Insert(data).RunWrite(session)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
		}
	}

}

func BenchmarkRandomWritesParallel(b *testing.B) {

	rand.Seed(time.Now().UTC().UnixNano())

	var err error

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  100,
		MaxOpen:  100,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data := map[string]interface{}{
				"customer_id": strconv.FormatInt(rand.Int63(), 10),
			}
			// Insert the new item into the database
			_, err = gr.Table("benchmarks").Insert(data).RunWrite(session)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
			}
		}
	})

}

func BenchmarkSoftRandomWrites(b *testing.B) {

	rand.Seed(time.Now().UTC().UnixNano())

	var err error

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  100,
		MaxOpen:  100,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	for i := 0; i < b.N; i++ {
		data := map[string]interface{}{
			"customer_id": strconv.FormatInt(rand.Int63(), 10),
		}
		// Insert the new item into the database
		opts := gr.InsertOpts{Durability: "soft"}
		_, err = gr.Table("benchmarks").Insert(data, opts).RunWrite(session)
		if err != nil {
			b.Errorf("insert failed [%s] ", err)
		}
	}

}

func BenchmarkSoftRandomWritesParallel(b *testing.B) {

	rand.Seed(time.Now().UTC().UnixNano())

	var err error

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  100,
		MaxOpen:  100,
	})

	if err != nil {
		log.Fatalln(err.Error())
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data := map[string]interface{}{
				"customer_id": strconv.FormatInt(rand.Int63(), 10),
			}

			// Insert the new item into the database
			opts := gr.InsertOpts{Durability: "soft"}
			_, err = gr.Table("benchmarks").Insert(data, opts).RunWrite(session)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
			}
		}
	})

}

func BenchmarkGoSequentialWrites(b *testing.B) {
	var err error
	c := make(chan struct{}, 100)

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  100,
		MaxOpen:  100,
	})

	if err != nil {
		log.Fatalln(err.Error())

	}

	si := 0
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
		si++
		data := map[string]interface{}{
			"customer_id": si,
		}

		go func() {
			// Insert the new item into the database
			_, err = gr.Table("benchmarks").Insert(data).RunWrite(session)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
				return

			}
			<-c
		}()

	}
}

func BenchmarkGoSoftSequentialWrites(b *testing.B) {
	var err error
	c := make(chan struct{}, 100)

	session, err := gr.Connect(gr.ConnectOpts{
		Address:  "localhost:28015",
		Database: "test",
		MaxIdle:  100,
		MaxOpen:  100,
	})

	if err != nil {
		log.Fatalln(err.Error())

	}

	si := 0
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
		si++
		data := map[string]interface{}{
			"customer_id": si,
		}

		go func() {
			// Insert the new item into the database
			opts := gr.InsertOpts{Durability: "soft"}
			_, err = gr.Table("benchmarks").Insert(data, opts).RunWrite(session)
			if err != nil {
				b.Errorf("insert failed [%s] ", err)
				return

			}
			<-c
		}()

	}
}
