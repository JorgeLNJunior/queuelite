package queuelite_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/JorgeLNJunior/queuelite"
	"golang.org/x/sync/errgroup"
	_ "modernc.org/sqlite"
)

func TestEnqueue(t *testing.T) {
	t.Run("should enqueue a Job", func(tt *testing.T) {
		db, err := sql.Open("sqlite", "queuelite.db")
		if err != nil {
			t.Error(err)
		}
		err = db.Ping()
		if err != nil {
			t.Error(err)
		}

		queue, err := queuelite.NewSQLiteQueue(db)
		if err != nil {
			t.Error(err)
		}

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Enqueue(context.Background(), job)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("should support concurrent enqueues", func(tt *testing.T) {
		db, err := sql.Open("sqlite", "queuelite.db")
		if err != nil {
			t.Error(err)
		}
		err = db.Ping()
		if err != nil {
			t.Error(err)
		}

		queue, err := queuelite.NewSQLiteQueue(db)
		if err != nil {
			t.Error(err)
		}

		var eg errgroup.Group
		eg.SetLimit(25)

		for range 100 {
			eg.Go(func() error {
				job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))
				return queue.Enqueue(context.Background(), job)
			})
		}

		if err := eg.Wait(); err != nil {
			t.Error(err)
		}
	})
}

func BenchmarkEnqueue(b *testing.B) {
	db, err := sql.Open("sqlite", "queuelite.db")
	if err != nil {
		b.Error(err)
	}
	err = db.Ping()
	if err != nil {
		b.Error(err)
	}

	queue, err := queuelite.NewSQLiteQueue(db)
	if err != nil {
		b.Error(err)
	}

	for range b.N {
		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Enqueue(context.Background(), job)
		if err != nil {
			b.Error(err)
		}
	}
}
