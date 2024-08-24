package queuelite_test

import (
	"context"
	"os"
	"testing"

	"github.com/JorgeLNJunior/queuelite"
	"golang.org/x/sync/errgroup"
)

const dbDir string = "queuelite.db"

func TestEnqueue(t *testing.T) {
	t.Run("should enqueue a Job", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Enqueue(context.Background(), job)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("should support concurrent enqueues", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		var eg errgroup.Group
		eg.SetLimit(25)

		for range 500 {
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

func TestDequeue(t *testing.T) {
	t.Run("should dequeue a job", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Enqueue(context.Background(), job)
		if err != nil {
			tt.Error(err)
		}

		if _, err := queue.Dequeue(context.Background()); err != nil {
			tt.Error(err)
		}
	})
}

func TestIsEmpty(t *testing.T) {
	t.Run("should return true if the queue is empty", func(tt *testing.T) {
		_ = os.Remove(dbDir)

		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		isEmpty, err := queue.IsEmpty()
		if err != nil {
			t.Error(err)
		}
		if !isEmpty {
			t.Error("expected 'true' but received 'false'")
		}
	})

	t.Run("should return false if the queue is not empty", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Enqueue(context.Background(), job)
		if err != nil {
			tt.Error(err)
		}

		isEmpty, err := queue.IsEmpty()
		if err != nil {
			t.Error(err)
		}
		if isEmpty {
			t.Error("expected 'false' but received 'true'")
		}
	})
}

func BenchmarkEnqueue(b *testing.B) {
	queue, err := queuelite.NewSQLiteQueue(dbDir)
	if err != nil {
		b.Error(err)
	}
	defer queue.Close()

	for range b.N {
		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Enqueue(context.Background(), job)
		if err != nil {
			b.Error(err)
		}
	}
}
