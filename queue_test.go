package queuelite_test

import (
	"context"
	"errors"
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

		if err = queue.Enqueue(context.Background(), job); err != nil {
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

		isEmpty, err := queue.IsEmpty(context.Background())
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

		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}

		isEmpty, err := queue.IsEmpty(context.Background())
		if err != nil {
			t.Error(err)
		}
		if isEmpty {
			t.Error("expected 'false' but received 'true'")
		}
	})
}

func TestRetry(t *testing.T) {
	t.Run("should re-add a job in the queue with [JobStatusRetry] status", func(tt *testing.T) {
		os.Remove(dbDir)

		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}

		if err = queue.Retry(context.Background(), job); err != nil {
			tt.Error(err)
		}
	})

	t.Run("should increase the retry count of the job", func(tt *testing.T) {
		os.Remove(dbDir)

		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}

		if err = queue.Retry(context.Background(), job); err != nil {
			tt.Error(err)
		}

		result, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}
		if result.ID != job.ID {
			tt.Errorf("expected job '%s' but received '%s'", job.ID, result.ID)
		}
		if result.RetryCount != 1 {
			tt.Errorf("expect retry count to be 1 but received %d", result.RetryCount)
		}
	})

	t.Run("should return [JobNotFoundErr] if a job is not in the queue", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Retry(context.Background(), job)
		if err == nil {
			tt.Error("expected an error but got nil")
		}
		if !errors.Is(err, queuelite.JobNotFoundErr) {
			tt.Errorf("expected an [JobNotFoundErr] but got '%s'", err.Error())
		}
	})
}

func TestCount(t *testing.T) {
	t.Run("should return how many jobs are in the queue", func(tt *testing.T) {
		os.Remove(dbDir)

		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))
		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}
		_, err = queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		job = queuelite.NewJob([]byte("{ \"key\": \"value\" }"))
		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}
		if err = queue.Retry(context.Background(), job); err != nil {
			tt.Error(err)
		}

		job = queuelite.NewJob([]byte("{ \"key\": \"value\" }"))
		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}

		count, err := queue.Count(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if count.Total != 3 {
			tt.Errorf("expected total to be 3 but got %d", count.Total)
		}
		if count.Pending != 1 {
			tt.Errorf("expected pending to be 1 but got %d", count.Pending)
		}
		if count.Retry != 1 {
			tt.Errorf("expected retry to be 1 but got %d", count.Retry)
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
