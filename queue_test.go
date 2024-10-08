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
	t.Run("should re-add a job in the queue with [JobStateRetry] state", func(tt *testing.T) {
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

		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if err = queue.Retry(context.Background(), j.ID); err != nil {
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

		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if err = queue.Retry(context.Background(), j.ID); err != nil {
			tt.Error(err)
		}

		result, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
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

		err = queue.Retry(context.Background(), job.ID)
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
		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}
		if err = queue.Retry(context.Background(), j.ID); err != nil {
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

	t.Run("should return an empty struct if there is no jobs in the queue", func(tt *testing.T) {
		os.Remove(dbDir)

		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		count, err := queue.Count(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if count.Total != 0 {
			tt.Errorf("expected total to be 0 but got %d", count.Total)
		}
	})
}

func TestComplete(t *testing.T) {
	t.Run("should set the state of a task to [JobStateCompleted]", func(tt *testing.T) {
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

		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if err = queue.Complete(context.Background(), j.ID); err != nil {
			tt.Error(err)
		}

		count, err := queue.Count(context.Background())
		if err != nil {
			tt.Error(err)
		}
		if count.Completed != 1 {
			tt.Errorf("expected 'completed' to be 1 but got %d", count.Completed)
		}
	})

	t.Run("should return [JobNotFoundErr] if a job is not in the queue", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Complete(context.Background(), job.ID)
		if err == nil {
			tt.Error("expected an error but got nil")
		}
		if !errors.Is(err, queuelite.JobNotFoundErr) {
			tt.Errorf("expected an [JobNotFoundErr] but got '%s'", err.Error())
		}
	})
}

func TestFail(t *testing.T) {
	t.Run("should set the state of a task to [JobStateFailed]", func(tt *testing.T) {
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

		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if err = queue.Fail(context.Background(), j.ID, "a test"); err != nil {
			tt.Error(err)
		}

		count, err := queue.Count(context.Background())
		if err != nil {
			tt.Error(err)
		}
		if count.Failed != 1 {
			tt.Errorf("expected 'failed' to be 1 but got %d", count.Failed)
		}
	})

	t.Run("should return [JobNotFoundErr] if a job is not in the queue", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		err = queue.Fail(context.Background(), job.ID, "a test")
		if err == nil {
			tt.Error("expected an error but got nil")
		}
		if !errors.Is(err, queuelite.JobNotFoundErr) {
			tt.Errorf("expected an [JobNotFoundErr] but got '%s'", err.Error())
		}
	})
}

func TestListPending(t *testing.T) {
	t.Run("should return a list of pending jobs", func(tt *testing.T) {
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

		pending, err := queue.ListPending(context.Background(), queuelite.WithLimit(5))
		if err != nil {
			tt.Error(err)
		}
		count := len(pending)

		if count < 1 {
			tt.Errorf("expected pending jobs count to be 1 but received %d", count)
		}
	})
}

func TestListRunning(t *testing.T) {
	t.Run("should return a list of running jobs", func(tt *testing.T) {
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
		if _, err = queue.Dequeue(context.Background()); err != nil {
			tt.Error(err)
		}

		running, err := queue.ListRunning(context.Background(), queuelite.WithLimit(5))
		if err != nil {
			tt.Error(err)
		}
		count := len(running)

		if count < 1 {
			tt.Errorf("expected running jobs count to be 1 but received %d", count)
		}
	})
}

func TestListRetry(t *testing.T) {
	t.Run("should return a list of retry jobs", func(tt *testing.T) {
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
		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}
		if err = queue.Retry(context.Background(), j.ID); err != nil {
			tt.Error(err)
		}

		retry, err := queue.ListRetry(context.Background(), queuelite.WithLimit(5))
		if err != nil {
			tt.Error(err)
		}
		count := len(retry)

		if count < 1 {
			tt.Errorf("expected retry jobs count to be 1 but received %d", count)
		}
	})
}

func TestListFailed(t *testing.T) {
	t.Run("should return a list of failed jobs", func(tt *testing.T) {
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
		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}
		if err = queue.Fail(context.Background(), j.ID, "error"); err != nil {
			tt.Error(err)
		}

		failed, err := queue.ListFailed(context.Background(), queuelite.WithLimit(5))
		if err != nil {
			tt.Error(err)
		}
		count := len(failed)

		if count < 1 {
			tt.Errorf("expected failed jobs count to be 1 but received %d", count)
		}
	})
}

func TestGetJob(t *testing.T) {
	t.Run("should return a job", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}
		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		_, err = queue.GetJob(context.Background(), j.ID)
		if err != nil {
			tt.Error(err)
		}
	})

	t.Run("should return JobNotFoundErr if a job is not in the queue", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		_, err = queue.GetJob(context.Background(), job.ID)
		if !errors.Is(err, queuelite.JobNotFoundErr) {
			tt.Error(err)
		}
	})
}

func TestRemoveJob(t *testing.T) {
	t.Run("should remove a job", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		if err = queue.Enqueue(context.Background(), job); err != nil {
			tt.Error(err)
		}
		j, err := queue.Dequeue(context.Background())
		if err != nil {
			tt.Error(err)
		}

		if err = queue.RemoveJob(context.Background(), j.ID); err != nil {
			tt.Error(err)
		}
	})

	t.Run("should return JobNotFoundErr if a job is not in the queue", func(tt *testing.T) {
		queue, err := queuelite.NewSQLiteQueue(dbDir)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()

		job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

		if err = queue.RemoveJob(context.Background(), job.ID); !errors.Is(err, queuelite.JobNotFoundErr) {
			tt.Error(err)
		}
	})
}

func BenchmarkEnqueue(b *testing.B) {
	os.Remove(dbDir)

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

func BenchmarkDequeue(b *testing.B) {
	os.Remove(dbDir)

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

		if _, err = queue.Dequeue(context.Background()); err != nil {
			b.Error(err)
		}
	}
}
