<div align="center">

# QueueLite

QueueLite is a simple and persistent queue backed by SQLite. Wrote following the [guide](https://kerkour.com/sqlite-for-servers) made by Silvain Kerkour.

</div>

## Getting started

### Install

```
go get github.com/JorgeLNJunior/queuelite
```

### Enqueue

```go
import "github.com/JorgeLNJunior/queuelite"

queue, err := queuelite.NewSQLiteQueue("queue.db")
if err != nil {
	return err
}
defer queue.Close()

job := queuelite.NewJob([]byte("{ \"key\": \"value\" }"))

if err = queue.Enqueue(context.Background(), job); err != nil {
	return err
}
```

### Dequeue

```go
job, err := queue.Dequeue(context.Background())
if err != nil {
	return err
}
```

### Complete

```go
if err := queue.Complete(context.Background(), job); err != nil {
  return err
}
```

### Retry

```go
if err := queue.Retry(context.Background(), job); err != nil {
  return err
}
```

### Fail

```go
if err := queue.Fail(context.Background(), job, "reason"); err != nil {
  return err
}
```
