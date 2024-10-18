<div align="center">

# QueueLite

QueueLite is a simple and persistent queue that uses SQLite to store and retrieve data. It is written with concurrency and high throughput in mind, following an [article](https://kerkour.com/sqlite-for-servers) by Sylvain Kerkour to manage the `SQLITE_BUSY` error.

</div>

## Getting started

### Install

In order to start using the library, you should first install it by running the following command:

```
go get github.com/JorgeLNJunior/queuelite
```

### Enqueue

To create a new queue you should call the method `NewSQLiteQueue` passing the directory where the database is located. Queuelite will create a new database file if the directory does not exist. You must not open new connections to this database; queuelite will fail to start if you do.

To add a new job in the queue just create a new job by calling the method `NewJob` and passing it alongside a `context.Context` object.

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

Queuelite does not implement a job processor, it simply exports some basic queue methods. You must handle job processing yourself by calling `Dequeue`, `Complete`, `Retry` and `Fail` methods. The library will handle the status updates, as well as the queue and dequeue processes.

### Dequeue

```go
job, err := queue.Dequeue(context.Background())
if err != nil {
	return err
}
```

### Complete

```go
if err := queue.Complete(context.Background(), job.ID); err != nil {
	return err
}
```

### Retry

```go
if err := queue.Retry(context.Background(), job.ID); err != nil {
	return err
}
```

### Fail

```go
if err := queue.Fail(context.Background(), job.ID, "reason"); err != nil {
	return err
}
```
