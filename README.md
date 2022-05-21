# querypool
Utilize concurrent programming technique with worker pool

✨**Here is the idea**✨
> How to store a massive data in a short time?
> So, I tried to play around with golang's goroutine with failover mechanism for insert failure handler, so when an insert operation fails, it will be automatically recovered and retry. So ideally at the end, all data, amounting to a million, will be successfully inserted.

I dont use db connection pool, As per Go official documentation for the sql/database package, the connection pool is managed by Go, we engineers just call *sql.DB's .Conn() method to retrieve the pool items, this pool item can be reused old connections or connections newly created.

How to run this example
```sh
go run main.go config.conf data_batch.csv
```

note:
i will try to create this code as golang package in the future.