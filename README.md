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
i use following specs to run the program:
```
proccessor: 2 GHz Quad-Core Intel Core i5
memory: 16 GB 3733 MHz LPDDR4X
hadrdisk: ssd
```
`Not recommended on low iops hdd`

note:
i will try to create this code as golang package in the future.