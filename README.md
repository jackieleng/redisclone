Run using:

```bash
go run .
```

Test using netcat:

```bash
# ping
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 3333

# set
printf  '*3\r\n$3\r\nSET\r\n$3\r\nFOO\r\n$3\r\nBAR\r\n' | nc localhost 3333

# get
printf  '*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n' | nc localhost 3333
```

Or test using `redis-cli`:

```bash
redis-cli -p 3333
```
