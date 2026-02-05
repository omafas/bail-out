# chunked_reader.go - http/1.1 "chunked"-encoding reader
Very simple http/1.1 "chunked" encoding sink.

## Examples
```
$ go run chunked_reader.go 9000 asdasd
```

or (if already built):

```
$ ./chunked_reader 9000 asdasd
```

Which is to say that the arguments are:

```
$ ./chunked_reader $PORT $ADMIN_PASSWORD
```

## Notes:
Used for the sole purpose of validating that we are properly closing the
chunked-encoded stream(s), _even when there are crashes in ns\_server_.

### Return codes:
The return value will be 0 if the program ended in a way we want/expect while
any non-zero value indicates that there was an error. The different error
numbers are given to different errors. The known error codes are:

* 0: success
* 1: error (generic)
* 2: unable to parse url from interpolated port / password
* 3: stream is not encoded as "chunked"
* 4: no port given or could not parse from string
* 5: no password given, should be 2nd argument to binary

### Usage
This should be used in conjunction with a cluster-test that will listen on the
streaming endpoint, remove the node from the cluster, and verify that the
chunked stream is closed correctly. The original case had reset it's cbas_dirs
when leaving the node, but this caused a crash inside the poolStreaming code,
causing us to close the stream abruptly without ending it properly.

This uses golang because a specific parsing error was produced by one of the
services that's written in go. The test primarily ensures we don't see the
error message "`invalid byte in chunk length`". Instead we should
see `EOF`/`ErrUnexpectedEOF` which indicates that it was closed correctly.
