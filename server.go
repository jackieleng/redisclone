package main

import (
	"bytes"
	"errors"
	"fmt"
	"redisclone/resp"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"log"
)

const (
    CONN_HOST = "localhost"
    CONN_PORT = 3333
    CONN_TYPE = "tcp"
    RESP_SEPARATOR = "\r\n"
)


type Store struct {
	lock sync.RWMutex
	data map[string]string
}

func (store *Store) Set(k string, v string) {
	store.lock.RLock()
	store.data[k] = v
	store.lock.RUnlock()
}

func (store *Store) Get(k string) (string, bool) {
	store.lock.Lock()
	defer store.lock.Unlock()
	s, exists := store.data[k]
	return s, exists
}

var STORE = Store{data: make(map[string]string)}

func main() {
	port := CONN_PORT
	var err error
	if len(os.Args) > 1 {
		port, err = strconv.Atoi(os.Args[1])
		if err != nil {
			log.Println("invalid port:", os.Args[1])
			os.Exit(1)
		}
	}
	l, err := net.Listen(CONN_TYPE, fmt.Sprintf("%v:%d", CONN_HOST, port))
	if err != nil {
		log.Println("error listening:", err)
		os.Exit(1)
	}
	defer l.Close()

	log.Printf("listening on %v:%d\n", CONN_HOST, port)

	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
		    log.Println("error accepting:", err)
		    os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	log.Printf("serving %s\n", conn.RemoteAddr().String())

	// close connection when done
	defer func() {
		log.Println("closing connection")
		conn.Close()
	}()

	// make sure server doesn't crash when a panic occurs
	defer func() {
		if r := recover(); r != nil {
		    log.Println("Recovered. Error:\n", r)
		}
	}()

	for {
		buf := bytes.Buffer{}
		tmpLen := 256
		tmp := make([]byte, tmpLen)

		log.Println("reading message...")
		for {
			nbytes, err := conn.Read(tmp)
			if err != nil {
				log.Println("error reading:", err)
				return
			}
			log.Printf("read %d bytes\n", nbytes)
			buf.Write(tmp[:nbytes])
			if nbytes < tmpLen {
				break
			}
		}
		message := buf.String()
		log.Printf("handling message: %#v", message)
		arr, err := parseRespArray(message)
		if err != nil {
			log.Println("error parsing RESP array:", err)
			return
		}
		log.Printf("parsed bulk string array: %#v", arr)
		err = handleCommand(conn, arr)
		if err != nil {
			log.Println("error handling command:", err)
			continue
		}



	}
}

func parseRespArray(data string) (arr []string, err error) {
	// data e.g.: *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
	if len(data) == 0 || data[0] != '*' {
		return arr, errors.New("invalid array")
	}
	sepIdx := strings.Index(data, "\r\n")
	arrLen, err := strconv.Atoi(data[1:sepIdx])
	if err != nil {
		return arr, err
	}
	arr2 := make([]string, arrLen)
	arrIdx := 0
	remainder := data[sepIdx + 2:]
	var s string
	for len(remainder) > 0 && arrIdx < arrLen {
		s, remainder = parseRespBulkStrings(remainder)
		if s == "" {
			log.Printf("no bulk string parse (remainder: %#v)", remainder)
			break
		}
		log.Printf("parsed a bulk string: %#v", s)
		arr2[arrIdx] = s
		arrIdx += 1
	}
	if len(remainder) > 0 || arrIdx != arrLen {
		return arr, errors.New("unparsed items in array")
	}
	return arr2, nil
}

func parseRespBulkStrings(data string) (string, string) {
	// format: $<length>\r\n<data>\r\n
	// e.g. $5\r\nhello\r\n
	if len(data) == 0 || data[0] != '$' {
		return "", data
	}
	idxFirst := strings.Index(data, RESP_SEPARATOR)
	if idxFirst == -1 {
		return "", data
	}
	strLen, err := strconv.Atoi(data[1:idxFirst])
	if err != nil {
		log.Println(err)
		return "", data
	}
	idxLast := idxFirst + 2 + strLen
	if data[idxLast:idxLast + 2] != RESP_SEPARATOR {
		log.Println("incorrect bulk string length")
		return "", data
	}

	s := data[idxFirst+len(RESP_SEPARATOR):idxLast]

	return s, data[idxLast+2:]
}

func handleCommand(conn net.Conn, arr []string) error {
	if len(arr) == 0 {
		return errors.New("empty command arrary")
	}
	command := strings.ToUpper(string(arr[0]))
	log.Println("parsed the following command:", command)

	switch command {
	case "PING":
		handlePing(conn, arr)
	case "SET":
		err := handleSet(conn, arr)
		if err != nil {
			return err
		}
	case "GET":
		err := handleGet(conn, arr)
		if err != nil {
			return err
		}
	default:
		simpErr := resp.SimpleError{Data: fmt.Sprintf("unknown command: %s", command)}
		conn.Write([]byte(simpErr.Serialize()))
		log.Printf("unknown command: %v\n", command)
	}
	return nil
}

func handlePing(conn net.Conn, arr []string) {
	if len(arr) > 1 {
		reply := resp.BulkString{Data: arr[1]}
		conn.Write([]byte(reply.Serialize()))
		return
	}
	pong := resp.SimpleString{Data: "PONG"}
	response := pong.Serialize()
	log.Printf("sending response: %#v\n", response)
	conn.Write([]byte(response))
}

func handleSet(conn net.Conn, arr []string) error {
	if len(arr) < 3 {
		return errors.New("not enough arguments")
	}
	key := arr[1]
	val := arr[2]
	log.Printf("setting key=%s, value=%s\n", key, val)
	STORE.Set(key, val)
	log.Printf("%s was set\n", key)
	ok := resp.SimpleString{Data: "OK"}
	conn.Write([]byte(ok.Serialize()))
	return nil
}

func handleGet(conn net.Conn, arr []string) error {
	if len(arr) < 2 {
		simpErr := resp.SimpleError{Data: "not enough arguments"}
		conn.Write([]byte(simpErr.Serialize()))
		return nil
	}
	key := arr[1]
	log.Printf("getting key=%s\n", key)
	val, exists := STORE.Get(key)
	if !exists {
		log.Printf("key not found: %s", key)
		conn.Write([]byte(resp.NULL_BULK_STRING))
		return nil
	}
	log.Println("got value: ", val)
	bs := resp.BulkString{Data: val}
	conn.Write([]byte(bs.Serialize()))
	return nil
}
