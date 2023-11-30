package resp

import (
	"fmt"
)

const (
	RESP_SEPARATOR = "\r\n"
	NULL_BULK_STRING =  "$-1\r\n"
)

type RespType interface {
	Serialize() string
}

type BulkStringArray struct {
	Data []BulkString
}

func (arr *BulkStringArray) Serialize() string {
	s := fmt.Sprintf("*%d%s", len(arr.Data), RESP_SEPARATOR)
	for _, bs := range arr.Data {
		s = s + bs.Serialize()
	}
	return s
}

type BulkString struct {
	Data string
}

func (bs *BulkString) Serialize() string {
	return fmt.Sprintf("$%d%s%s%s", len(bs.Data), RESP_SEPARATOR, bs.Data, RESP_SEPARATOR)
}

type SimpleString struct {
	Data string
}

func (ss *SimpleString) Serialize() string {
	return fmt.Sprintf("+%s%s", ss.Data, RESP_SEPARATOR)
}

type SimpleError struct {
	Data string
}

func (err *SimpleError) Serialize() string {
	return fmt.Sprintf("-%s%s", err.Data, RESP_SEPARATOR)
}
