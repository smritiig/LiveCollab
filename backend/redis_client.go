package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type RedisClient struct {
	addr        string
	dialTimeout time.Duration
}

func NewRedisClient(addr string) *RedisClient {
	return &RedisClient{addr: addr, dialTimeout: 2 * time.Second}
}

func (c *RedisClient) Do(args ...string) (any, error) {
	conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := writeRESPCommand(conn, args); err != nil {
		return nil, err
	}
	return readRESP(bufio.NewReader(conn))
}

func writeRESPCommand(w io.Writer, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func readRESP(r *bufio.Reader) (any, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	line, err := readRESPLine(r)
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+':
		return line, nil
	case '-':
		return nil, errors.New(line)
	case ':':
		value, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	case '$':
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		return string(payload[:length]), nil
	case '*':
		count, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		values := make([]any, 0, count)
		for i := 0; i < count; i++ {
			value, err := readRESP(r)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func readRESPLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

func redisString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return fmt.Sprint(typed)
	}
}

func redisInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		if typed == "" {
			return 0, nil
		}
		return strconv.ParseInt(typed, 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert Redis value %T to int64", value)
	}
}
