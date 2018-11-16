package bdc

import (
	"encoding/binary"
	"net"
	"sync"
	"time"
)

// Priority priority
type Priority int

const (
	//Normal Normal
	Normal Priority = iota
	//Low Low
	Low
	//High High
	High
)

// Session Session
type Session struct {
	closedErr error //if error,session is closed
	conn      net.Conn
	mut       sync.Mutex
	sendCh    [High + 1]chan []byte
	errCh     chan error
}

func (v *Session) handle() {
	go func() {
		//sending loop
	SEND:
		//this loop will break if any send channel close.
		for {
			select {
			case data, ok := <-v.sendCh[High]:
				if !ok {
					break SEND
				}
				v.write(data)
			default:
				select {
				case data, ok := <-v.sendCh[Normal]:
					if !ok {
						break SEND
					}
					v.write(data)
				default:
					select {
					case data, ok := <-v.sendCh[Low]:
						if !ok {
							break SEND
						}
						v.write(data)
					default:
					}
				}
			}
		}
	}()
	go func() {
	RECEIVE:
		for {
			if _, err := v.read(); nil == err {
			} else {
				v.errCh <- err
				break RECEIVE
			}
		}
	}()
}

func (v *Session) read() ([]byte, error) {
	read := func(conn net.Conn, data []byte) error {
		didReadBytesTotal := 0
		didReadBytes := 0
		var err error
		for {
			if didReadBytesTotal == len(data) {
				break
			}
			err = conn.SetReadDeadline(time.Now().Add(time.Second))
			if err != nil {
				return err
			}
			if didReadBytes, err = conn.Read(data[didReadBytesTotal:]); nil != err {
				break
			}
			didReadBytesTotal += didReadBytes
		}
		return err
	}
	head := make([]byte, 4)
	if err := read(v.conn, head); nil != err {
		return nil, err
	}
	size := binary.BigEndian.Uint32(head)
	if 0 == size {
		return nil, nil
	}
	// if conn.maximumBodySize < bodySize {
	// 	return nil, ErrTooLargePayload
	// }
	data := make([]byte, size)
	if err := read(v.conn, data); nil != err {
		return nil, err
	}
	return data, nil
}

func (v *Session) write(data []byte) error {
	write := func(conn net.Conn, data []byte) error {
		var err error
		didWriteBytes := 0
		for 0 < len(data) {
			didWriteBytes, err = conn.Write(data)
			if nil != err {
				break
			}
			data = data[didWriteBytes:]
		}
		return err
	}
	head := make([]byte, 4)
	if nil != data && 0 <= len(data) {
		binary.BigEndian.PutUint32(head, uint32(len(data)))
		head = append(head, data...)
	} else {
		binary.BigEndian.PutUint32(head, 0)
	}
	return write(v.conn, head)
}
