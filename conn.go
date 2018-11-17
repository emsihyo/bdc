package bdc

import (
	"errors"
	"net"
	"sync"
	"time"
)

var (
	//ErrMethodUnsupported ErrMethodUnsupported
	ErrMethodUnsupported = errors.New("bdc. method unsupported")
	//ErrTooLarge ErrTooLarge
	ErrTooLarge = errors.New("bdc. too large")
	//ErrSizeWrong ErrSizeWrong
	ErrSizeWrong = errors.New("bdc. size wrong")
	//ErrSendChanFull ErrSendChanFull
	ErrSendChanFull = errors.New("bdc. sending chan full")
	//ErrReceiveChanFull ErrReceiveChanFull
	ErrReceiveChanFull = errors.New("bdc. receiving chan full")
	//ErrTimedout ErrTimedout
	ErrTimedout = errors.New("bdc. timedout")
	//ErrClosed ErrClosed
	ErrClosed = errors.New("bdc. conn closed")
	//ErrMarshal ErrMarshal
	ErrMarshal = errors.New("bdc. marshal error")
	//ErrUnmarshal ErrUnmarshal
	ErrUnmarshal = errors.New("bdc. unmarshal error")
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

// Conn Conn
type Conn struct {
	conn         net.Conn
	timeout      time.Duration
	limitSize    int32
	mut          sync.Mutex
	err          error
	connectCh    chan struct{}
	disconnectCh chan error
	receiveCh    chan []byte
	_closeCh     chan error
	_closeChs    []chan error
	_sendChs     [High + 1]chan []byte
}

//NewConn NewConn
func NewConn(conn net.Conn, timeout time.Duration, limitSize int32) *Conn {
	v := &Conn{conn: conn, timeout: timeout, limitSize: limitSize, connectCh: make(chan struct{}, 1), disconnectCh: make(chan error, 1), _closeCh: make(chan error, 1), _closeChs: []chan error{}, receiveCh: make(chan []byte, 64), _sendChs: [High + 1]chan []byte{make(chan []byte, 64), make(chan []byte, 128), make(chan []byte, 256)}}
	v.handle()
	return v
}

func (v *Conn) close(err error) {
	select {
	case v._closeCh <- err:
	default:
	}
}

func (v *Conn) send(data []byte, priority Priority) error {
	select {
	case v._sendChs[priority] <- data:
		return nil
	default:
		v.close(ErrSendChanFull)
		return ErrSendChanFull
	}
}

func (v *Conn) wait() chan error {
	v.mut.Lock()
	defer v.mut.Unlock()
	ch := make(chan error, 1)
	if nil != v.err {
		ch <- v.err
		return ch
	}
	v._closeChs = append(v._closeChs, ch)
	return ch
}

func (v *Conn) handle() {
	v.connectCh <- struct{}{}
	go func() {
	SEND:
		for {
			select {
			case data, ok := <-v._sendChs[High]:
				if !ok {
					break SEND
				}
				if err := v.write(data); nil != err {
					v.close(err)
					break SEND
				}
			default:
				select {
				case data, ok := <-v._sendChs[Normal]:
					if !ok {
						break SEND
					}
					if err := v.write(data); nil != err {
						v.close(err)
						break SEND
					}
				default:
					select {
					case data, ok := <-v._sendChs[Low]:
						if !ok {
							break SEND
						}
						if err := v.write(data); nil != err {
							v.close(err)
							break SEND
						}
					default:
					}
				}
			}
		}
	}()
	go func() {
		var err error
	RECEIVE:
		for {
			select {
			case err = <-v._closeCh:
				break RECEIVE
			default:
				var data []byte
				if data, err = v.read(); nil != err {
					break RECEIVE
				}
				select {
				case v.receiveCh <- data:
				default:
					err = ErrReceiveChanFull
					break RECEIVE
				}
			}
		}
		v.conn.Close()
		for _, send := range v._sendChs {
			close(send)
		}
		v.mut.Lock()
		v.err = err
		for _, close := range v._closeChs {
			close <- err
		}
		v.mut.Unlock()
		v.disconnectCh <- err
	}()
}

func (v *Conn) read() ([]byte, error) {
	read := func(conn net.Conn, data []byte) error {
		didReadBytesTotal := 0
		didReadBytes := 0
		var err error
		for {
			if didReadBytesTotal == len(data) {
				break
			}
			if didReadBytes, err = conn.Read(data[didReadBytesTotal:]); nil != err {
				break
			}
			didReadBytesTotal += didReadBytes
		}
		return err
	}
	b2i := func(b []byte) int32 {
		_ = b[3]
		return int32(b[3]) | int32(b[2])<<8 | int32(b[1])<<16 | int32(b[0])<<24
	}
	if err := v.conn.SetReadDeadline(time.Now().Add(v.timeout)); nil != err {
		return nil, err
	}
	head := make([]byte, 4)
	if err := read(v.conn, head); nil != err {
		return nil, err
	}
	size := b2i(head)
	if 0 == size {
		if err := v.conn.SetReadDeadline(time.Time{}); nil != err {
			return nil, err
		}
		return nil, nil
	}
	if 0 > size {
		return nil, ErrSizeWrong
	}
	if v.limitSize < size {
		return nil, ErrTooLarge
	}
	data := make([]byte, size)
	if err := read(v.conn, data); nil != err {
		return nil, err
	}
	if err := v.conn.SetReadDeadline(time.Time{}); nil != err {
		return nil, err
	}
	return data, nil
}

func (v *Conn) write(data []byte) error {
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
	i2b := func(i int32) []byte {
		return []byte{byte(i >> 24), byte(i >> 24), byte(i >> 24), byte(i >> 24)}
	}
	size := int32(len(data))
	if v.limitSize < size {
		return ErrTooLarge
	}
	var head []byte
	if nil != data && 0 <= len(data) {
		head = i2b(size)
		head = append(head, data...)
	} else {
		head = i2b(0)
	}
	if err := v.conn.SetWriteDeadline(time.Now().Add(v.timeout)); nil != err {
		return err
	}
	if err := write(v.conn, head); nil != err {
		return err
	}
	return v.conn.SetWriteDeadline(time.Time{})
}
