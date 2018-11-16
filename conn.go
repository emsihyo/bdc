package bdc

//Conn Conn
type Conn interface {
	Close()
	Write([]byte) error
	Read() ([]byte, error)
	RemoteAddr() string
}
