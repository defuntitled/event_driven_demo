package contracts

import "fmt"

type Failure struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Status    int    `json:"-"`
}

func (e *Failure) Error() string { return e.Code + ": " + e.Message }
func Fail(status int, code, message string) error {
	return &Failure{Code: code, Message: message, Status: status, Retryable: status >= 500}
}
func Protocol(format string, args ...any) error {
	return Fail(500, "PROTOCOL_ERROR", fmt.Sprintf(format, args...))
}
