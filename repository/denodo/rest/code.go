package rest

import (
	"errors"
	"fmt"
	"net/http"
)

type DenodoRestAPIError struct {
	ErrorCode    int
	ErrorMessage string
}

func (d *DenodoRestAPIError) Error() string {
	return fmt.Sprintf("DenodoRestAPI Execution failed. Code: %v, Message: %s", d.ErrorCode, d.ErrorMessage)
}

func WrapError(data *http.Response) error {
	denodoAPIError := DenodoRestAPIError{
		ErrorCode:    data.StatusCode,
		ErrorMessage: data.Status,
	}
	return &denodoAPIError
}

func GetErrorCode(err error) (int, error) {
	var denodoErr *DenodoRestAPIError
	if errors.As(err, &denodoErr) {
		return denodoErr.ErrorCode, nil
	}
	return 0, fmt.Errorf("Error is not of type DenodoRestAPIError")
}
