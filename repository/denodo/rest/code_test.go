package rest

import (
	"net/http"
	"testing"
)

func TestGetErrorCode(t *testing.T) {
	res := http.Response{
		StatusCode: 401,
		Status:     "Unauthorized",
	}

	err := WrapError(&res)
	errorCode, extractErr := GetErrorCode(err)
	if errorCode != 401 && extractErr == nil {
		t.Errorf("GetErrorCode failed. Expect: %s but got %v", "401", errorCode)
	}
}
