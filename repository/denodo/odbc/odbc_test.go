package odbc_test

import (
	"quollio-reverse-agent/repository/denodo/odbc"
	"testing"
)

func TestGetAlterViewType(t *testing.T) {
	testCases := []struct {
		Input  int
		Expect string
	}{
		{
			Input:  0,
			Expect: "table",
		},
		{
			Input:  1,
			Expect: "view",
		},
		{
			Input:  2,
			Expect: "view",
		},
		{
			Input:  3,
			Expect: "view",
		},
	}
	for _, testCase := range testCases {
		res := odbc.GetAlterViewType(testCase.Input)
		if testCase.Expect != res {
			t.Errorf("GetAlterViewType failed expect %s but got %s", testCase.Expect, res)
		}
	}
}
