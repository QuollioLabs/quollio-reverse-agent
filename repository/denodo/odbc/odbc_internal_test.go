package odbc

import (
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
		res := getAlterViewType(testCase.Input)
		if testCase.Expect != res {
			t.Errorf("getAlterViewType failed expect %s but got %s", testCase.Expect, res)
		}
	}
}

func TestEscapeSingleQuoteInString(t *testing.T) {
	testCases := []struct {
		Input  string
		Expect string
	}{
		{
			Input:  "This is test",
			Expect: "This is test",
		},
		{
			Input:  "This is single quote's test",
			Expect: "This is single quote''s test",
		},
	}
	for _, testCase := range testCases {
		res := escapeSingleQuoteInString(testCase.Input)
		if testCase.Expect != res {
			t.Errorf("escapeSingleQuoteInString failed expect %s but got %s", testCase.Expect, res)
		}
	}
}
