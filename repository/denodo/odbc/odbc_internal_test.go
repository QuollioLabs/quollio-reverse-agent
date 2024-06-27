package odbc

import (
	"strings"
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

func TestBuildQueryToGetDatabases(t *testing.T) {
	testCases := []struct {
		Input  []string
		Expect struct {
			DBQuery string
			Params  []interface{}
		}
	}{
		{
			Input: []string{},
			Expect: struct {
				DBQuery string
				Params  []interface{}
			}{
				DBQuery: `
					select
						db_name
						, description
					from
						get_databases()`,
				Params: []interface{}{},
			},
		},
		{
			Input: []string{"db1"},
			Expect: struct {
				DBQuery string
				Params  []interface{}
			}{
				DBQuery: `
					select
						db_name
						, description
					from
						get_databases()
					where
						db_name in ($1)`,
				Params: []interface{}{"db1"},
			},
		},
		{
			Input: []string{"db1", "db2"},
			Expect: struct {
				DBQuery string
				Params  []interface{}
			}{
				DBQuery: `
					select
						db_name
						, description
					from
						get_databases()
					where
						db_name in ($1, $2)`,
				Params: []interface{}{"db1", "db2"},
			},
		},
	}
	for _, testCase := range testCases {
		dbQuery, args, err := buildQueryToGetDatabases(testCase.Input)
		if err != nil {
			t.Errorf("buildQueryToGetDatabases err Occurred %s", err.Error())
		}
		normalizeQuery := normalizeWhitespace(dbQuery)
		normalizeExpectedQuery := normalizeWhitespace(testCase.Expect.DBQuery)
		if normalizeExpectedQuery != normalizeQuery {
			t.Errorf("buildQueryToGetDatabases failed expect %s but got %s", testCase.Expect, normalizeQuery)
		}
		if !equal(args, testCase.Expect.Params) {
			t.Errorf("buildQueryToGetDatabases failed expect %+v but got %s", testCase.Expect.Params, args)
		}
	}
}

// Function to normalize whitespace by collapsing it
func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func equal(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
