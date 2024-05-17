package utils_test

import (
	"quollio-reverse-agent/utils"
	"reflect"
	"testing"
)

func TestSplitArrayToChunks(t *testing.T) {
	testCases := []struct {
		Input struct {
			Arr  []string
			Size int
		}
		Expect [][]string
	}{
		{
			Input: struct {
				Arr  []string
				Size int
			}{
				[]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
				2,
			},
			Expect: [][]string{{"1", "2"}, {"3", "4"}, {"5", "6"}, {"7", "8"}, {"9", "10"}},
		},
		{
			Input: struct {
				Arr  []string
				Size int
			}{
				[]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
				3,
			},
			Expect: [][]string{{"1", "2", "3"}, {"4", "5", "6"}, {"7", "8", "9"}, {"10"}},
		},
	}
	for _, testCase := range testCases {
		res := utils.SplitArrayToChunks(testCase.Input.Arr, testCase.Input.Size)
		if !reflect.DeepEqual(res, testCase.Expect) {
			t.Errorf("want %+v but got %+v", testCase.Expect, res)
		}
	}
}
