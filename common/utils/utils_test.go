package utils_test

import (
	"quollio-reverse-agent/common/utils"
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

func TestAddPrefixToStringIfNotHas(t *testing.T) {
	testCases := []struct {
		Input struct {
			PrefixForUpdate string
			Str             string
		}
		Expect string
	}{
		{
			Input: struct {
				PrefixForUpdate string
				Str             string
			}{
				"【QDIC】",
				"test-string",
			},
			Expect: "【QDIC】test-string",
		},
		{
			Input: struct {
				PrefixForUpdate string
				Str             string
			}{
				"【QDIC】",
				"【QDIC】test-string",
			},
			Expect: "【QDIC】test-string",
		},
	}
	for _, testCase := range testCases {
		res := utils.AddPrefixToStringIfNotHas(testCase.Input.PrefixForUpdate, testCase.Input.Str)
		if res != testCase.Expect {
			t.Errorf("test failed. want %s, but got %s", testCase.Expect, res)
		}
	}
}

func TestIsStringContainJapanese(t *testing.T) {
	testCases := []struct {
		Input  string
		Expect bool
	}{
		{
			Input:  "あ",
			Expect: true,
		},
		{
			Input:  "カ",
			Expect: true,
		},
		{
			Input:  "他",
			Expect: true,
		},
		{
			Input:  "aioueo",
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := utils.IsStringContainJapanese(testCase.Input)
		if res != testCase.Expect {
			t.Errorf("test failed. want %v, but got %v", testCase.Expect, res)
		}
	}
}
