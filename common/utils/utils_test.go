package utils_test

import (
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/connector/bigquery"
	"quollio-reverse-agent/repository/qdc"
	"reflect"
	"testing"
)

func TestGetSpecifiedAssetFromPath(t *testing.T) {
	testCases := []struct {
		Input struct {
			Asset     qdc.Data
			PathLayer string
		}
		Expect qdc.Path
	}{
		{
			Input: struct {
				Asset     qdc.Data
				PathLayer string
			}{
				Asset: qdc.Data{
					Path: []qdc.Path{
						{
							PathLayer:  "schema4",
							ID:         "schm-1234",
							ObjectType: "schema",
							Name:       "test-project",
						},
						{
							PathLayer:  "schema3",
							ID:         "schm-5678",
							ObjectType: "schema",
							Name:       "test-dataset1",
						},
						{
							PathLayer:  "table",
							ID:         "tbl-1234",
							ObjectType: "table",
							Name:       "test-table1",
						},
						{
							PathLayer:  "column",
							ID:         "clmn-1234",
							ObjectType: "column",
							Name:       "test-column-name1",
						},
					},
					ID:           "clmn-1234",
					ObjectType:   "column",
					ServiceName:  "bigquery",
					PhysicalName: "test-column-name1",
					Description:  "test-description",
					DataType:     "string",
				},
				PathLayer: "schema3",
			},
			Expect: qdc.Path{
				PathLayer:  "schema3",
				ID:         "schm-5678",
				ObjectType: "schema",
				Name:       "test-dataset1",
			},
		},
	}
	for _, testCase := range testCases {
		res := bigquery.GetSpecifiedAssetFromPath(testCase.Input.Asset, testCase.Input.PathLayer)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

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

func TestAddQDICToStringAsPrefix(t *testing.T) {
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
	}
	for _, testCase := range testCases {
		res := utils.AddQDICToStringAsPrefix(testCase.Input.PrefixForUpdate, testCase.Input.Str)
		if res != testCase.Expect {
			t.Errorf("test failed. want %s, but got %s", testCase.Expect, res)
		}
	}
}
