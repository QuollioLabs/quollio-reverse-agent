package qdc_test

import (
	"quollio-reverse-agent/repository/qdc"
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
		res := qdc.GetSpecifiedAssetFromPath(testCase.Input.Asset, testCase.Input.PathLayer)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func TestIsAssetContainsValueAsDescription(t *testing.T) {
	testCases := []struct {
		Input struct {
			Asset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				Asset qdc.Data
			}{
				Asset: qdc.Data{
					ID:           "clmn-1234",
					ObjectType:   "column",
					ServiceName:  "bigquery",
					PhysicalName: "test-column-name1",
					Description:  "test-description",
					DataType:     "string",
				},
			},
			Expect: true,
		},
		{
			Input: struct {
				Asset qdc.Data
			}{
				Asset: qdc.Data{
					ID:           "clmn-1234",
					ObjectType:   "column",
					ServiceName:  "bigquery",
					PhysicalName: "test-column-name1",
					Description:  "",
					DataType:     "string",
				},
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := qdc.IsAssetContainsValueAsDescription(testCase.Input.Asset)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}
