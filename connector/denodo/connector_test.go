package denodo_test

import (
	"quollio-reverse-agent/connector/denodo"
	"quollio-reverse-agent/repository/denodo/rest/models"
	"quollio-reverse-agent/repository/qdc"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestConvertQdcAssetListToMap(t *testing.T) {
	testCases := []struct {
		Input  []qdc.Data
		Expect map[string]qdc.Data
	}{
		{
			Input: []qdc.Data{
				{
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
					ServiceName:  "athena",
					PhysicalName: "test-column-name1",
					Description:  "test-description",
					DataType:     "string",
				},
				{
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
							ID:         "clmn-5678",
							ObjectType: "column",
							Name:       "test-column-name2",
						},
					},
					ID:           "clmn-5678",
					ObjectType:   "column",
					ServiceName:  "athena",
					PhysicalName: "test-column-name2",
					Description:  "test-description",
					DataType:     "string",
				},
			},
			Expect: map[string]qdc.Data{
				"test-column-name1": {
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
					ServiceName:  "athena",
					PhysicalName: "test-column-name1",
					Description:  "test-description",
					DataType:     "string",
				},
				"test-column-name2": {
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
							ID:         "clmn-5678",
							ObjectType: "column",
							Name:       "test-column-name2",
						},
					},
					ID:           "clmn-5678",
					ObjectType:   "column",
					ServiceName:  "athena",
					PhysicalName: "test-column-name2",
					Description:  "test-description",
					DataType:     "string",
				},
			},
		},
	}
	for _, testCase := range testCases {
		res := denodo.ConvertQdcAssetListToMap(testCase.Input)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
		}
	}
}

func TestConvertLocalColumnListToMap(t *testing.T) {
	testCases := []struct {
		Input  []models.ViewColumn
		Expect map[string]models.ViewColumn
	}{
		{
			Input: []models.ViewColumn{
				{
					Name:        "test-view1",
					Description: "test-desc1",
				},
				{
					Name:        "test-view2",
					Description: "test-desc2",
				},
				{
					Name:        "test-view3",
					Description: "test-desc3",
				},
			},
			Expect: map[string]models.ViewColumn{
				"test-view1": {
					Name:        "test-view1",
					Description: "test-desc1",
				},
				"test-view2": {
					Name:        "test-view2",
					Description: "test-desc2",
				},
				"test-view3": {
					Name:        "test-view3",
					Description: "test-desc3",
				},
			},
		},
	}
	for _, testCase := range testCases {
		res := denodo.ConvertLocalColumnListToMap(testCase.Input)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
		}
	}
}

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
		res := denodo.GetAlterViewType(testCase.Input)
		if testCase.Expect != res {
			t.Errorf("GetAlterViewType failed expect %s but got %s", testCase.Expect, res)
		}
	}
}
