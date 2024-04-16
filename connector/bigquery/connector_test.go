package bigquery_test

import (
	"quollio-reverse-agent/connector/bigquery"
	"quollio-reverse-agent/repository/qdc"
	"reflect"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
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

func TestMapColumnAssetByColumnName(t *testing.T) {
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
					ServiceName:  "bigquery",
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
					ServiceName:  "bigquery",
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
					ServiceName:  "bigquery",
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
					ServiceName:  "bigquery",
					PhysicalName: "test-column-name2",
					Description:  "test-description",
					DataType:     "string",
				},
			},
		},
	}
	for _, testCase := range testCases {
		res := bigquery.MapColumnAssetByColumnName(testCase.Input)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
		}
	}
}

func TestGetDescUpdatedSchema(t *testing.T) {
	testCases := []struct {
		Input struct {
			GetAssetByIDsResponseData []qdc.Data
			TableMetadata             *bq.TableMetadata
		}
		Expect struct {
			FieldSchema     []*bq.FieldSchema
			ShouldBeUpdated bool
		}
	}{
		{
			Input: struct {
				GetAssetByIDsResponseData []qdc.Data
				TableMetadata             *bq.TableMetadata
			}{
				GetAssetByIDsResponseData: []qdc.Data{
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
						ServiceName:  "bigquery",
						PhysicalName: "test-column-name1",
						Description:  "",
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
						ServiceName:  "bigquery",
						PhysicalName: "test-column-name2",
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
								ID:         "clmn-9012",
								ObjectType: "column",
								Name:       "test-column-name3",
							},
						},
						ID:           "clmn-9012",
						ObjectType:   "column",
						ServiceName:  "bigquery",
						PhysicalName: "test-column-name3",
						Description:  "",
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
								ID:         "clmn-3456",
								ObjectType: "column",
								Name:       "test-column-name4",
							},
						},
						ID:           "clmn-3456",
						ObjectType:   "column",
						ServiceName:  "bigquery",
						PhysicalName: "test-column-name4",
						Description:  "test-description-for-test-column-4",
						DataType:     "string",
					},
				},
				TableMetadata: &bq.TableMetadata{
					Name: "test-table1",
					Schema: []*bq.FieldSchema{
						{
							Name:        "test-column-name1",
							Description: "",
						},
						{
							Name:        "test-column-name2",
							Description: "",
						},
						{
							Name:        "test-column-name3",
							Description: "test-desc-col3",
						},
						{
							Name:        "test-column-name4",
							Description: "test-desc-col4",
						},
					},
				},
			},
			Expect: struct {
				FieldSchema     []*bq.FieldSchema
				ShouldBeUpdated bool
			}{
				FieldSchema: []*bq.FieldSchema{
					{
						Name:        "test-column-name1",
						Description: "",
					},
					{
						Name:        "test-column-name2",
						Description: "test-description", // This desc is copied from qdc.
					},
					{
						Name:        "test-column-name3",
						Description: "test-desc-col3",
					},
					{
						Name:        "test-column-name4",
						Description: "test-desc-col4",
					},
				},
				ShouldBeUpdated: true,
			},
		},
		{
			Input: struct {
				GetAssetByIDsResponseData []qdc.Data
				TableMetadata             *bq.TableMetadata
			}{
				GetAssetByIDsResponseData: []qdc.Data{
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
						ServiceName:  "bigquery",
						PhysicalName: "test-column-name1",
						Description:  "",
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
						ServiceName:  "bigquery",
						PhysicalName: "test-column-name2",
						Description:  "",
						DataType:     "string",
					},
				},
				TableMetadata: &bq.TableMetadata{
					Name: "test-table1",
					Schema: []*bq.FieldSchema{
						{
							Name:        "test-column-name1",
							Description: "",
						},
						{
							Name:        "test-column-name2",
							Description: "",
						},
					},
				},
			},
			Expect: struct {
				FieldSchema     []*bq.FieldSchema
				ShouldBeUpdated bool
			}{
				FieldSchema: []*bq.FieldSchema{
					{
						Name:        "test-column-name1",
						Description: "",
					},
					{
						Name:        "test-column-name2",
						Description: "",
					},
				},
				ShouldBeUpdated: false,
			},
		},
	}
	for _, testCase := range testCases {
		res, b := bigquery.GetDescUpdatedSchema(testCase.Input.GetAssetByIDsResponseData, testCase.Input.TableMetadata)
		if !reflect.DeepEqual(res, testCase.Expect.FieldSchema) || b != testCase.Expect.ShouldBeUpdated {
			t.Errorf("want %+v, but got %+v", testCase.Expect, res)
		}
	}
}
