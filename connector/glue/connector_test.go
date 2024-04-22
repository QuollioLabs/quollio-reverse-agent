package glue_test

import (
	"quollio-reverse-agent/connector/glue"
	"quollio-reverse-agent/repository/qdc"
	"reflect"
	"testing"

	glueService "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/go-cmp/cmp"
)

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
		res := glue.MapColumnAssetByColumnName(testCase.Input)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
		}
	}
}

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
					ServiceName:  "athena",
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
		res := glue.GetSpecifiedAssetFromPath(testCase.Input.Asset, testCase.Input.PathLayer)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func TestMapDBAssetByDBName(t *testing.T) {
	testCases := []struct {
		Input  []types.Database
		Expect map[string]types.Database
	}{
		{
			Input: []types.Database{
				{
					Name:        genStringPointer("test-db-name1"),
					Description: genStringPointer("test-db-description1"),
				},
				{
					Name:        genStringPointer("test-db-name2"),
					Description: genStringPointer("test-db-description2"),
				},
			},
			Expect: map[string]types.Database{
				"test-db-name1": {
					Name:        genStringPointer("test-db-name1"),
					Description: genStringPointer("test-db-description1"),
				},
				"test-db-name2": {
					Name:        genStringPointer("test-db-name2"),
					Description: genStringPointer("test-db-description2"),
				},
			},
		},
	}
	for _, testCase := range testCases {
		res := glue.MapDBAssetByDBName(testCase.Input)
		for k, v := range res {
			if !reflect.DeepEqual(v, testCase.Expect[k]) {
				t.Errorf("want %v but got %v.", testCase.Expect[k], v)
			}
		}
	}
}

func TestMapTableAssetByTableName(t *testing.T) {
	testCases := []struct {
		Input  []types.Table
		Expect map[string]types.Table
	}{
		{
			Input: []types.Table{
				{
					Name:        genStringPointer("test-table-name1"),
					Description: genStringPointer("test-table-description1"),
				},
				{
					Name:        genStringPointer("test-table-name2"),
					Description: genStringPointer("test-table-description2"),
				},
			},
			Expect: map[string]types.Table{
				"test-table-name1": {
					Name:        genStringPointer("test-table-name1"),
					Description: genStringPointer("test-table-description1"),
				},
				"test-table-name2": {
					Name:        genStringPointer("test-table-name2"),
					Description: genStringPointer("test-table-description2"),
				},
			},
		},
	}
	for _, testCase := range testCases {
		res := glue.MapTableAssetByTableName(testCase.Input)
		for k, v := range res {
			if !reflect.DeepEqual(v, testCase.Expect[k]) {
				t.Errorf("want %v but got %v.", testCase.Expect[k], v)
			}
		}
	}
}

func TestGetDescUpdatedColumns(t *testing.T) {
	testCases := []struct {
		Input struct {
			GlueTable    *glueService.GetTableOutput
			ColumnAssets []qdc.Data
		}
		Expect struct {
			Columns         []types.Column
			ShouldBeUpdated bool
		}
	}{
		{
			Input: struct {
				GlueTable    *glueService.GetTableOutput
				ColumnAssets []qdc.Data
			}{
				GlueTable: &glueService.GetTableOutput{
					Table: &types.Table{
						Name: genStringPointer("test-table1"),
						StorageDescriptor: &types.StorageDescriptor{
							Columns: []types.Column{
								{
									Name:    genStringPointer("test-column1"),
									Comment: genStringPointer("test-column-comment1"),
								},
								{
									Name:    genStringPointer("test-column2"),
									Comment: genStringPointer(""),
								},
								{
									Name:    genStringPointer("test-column3"),
									Comment: genStringPointer("test-column-comment3"),
								},
								{
									Name:    genStringPointer("test-column4"),
									Comment: genStringPointer(""),
								},
							},
						},
					},
				},
				ColumnAssets: []qdc.Data{
					{
						PhysicalName: "test-column1",
						Description:  "test-column-comment-qdc1",
					},
					{
						PhysicalName: "test-column2",
						Description:  "test-column-comment-qdc2",
					},
					{
						PhysicalName: "test-column3",
						Description:  "",
					},
					{
						PhysicalName: "test-column4",
						Description:  "",
					},
				},
			},
			Expect: struct {
				Columns         []types.Column
				ShouldBeUpdated bool
			}{
				Columns: []types.Column{
					{
						Name:    genStringPointer("test-column1"),
						Comment: genStringPointer("test-column-comment1"),
					},
					{
						Name:    genStringPointer("test-column2"),
						Comment: genStringPointer("test-column-comment-qdc2"),
					},
					{
						Name:    genStringPointer("test-column3"),
						Comment: genStringPointer("test-column-comment3"),
					},
					{
						Name:    genStringPointer("test-column4"),
						Comment: genStringPointer(""),
					},
				},
				ShouldBeUpdated: true,
			},
		},
	}
	for _, testCase := range testCases {
		res, b := glue.GetDescUpdatedColumns(testCase.Input.GlueTable, testCase.Input.ColumnAssets)
		if !reflect.DeepEqual(res, testCase.Expect.Columns) || b != testCase.Expect.ShouldBeUpdated {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func genStringPointer(s string) *string {
	return &s
}
