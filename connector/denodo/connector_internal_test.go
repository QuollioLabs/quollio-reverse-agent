package denodo

import (
	"database/sql"
	"fmt"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/odbc/models"
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
				"clmn-1234": {
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
				"clmn-5678": {
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
		res := convertQdcAssetListToMap(testCase.Input)
		fmt.Println(res)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
		}
	}
}

func TestShouldUpdateDenodoVdpDatabase(t *testing.T) {
	testCases := []struct {
		Input struct {
			VdpAsset      models.GetDatabasesResult
			QdcDBAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				VdpAsset      models.GetDatabasesResult
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetDatabasesResult{
					DatabaseName: "test-db1",
					Description: sql.NullString{
						Valid:  true,
						String: "test-db1",
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db1",
					Description:  "test-db1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetDatabasesResult
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetDatabasesResult{
					DatabaseName: "test-db2",
					Description: sql.NullString{
						Valid:  false,
						String: "",
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db2",
					Description:  "test-db2",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetDatabasesResult
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetDatabasesResult{
					DatabaseName: "test-db3",
					Description: sql.NullString{
						Valid:  true,
						String: "test-db3",
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetDatabasesResult
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetDatabasesResult{
					DatabaseName: "test-db4",
					Description: sql.NullString{
						Valid:  false,
						String: "",
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetDatabasesResult
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetDatabasesResult{
					DatabaseName: "test-db5",
					Description: sql.NullString{
						Valid:  true,
						String: "【QDIC】test",
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "【QDIC】test1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetDatabasesResult
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetDatabasesResult{
					DatabaseName: "test-db6",
					Description: sql.NullString{
						Valid:  true,
						String: "test【QDIC】",
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db6",
					Description:  "【QDIC】test1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoVdpDatabase(testCase.Input.OverwriteMode, testCase.Input.VdpAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.VdpAsset.DatabaseName)
		}
	}
}

func TestShouldUpdateDenodoVdpTable(t *testing.T) {
	testCases := []struct {
		Input struct {
			VdpAsset      models.GetViewsResult
			QdcTableAsset qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewsResult{
					ViewName: "test-view1",
					Description: sql.NullString{
						Valid:  true,
						String: "test-view1",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test-table1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewsResult{
					ViewName: "test-table2",
					Description: sql.NullString{
						Valid:  false,
						String: "",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table2",
					Description:  "test-table2",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewsResult{
					ViewName: "test-table3",
					Description: sql.NullString{
						Valid:  true,
						String: "test-table3",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewsResult{
					ViewName: "test-table4",
					Description: sql.NullString{
						Valid:  false,
						String: "",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewsResult{
					ViewName: "test-table5",
					Description: sql.NullString{
						Valid:  true,
						String: "【QDIC】test5",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table5",
					Description:  "【QDIC】test5",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewsResult{
					ViewName: "test-table6",
					Description: sql.NullString{
						Valid:  true,
						String: "test6【QDIC】",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table6",
					Description:  "【QDIC】test6",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoVdpTable(testCase.Input.OverwriteMode, testCase.Input.VdpAsset, testCase.Input.QdcTableAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.VdpAsset.ViewName)
		}
	}
}

func TestShouldUpdateDenodoVdpColumn(t *testing.T) {
	testCases := []struct {
		Input struct {
			VdpAsset      models.GetViewColumnsResult
			QdcTableAsset qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewColumnsResult{
					ColumnName: "test-col1",
					ColumnRemarks: sql.NullString{
						Valid:  false,
						String: "",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col1",
					Description:  "test-col1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewColumnsResult{
					ColumnName: "test-col2",
					ColumnRemarks: sql.NullString{
						Valid:  true,
						String: "test-col2",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewColumnsResult{
					ColumnName: "test-col3",
					ColumnRemarks: sql.NullString{
						Valid:  true,
						String: "test-col3",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewColumnsResult{
					ColumnName: "test-col4",
					ColumnRemarks: sql.NullString{
						Valid:  false,
						String: "",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewColumnsResult{
					ColumnName: "test-col5",
					ColumnRemarks: sql.NullString{
						Valid:  true,
						String: "【QDIC】test5",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col5",
					Description:  "【QDIC】test5",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				VdpAsset: models.GetViewColumnsResult{
					ColumnName: "test-col6",
					ColumnRemarks: sql.NullString{
						Valid:  true,
						String: "test6【QDIC】",
					},
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col6",
					Description:  "【QDIC】test6",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoVdpColumn(testCase.Input.OverwriteMode, testCase.Input.VdpAsset, testCase.Input.QdcTableAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.VdpAsset.ColumnName)
		}
	}
}
