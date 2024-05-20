package denodo

import (
	"database/sql"
	"quollio-reverse-agent/repository/denodo/odbc/models"
	"quollio-reverse-agent/repository/qdc"
	"testing"
)

func TestShouldUpdateDenodoVdpDatabase(t *testing.T) {
	testCases := []struct {
		Input struct {
			VdpAsset   models.GetDatabasesResult
			QdcDBAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				VdpAsset   models.GetDatabasesResult
				QdcDBAsset qdc.Data
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
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset   models.GetDatabasesResult
				QdcDBAsset qdc.Data
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
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset   models.GetDatabasesResult
				QdcDBAsset qdc.Data
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
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset   models.GetDatabasesResult
				QdcDBAsset qdc.Data
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
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoVdpDatabase(testCase.Input.VdpAsset, testCase.Input.QdcDBAsset)
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
		}
		Expect bool
	}{
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoVdpTable(testCase.Input.VdpAsset, testCase.Input.QdcTableAsset)
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
		}
		Expect bool
	}{
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: true,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: false,
		},
		{
			Input: struct {
				VdpAsset      models.GetViewColumnsResult
				QdcTableAsset qdc.Data
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
			},
			Expect: false,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoVdpColumn(testCase.Input.VdpAsset, testCase.Input.QdcTableAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.VdpAsset.ColumnName)
		}
	}
}
