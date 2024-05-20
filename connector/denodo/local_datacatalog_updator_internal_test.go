package denodo

import (
	"quollio-reverse-agent/repository/denodo/rest/models"
	"quollio-reverse-agent/repository/qdc"
	"testing"
)

func TestShouldUpdateDonodoLocalDatabase(t *testing.T) {
	testCases := []struct {
		Input struct {
			LocalAsset models.Database
			QdcDBAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				LocalAsset models.Database
				QdcDBAsset qdc.Data
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db1",
					DatabaseDescription: "test-db1-desc",
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
				LocalAsset models.Database
				QdcDBAsset qdc.Data
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db2",
					DatabaseDescription: "",
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
				LocalAsset models.Database
				QdcDBAsset qdc.Data
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db3",
					DatabaseDescription: "test-db3",
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
				LocalAsset models.Database
				QdcDBAsset qdc.Data
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db4",
					DatabaseDescription: "",
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
		res := shouldUpdateDenodoLocalDatabase(testCase.Input.LocalAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.LocalAsset.DatabaseName)
		}
	}
}

func TestShouldUpdateDenodoLocalTable(t *testing.T) {
	testCases := []struct {
		Input struct {
			LocalAsset    models.ViewDetail
			QdcTableAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-view1",
					Description: "test-view1-desc",
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table2",
					Description: "",
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table3",
					Description: "test-table3",
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table4",
					Description: "",
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
		res := shouldUpdateDenodoLocalTable(testCase.Input.LocalAsset, testCase.Input.QdcTableAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.LocalAsset.Name)
		}
	}
}

func TestShouldUpdateDenodoLocalColumn(t *testing.T) {
	testCases := []struct {
		Input struct {
			LocalAsset    models.ViewColumn
			QdcTableAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col1",
					Description: "test-col1-desc",
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col1",
					Description:  "test-col1",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "",
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col3",
					Description: "test-col3",
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
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col4",
					Description: "",
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
		res := shouldUpdateDenodoLocalColumn(testCase.Input.LocalAsset, testCase.Input.QdcTableAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.LocalAsset.Name)
		}
	}
}
