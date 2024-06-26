package denodo

import (
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/denodo/rest/models"
	"quollio-reverse-agent/repository/qdc"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestShouldUpdateDonodoLocalDatabase(t *testing.T) {
	testCases := []struct {
		Input struct {
			LocalAsset    models.Database
			QdcDBAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db1",
					DatabaseDescription: "test-db1-desc",
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
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db2",
					DatabaseDescription: "",
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
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db3",
					DatabaseDescription: "test-db3",
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
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db4",
					DatabaseDescription: "",
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
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db5",
					DatabaseDescription: "【QDIC】testDescription",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "test",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db1",
					DatabaseDescription: "test-db1-desc",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db1",
					Description:  "test-db1",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db2",
					DatabaseDescription: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db2",
					Description:  "test-db2",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db3",
					DatabaseDescription: "test-db3",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db4",
					DatabaseDescription: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.Database
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.Database{
					DatabaseName:        "test-db5",
					DatabaseDescription: "【QDIC】testDescription",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "test",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoLocalDatabase("【QDIC】", testCase.Input.OverwriteMode, testCase.Input.LocalAsset, testCase.Input.QdcDBAsset)
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
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-view1",
					Description: "test-view1-desc",
					InLocal:     true,
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table2",
					Description: "",
					InLocal:     true,
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table3",
					Description: "test-table3",
					InLocal:     true,
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table4",
					Description: "",
					InLocal:     true,
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
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table5",
					Description: "",
					InLocal:     false,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table5",
					Description:  "test-table5",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table6",
					Description: "【QDIC】test",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table6",
					Description:  "test-table6",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-view1",
					Description: "test-view1-desc",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test-table1",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table2",
					Description: "",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table2",
					Description:  "test-table2",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table3",
					Description: "test-table3",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table4",
					Description: "",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table2",
					Description: "",
					InLocal:     false,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table2",
					Description:  "test-table2",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewDetail
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewDetail{
					Name:        "test-table7",
					Description: "【QDIC】test",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-table7",
					Description:  "test-table7",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoLocalTable("【QDIC】", testCase.Input.OverwriteMode, testCase.Input.LocalAsset, testCase.Input.QdcTableAsset)
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
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col1",
					Description: "test-col1-desc",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col1",
					Description:  "test-col1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col3",
					Description: "test-col3",
					InLocal:     true,
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
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col4",
					Description: "",
					InLocal:     true,
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
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "",
					InLocal:     false,
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
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "【QDIC】a",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col1",
					Description: "test-col1-desc",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col1",
					Description:  "test-col1",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col3",
					Description: "test-col3",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col4",
					Description: "",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "",
					InLocal:     false,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				LocalAsset    models.ViewColumn
				QdcTableAsset qdc.Data
				OverwriteMode string
			}{
				LocalAsset: models.ViewColumn{
					Name:        "test-col2",
					Description: "【QDIC】a",
					InLocal:     true,
				},
				QdcTableAsset: qdc.Data{
					PhysicalName: "test-col2",
					Description:  "test-col2",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateDenodoLocalColumn("【QDIC】", testCase.Input.OverwriteMode, testCase.Input.LocalAsset, testCase.Input.QdcTableAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.LocalAsset.Name)
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
		res := convertLocalColumnListToMap(testCase.Input)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
		}
	}
}
