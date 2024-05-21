package bigquery

import (
	"quollio-reverse-agent/repository/qdc"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/datacatalog/apiv1/datacatalogpb"
)

func TestShouldUpdateBqDatabase(t *testing.T) {
	testCases := []struct {
		Input struct {
			BqAsset    *bq.DatasetMetadata
			QdcDBAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				BqAsset    *bq.DatasetMetadata
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db1",
					Description: "test-db1",
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
				BqAsset    *bq.DatasetMetadata
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db2",
					Description: "",
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
				BqAsset    *bq.DatasetMetadata
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db3",
					Description: "test-db3",
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
				BqAsset    *bq.DatasetMetadata
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db4",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-d4",
					Description:  "",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *bq.DatasetMetadata
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db5",
					Description: "【QDIC】test-db5",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "test-db5",
				},
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateBqDataset(testCase.Input.BqAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.BqAsset.Name)
		}
	}
}

func TestShouldUpdateBqTable(t *testing.T) {
	testCases := []struct {
		Input struct {
			BqAsset    *datacatalogpb.Entry
			QdcDBAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				BqAsset    *datacatalogpb.Entry
				QdcDBAsset qdc.Data
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table1",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "test-table1",
						},
					},
					Description: "test-table1",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test-table1",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *datacatalogpb.Entry
				QdcDBAsset qdc.Data
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table2",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table2",
					Description:  "test-table2",
				},
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset    *datacatalogpb.Entry
				QdcDBAsset qdc.Data
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table3",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "test-table3",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table3",
					Description:  "",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *datacatalogpb.Entry
				QdcDBAsset qdc.Data
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table4",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table4",
					Description:  "",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *datacatalogpb.Entry
				QdcDBAsset qdc.Data
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-test-table5",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "【QDIC】test-table5",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-table5",
					Description:  "test-test-table5",
				},
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateBqTable(testCase.Input.BqAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.BqAsset.Name)
		}
	}
}

func TestShouldUpdateBqColumn(t *testing.T) {
	testCases := []struct {
		Input struct {
			BqAsset    *bq.FieldSchema
			QdcDBAsset qdc.Data
		}
		Expect bool
	}{
		{
			Input: struct {
				BqAsset    *bq.FieldSchema
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column1",
					Description: "test-column1",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test-column1",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *bq.FieldSchema
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column2",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column2",
					Description:  "test-column2",
				},
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset    *bq.FieldSchema
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column3",
					Description: "test-column3",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column3",
					Description:  "",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *bq.FieldSchema
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column4",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column4",
					Description:  "",
				},
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset    *bq.FieldSchema
				QdcDBAsset qdc.Data
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column5",
					Description: "【QDIC】test-column5",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-column5",
					Description:  "test-test-column5",
				},
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateBqColumn(testCase.Input.BqAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.BqAsset.Name)
		}
	}
}
