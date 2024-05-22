package bigquery

import (
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/qdc"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/datacatalog/apiv1/datacatalogpb"
)

func TestShouldUpdateBqDatabase(t *testing.T) {
	testCases := []struct {
		Input struct {
			BqAsset       *bq.DatasetMetadata
			QdcDBAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db1",
					Description: "test-db1",
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
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db2",
					Description: "",
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
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db3",
					Description: "test-db3",
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
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db4",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-d4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db5",
					Description: "【QDIC】test-db5",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "test-db5",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db6",
					Description: "test-db6",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "test-db5",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db7",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db7",
					Description:  "test-db7",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db8",
					Description: "test-db8",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db8",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db9",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-d9",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.DatasetMetadata
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.DatasetMetadata{
					Name:        "test-db10",
					Description: "【QDIC】test-db10",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-db10",
					Description:  "test-db10",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateBqDataset("【QDIC】", testCase.Input.OverwriteMode, testCase.Input.BqAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.BqAsset.Name)
		}
	}
}

func TestShouldUpdateBqTable(t *testing.T) {
	testCases := []struct {
		Input struct {
			BqAsset       *datacatalogpb.Entry
			QdcDBAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
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
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
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
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
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
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
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
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
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
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table6",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "test-table6",
						},
					},
					Description: "test-table6",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table6",
					Description:  "test-table6",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table7",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table7",
					Description:  "test-table7",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table8",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "test-table8",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table8",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-table9",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-table9",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-test-table10",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "【QDIC】test-table10",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-table10",
					Description:  "test-test-table10",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-test-table11",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-table10",
					Description:  "test-test-table10",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *datacatalogpb.Entry
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &datacatalogpb.Entry{
					Name: "test-test-table10",
					BusinessContext: &datacatalogpb.BusinessContext{
						EntryOverview: &datacatalogpb.EntryOverview{
							Overview: "<p>【QDIC】aaa</p>",
						},
					},
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-table10",
					Description:  "test-test-table10",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateBqTable("【QDIC】", testCase.Input.OverwriteMode, testCase.Input.BqAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.BqAsset.Name)
		}
	}
}

func TestShouldUpdateBqColumn(t *testing.T) {
	testCases := []struct {
		Input struct {
			BqAsset       *bq.FieldSchema
			QdcDBAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column1",
					Description: "test-column1",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test-column1",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column2",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column2",
					Description:  "test-column2",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column3",
					Description: "test-column3",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column4",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column5",
					Description: "【QDIC】test-column5",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-column5",
					Description:  "test-test-column5",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column6",
					Description: "test-column6",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column6",
					Description:  "test-column6",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column7",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column7",
					Description:  "test-column7",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column8",
					Description: "test-column8",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column8",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column9",
					Description: "",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-column9",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				BqAsset       *bq.FieldSchema
				QdcDBAsset    qdc.Data
				OverwriteMode string
			}{
				BqAsset: &bq.FieldSchema{
					Name:        "test-column10",
					Description: "【QDIC】test-column10",
				},
				QdcDBAsset: qdc.Data{
					PhysicalName: "test-test-column10",
					Description:  "test-test-column10",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldUpdateBqColumn("【QDIC】", testCase.Input.OverwriteMode, testCase.Input.BqAsset, testCase.Input.QdcDBAsset)
		if res != testCase.Expect {
			t.Errorf("Test failed want %v but got %v. Name: %s", testCase.Expect, res, testCase.Input.BqAsset.Name)
		}
	}
}
