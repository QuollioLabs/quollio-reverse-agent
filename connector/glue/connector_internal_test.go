package glue

import (
	"encoding/json"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/qdc"
	"reflect"
	"testing"
	"time"

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
		res := mapColumnAssetByColumnName(testCase.Input)
		for k, v := range res {
			if d := cmp.Diff(v, testCase.Expect[k]); len(d) != 0 {
				t.Errorf("want %v but got %v. Diff %s", testCase.Expect[k], v, d)
			}
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
		res := mapDBAssetByDBName(testCase.Input)
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
									Comment: nil,
								},
								{
									Name:    genStringPointer("test-column3"),
									Comment: genStringPointer("test-column-comment3"),
								},
								{
									Name:    genStringPointer("test-column4"),
									Comment: nil,
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
						Comment: genStringPointer("【QDIC】test-column-comment-qdc2"),
					},
					{
						Name:    genStringPointer("test-column3"),
						Comment: genStringPointer("test-column-comment3"),
					},
					{
						Name:    genStringPointer("test-column4"),
						Comment: nil,
					},
				},
				ShouldBeUpdated: true,
			},
		},
	}
	for _, testCase := range testCases {
		res, b := getDescUpdatedColumns(utils.OverwriteIfEmpty, testCase.Input.GlueTable, testCase.Input.ColumnAssets)
		if !reflect.DeepEqual(res, testCase.Expect.Columns) || b != testCase.Expect.ShouldBeUpdated {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func TestGenUpdateMessage(t *testing.T) {
	testCases := []struct {
		Input struct {
			TableUpdated  bool
			ColumnUpdated bool
		}
		Expect string
	}{
		{
			Input: struct {
				TableUpdated  bool
				ColumnUpdated bool
			}{
				true,
				true,
			},
			Expect: "Both table and column descriptions were updated.",
		},
		{
			Input: struct {
				TableUpdated  bool
				ColumnUpdated bool
			}{
				true,
				false,
			},
			Expect: "Table description was updated.",
		},
		{
			Input: struct {
				TableUpdated  bool
				ColumnUpdated bool
			}{
				false,
				true,
			},
			Expect: "Column descriptions were updated.",
		},
		{
			Input: struct {
				TableUpdated  bool
				ColumnUpdated bool
			}{
				false,
				false,
			},
			Expect: "Nothing was updated.",
		},
	}
	for _, testCase := range testCases {
		res := genUpdateMessage(testCase.Input.TableUpdated, testCase.Input.ColumnUpdated)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func TestGenUpdateDatabaseInput(t *testing.T) {
	testCases := []struct {
		Input  types.Database
		Expect glueService.UpdateDatabaseInput
	}{
		{
			Input: types.Database{
				Name:      genStringPointer("test-db1"),
				CatalogId: genStringPointer("AwsDataCatalog"),
				CreateTableDefaultPermissions: []types.PrincipalPermissions{
					{
						Permissions: []types.Permission{
							"READ",
						},
						Principal: &types.DataLakePrincipal{
							DataLakePrincipalIdentifier: genStringPointer("test-dl-pi1"),
						},
					},
				},
				CreateTime:  &time.Time{},
				Description: genStringPointer("test-desc1"),
				FederatedDatabase: &types.FederatedDatabase{
					ConnectionName: genStringPointer("test-conn1"),
					Identifier:     genStringPointer("test-conn-identifier1"),
				},
				LocationUri: genStringPointer("test-loc1"),
				Parameters: map[string]string{
					"test-key1": "test-value1",
					"test-key2": "test-value2",
				},
				TargetDatabase: &types.DatabaseIdentifier{
					CatalogId:    genStringPointer("AwsDataCatalog"),
					DatabaseName: genStringPointer("test-db1"),
					Region:       genStringPointer("ap-northeast-1"),
				},
			},
			Expect: glueService.UpdateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{
					Name: genStringPointer("test-db1"),
					CreateTableDefaultPermissions: []types.PrincipalPermissions{
						{
							Permissions: []types.Permission{
								"READ",
							},
							Principal: &types.DataLakePrincipal{
								DataLakePrincipalIdentifier: genStringPointer("test-dl-pi1"),
							},
						},
					},
					Description: genStringPointer("test-desc1"),
					FederatedDatabase: &types.FederatedDatabase{
						ConnectionName: genStringPointer("test-conn1"),
						Identifier:     genStringPointer("test-conn-identifier1"),
					},
					LocationUri: genStringPointer("test-loc1"),
					Parameters: map[string]string{
						"test-key1": "test-value1",
						"test-key2": "test-value2",
					},
					TargetDatabase: &types.DatabaseIdentifier{
						CatalogId:    genStringPointer("AwsDataCatalog"),
						DatabaseName: genStringPointer("test-db1"),
						Region:       genStringPointer("ap-northeast-1"),
					},
				},
				Name:      genStringPointer("test-db1"),
				CatalogId: genStringPointer("AwsDataCatalog"),
			},
		},
	}
	for _, testCase := range testCases {
		res := genUpdateDatabaseInput(testCase.Input)
		got, err := json.Marshal(res)
		if err != nil {
			t.Errorf("parse failed. %s", err.Error())
		}
		want, err := json.Marshal(testCase.Expect)
		if err != nil {
			t.Errorf("parse failed. %s", err.Error())
		}
		if string(got) != string(want) {
			t.Errorf("want %v but got %v.", want, got)
		}
	}
}

func TestGenUpdateTableInput(t *testing.T) {
	testCases := []struct {
		Input  *glueService.GetTableOutput
		Expect glueService.UpdateTableInput
	}{
		{
			Input: &glueService.GetTableOutput{
				Table: &types.Table{
					Name:             genStringPointer("test-table1"),
					CatalogId:        genStringPointer("AwsDataCatalog"),
					DatabaseName:     genStringPointer("test-db1"),
					Description:      genStringPointer("test-desc1"),
					LastAccessTime:   &time.Time{},
					LastAnalyzedTime: &time.Time{},
					Owner:            genStringPointer("quollio"),
					Parameters: map[string]string{
						"test-key1": "test-value1",
						"test-key2": "test-value2",
					},
					PartitionKeys: []types.Column{
						{
							Name:    genStringPointer("id"),
							Comment: genStringPointer("id-comment"),
							Type:    genStringPointer("string"),
						},
					},
					Retention: int32(1),
					StorageDescriptor: &types.StorageDescriptor{
						Columns: []types.Column{
							{
								Name:    genStringPointer("id"),
								Comment: genStringPointer("id-comment"),
								Type:    genStringPointer("string"),
							},
							{
								Name:    genStringPointer("name"),
								Comment: genStringPointer("name-comment"),
								Type:    genStringPointer("string"),
							},
							{
								Name:    genStringPointer("age"),
								Comment: genStringPointer("name-comment"),
								Type:    genStringPointer("int"),
							},
						},
					},
					TableType: genStringPointer("EXTERNAL_TABLE"),
					TargetTable: &types.TableIdentifier{
						CatalogId:    genStringPointer("AwsDataCatalog"),
						DatabaseName: genStringPointer("test-db1"),
						Name:         genStringPointer("test-table1"),
						Region:       genStringPointer("ap-northeast-1"),
					},
					ViewExpandedText: genStringPointer("test-view-expanded-text"),
					ViewOriginalText: genStringPointer("test-view-original-text"),
				},
			},
			Expect: glueService.UpdateTableInput{
				CatalogId:    genStringPointer("AwsDataCatalog"),
				DatabaseName: genStringPointer("test-db1"),
				TableInput: &types.TableInput{
					Description:      genStringPointer("test-desc1"),
					LastAccessTime:   &time.Time{},
					LastAnalyzedTime: &time.Time{},
					Name:             genStringPointer("test-table1"),
					Owner:            genStringPointer("quollio"),
					Parameters: map[string]string{
						"test-key1": "test-value1",
						"test-key2": "test-value2",
					},
					PartitionKeys: []types.Column{
						{
							Name:    genStringPointer("id"),
							Comment: genStringPointer("id-comment"),
							Type:    genStringPointer("string"),
						},
					},
					Retention: int32(1),
					StorageDescriptor: &types.StorageDescriptor{
						Columns: []types.Column{
							{
								Name:    genStringPointer("id"),
								Comment: genStringPointer("id-comment"),
								Type:    genStringPointer("string"),
							},
							{
								Name:    genStringPointer("name"),
								Comment: genStringPointer("name-comment"),
								Type:    genStringPointer("string"),
							},
							{
								Name:    genStringPointer("age"),
								Comment: genStringPointer("name-comment"),
								Type:    genStringPointer("int"),
							},
						},
					},
					TableType: genStringPointer("EXTERNAL_TABLE"),
					TargetTable: &types.TableIdentifier{
						CatalogId:    genStringPointer("AwsDataCatalog"),
						DatabaseName: genStringPointer("test-db1"),
						Name:         genStringPointer("test-table1"),
						Region:       genStringPointer("ap-northeast-1"),
					},
					ViewExpandedText: genStringPointer("test-view-expanded-text"),
					ViewOriginalText: genStringPointer("test-view-original-text"),
				},
			},
		},
	}
	for _, testCase := range testCases {
		res := genUpdateTableInput(testCase.Input)
		got, err := json.Marshal(res)
		if err != nil {
			t.Errorf("parse failed. %s", err.Error())
		}
		want, err := json.Marshal(testCase.Expect)
		if err != nil {
			t.Errorf("parse failed. %s", err.Error())
		}
		if string(got) != string(want) {
			t.Errorf("want %v but got %v.", want, got)
		}
	}
}

func TestShouldDatabaseBeUpdated(t *testing.T) {
	testCases := []struct {
		Input struct {
			GlueDB        types.Database
			DBAsset       qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db1"),
					Description: nil,
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db2"),
					Description: genStringPointer(""),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db2",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db3"),
					Description: nil,
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db3",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db4"),
					Description: genStringPointer(""),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db4",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db5"),
					Description: genStringPointer("test on console"),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db5",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db6"),
					Description: genStringPointer("test on console"),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db6",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db7"),
					Description: genStringPointer("【QDIC】test on console"),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db7",
					Description:  "test from qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db8"),
					Description: nil,
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db8",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db9"),
					Description: genStringPointer(""),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db9",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db10"),
					Description: nil,
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db10",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db11"),
					Description: genStringPointer(""),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db11",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db12"),
					Description: genStringPointer("test on console"),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db12",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db13"),
					Description: genStringPointer("test on console"),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db6",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueDB        types.Database
				DBAsset       qdc.Data
				OverwriteMode string
			}{
				GlueDB: types.Database{
					Name:        genStringPointer("test-db14"),
					Description: genStringPointer("【QDIC】test on console"),
				},
				DBAsset: qdc.Data{
					PhysicalName: "test-db7",
					Description:  "test from qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldDatabaseBeUpdated(testCase.Input.OverwriteMode, testCase.Input.GlueDB, testCase.Input.DBAsset)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func TestShouldTableBeUpdated(t *testing.T) {
	testCases := []struct {
		Input struct {
			GlueTable     types.Table
			TableAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer("【QDIC】test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test from qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueTable     types.Table
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueTable: types.Table{
					Name:        genStringPointer("test-table1"),
					Description: genStringPointer("【QDIC】test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-table1",
					Description:  "test from qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldTableBeUpdated(testCase.Input.OverwriteMode, &testCase.Input.GlueTable, testCase.Input.TableAsset)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func TestShouldColumnBeUpdated(t *testing.T) {
	testCases := []struct {
		Input struct {
			GlueColumn    types.Column
			TableAsset    qdc.Data
			OverwriteMode string
		}
		Expect bool
	}{
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer("【QDIC】test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test from qdc",
				},
				OverwriteMode: utils.OverwriteIfEmpty,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: nil,
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer(""),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer("test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: false,
		},
		{
			Input: struct {
				GlueColumn    types.Column
				TableAsset    qdc.Data
				OverwriteMode string
			}{
				GlueColumn: types.Column{
					Name:    genStringPointer("test-column1"),
					Comment: genStringPointer("【QDIC】test on console"),
				},
				TableAsset: qdc.Data{
					PhysicalName: "test-column1",
					Description:  "test from qdc",
				},
				OverwriteMode: utils.OverwriteAll,
			},
			Expect: true,
		},
	}
	for _, testCase := range testCases {
		res := shouldColumnBeUpdated(testCase.Input.OverwriteMode, testCase.Input.GlueColumn, testCase.Input.TableAsset)
		if res != testCase.Expect {
			t.Errorf("want %v but got %v.", testCase.Expect, res)
		}
	}
}

func genStringPointer(s string) *string {
	return &s
}
