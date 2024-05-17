package glue

import (
	"errors"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/repository/glue"
	"quollio-reverse-agent/repository/glue/code"
	"quollio-reverse-agent/repository/qdc"
	"quollio-reverse-agent/utils"
	"reflect"

	glueService "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

type GlueConnector struct {
	QDCExternalAPIClient qdc.QDCExternalAPI
	GlueRepo             glue.GlueClient
	AthenaAccountID      string
	Logger               *logger.BuiltinLogger
}

func NewGlueConnector(logger *logger.BuiltinLogger) (GlueConnector, error) {
	iamRoleARN := os.Getenv("AWS_IAM_ROLE_FOR_GLUE_TABLE")
	profileName := os.Getenv("PROFILE_NAME")
	athenaAccountID := os.Getenv("ATHENA_ACCOUNT_ID")
	glueClient, err := glue.NewGlueClient(iamRoleARN, profileName)
	if err != nil {
		return GlueConnector{}, err
	}

	qdcBaseURL := os.Getenv("QDC_BASE_URL")
	qdcClientID := os.Getenv("QDC_CLIENT_ID")
	qdcClientSecret := os.Getenv("QDC_CLIENT_SECRET")
	externalAPI := qdc.NewQDCExternalAPI(qdcBaseURL, qdcClientID, qdcClientSecret)
	connector := GlueConnector{
		QDCExternalAPIClient: externalAPI,
		GlueRepo:             glueClient,
		AthenaAccountID:      athenaAccountID,
		Logger:               logger,
	}

	return connector, nil
}

func (g *GlueConnector) GetAllAthenaRootAssets() ([]qdc.Data, error) {
	var rootAssets []qdc.Data

	var lastAssetID string
	for {
		assetResponse, err := g.QDCExternalAPIClient.GetAssetByType("schema", lastAssetID)
		if err != nil {
			g.Logger.Error("Failed to GetAssetByType. lastAssetID: %s", lastAssetID)
			return nil, err
		}
		for _, assetData := range assetResponse.Data {
			switch assetData.ServiceName {
			case "athena":
				rootAssets = append(rootAssets, assetData)
			default:
				continue
			}
		}
		switch assetResponse.LastID {
		case "":
			return rootAssets, nil
		default:
			g.Logger.Debug("GetAllAthenaRootAssets will continue. lastAssetID: %s", lastAssetID)
			lastAssetID = assetResponse.LastID
		}
	}
}

func (g *GlueConnector) GetAllChildAssetsByID(parentAssets []qdc.Data) ([]qdc.Data, error) {
	var childAssets []qdc.Data

	for _, parentAsset := range parentAssets {
		childAssetIdChunks := utils.SplitArrayToChunks(parentAsset.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
		for _, childAssetIdChunk := range childAssetIdChunks {
			assets, err := g.QDCExternalAPIClient.GetAssetByIDs(childAssetIdChunk)
			if err != nil {
				return nil, err
			}
			childAssets = append(childAssets, assets.Data...)
		}
	}
	if os.Getenv("LOG_LEVEL") == "DEBUG" {
		g.Logger.Debug("The number of child assets is %v", len(childAssets))
		var childAssetIds []string
		for _, childAsset := range childAssets {
			childAssetIds = append(childAssetIds, childAsset.ID)
		}
		g.Logger.Debug("The child asset ids are %v", childAssetIds)
	}
	return childAssets, nil
}

func (g *GlueConnector) GetChildAssetsByParentAsset(assets qdc.Data) ([]qdc.Data, error) {
	var childAssets []qdc.Data

	childAssetIdChunks := utils.SplitArrayToChunks(assets.ChildAssetIds, 100) // MEMO: 100 is the max size of the each array.
	for _, childAssetIdChunk := range childAssetIdChunks {
		assets, err := g.QDCExternalAPIClient.GetAssetByIDs(childAssetIdChunk)
		if err != nil {
			return nil, err
		}
		childAssets = append(childAssets, assets.Data...)
	}
	g.Logger.Debug("The number of child asset chunks is %v", len(childAssets))
	return childAssets, nil
}

func (g *GlueConnector) ReflectDatabaseDescToAthena(dbAssets []qdc.Data) error {
	allGlueDBs, err := g.GetAllDatabases()
	if err != nil {
		return err
	}
	mapDBAssetByDBName := MapDBAssetByDBName(allGlueDBs)

	for _, dbAsset := range dbAssets {
		if glueDB, ok := mapDBAssetByDBName[dbAsset.PhysicalName]; ok {
			updateDatabaseInput := GenUpdateDatabaseInput(glueDB)

			if ShouldDatabaseBeUpdated(glueDB, dbAsset) {
				updateDatabaseInput.DatabaseInput.Description = &dbAsset.Description
				_, err := g.GlueRepo.UpdateDatabase(updateDatabaseInput, g.AthenaAccountID)
				if err != nil {
					var ge *code.GlueError
					if errors.As(err, &ge) {
						if ge.ErrorReason == code.RESOURCE_NOT_FOUND {
							g.Logger.Warning("Database Not Found in your AWS account. Skip to ingest the table name: %s", *updateDatabaseInput.Name)
							continue
						}
					}
					return err
				}
			}
		}
		// Todo: display diff after updating.
	}
	return nil
}

func (g GlueConnector) GetAllDatabases() ([]types.Database, error) {
	var glueDatabases []types.Database
	var nextToken string
	for {
		dbOutput, err := g.GlueRepo.GetDatabases(g.AthenaAccountID, nextToken)
		if err != nil {
			return []types.Database{}, err
		}
		glueDatabases = append(glueDatabases, dbOutput.DatabaseList...)
		if dbOutput.NextToken == nil {
			return glueDatabases, err
		}
		nextToken = *dbOutput.NextToken
	}
}

func (g *GlueConnector) ReflectTableAttributeToAthena(tableAssets []qdc.Data) error {
	for _, tableAsset := range tableAssets {
		tableShouldBeUpdated := false
		databaseAsset := GetSpecifiedAssetFromPath(tableAsset, "schema3")

		glueTable, err := g.GlueRepo.GetTable(g.AthenaAccountID, databaseAsset.Name, tableAsset.PhysicalName)
		if err != nil {
			var ge *code.GlueError
			if errors.As(err, &ge) {
				if ge.ErrorReason == code.RESOURCE_NOT_FOUND {
					g.Logger.Warning("Table Not Found in your AWS account. Skip to ingest the table name: %s", tableAsset.PhysicalName)
					continue
				}
			}
			return err
		}
		updateTableInput := GenUpdateTableInput(glueTable)
		if ShouldTableBeUpdated(*glueTable.Table, tableAsset) {
			updateTableInput.TableInput.Description = &tableAsset.Description
			tableShouldBeUpdated = true
		}
		columnAssets, err := g.GetChildAssetsByParentAsset(tableAsset)
		if err != nil {
			return err
		}
		updatedColumns, columnShouldBeUpdated := GetDescUpdatedColumns(glueTable, columnAssets)
		if columnShouldBeUpdated {
			updateTableInput.TableInput.StorageDescriptor.Columns = updatedColumns
		}
		if tableShouldBeUpdated || columnShouldBeUpdated {
			_, err = g.GlueRepo.UpdateTable(g.AthenaAccountID, databaseAsset.Name, updateTableInput)
			if err != nil {
				return err
			}
			msg := GenUpdateMessage(tableShouldBeUpdated, columnShouldBeUpdated)
			g.Logger.Debug("Update table. msg: %s table name %s", msg, tableAsset.PhysicalName)
		}
		// Todo: validate table def by compare the output and previous version.
	}
	return nil
}

func (g *GlueConnector) ReflectMetadataToDataCatalog() error {
	g.Logger.Info("List Athena database assets")
	rootAssets, err := g.GetAllAthenaRootAssets()
	if err != nil {
		g.Logger.Error("Failed to GetAllAthenaRootAssets: %s", err.Error())
		return err
	}

	g.Logger.Info("List Athena schema assets")
	schemaAssets, err := g.GetAllChildAssetsByID(rootAssets)
	if err != nil {
		g.Logger.Error("Failed to GetAllChildAssetsByID for schemaAssets: %s", err.Error())
		return err
	}

	err = g.ReflectDatabaseDescToAthena(schemaAssets)
	if err != nil {
		g.Logger.Error("Failed to ReflectDatabaseDescToAthena for schemaAssets: %s", err.Error())
		return err
	}

	g.Logger.Info("List Athena table assets")
	tableAssets, err := g.GetAllChildAssetsByID(schemaAssets)
	if err != nil {
		g.Logger.Error("Failed to GetAllChildAssetsByID: %s", err.Error())
		return err
	}

	err = g.ReflectTableAttributeToAthena(tableAssets)
	if err != nil {
		g.Logger.Error("Failed to ReflectTableAttributeToAthena: %s", err.Error())
		return err
	}
	return nil
}

func GetDescUpdatedColumns(glueTable *glueService.GetTableOutput, columnAssets []qdc.Data) ([]types.Column, bool) {
	var updatedColumns []types.Column
	shouldBeUpdated := false
	mapColumnAssetByColumnName := MapColumnAssetByColumnName(columnAssets)
	if glueTable.Table.StorageDescriptor == nil {
		return []types.Column{}, false
	}
	for _, column := range glueTable.Table.StorageDescriptor.Columns {
		var columnName string
		if column.Name != nil {
			columnName = *column.Name
		}
		if columnAsset, ok := mapColumnAssetByColumnName[columnName]; ok {
			if ShouldColumnBeUpdated(column, columnAsset) {
				updatedColumn := column
				updatedColumn.Comment = &columnAsset.Description
				updatedColumns = append(updatedColumns, updatedColumn)
				shouldBeUpdated = true
			} else {
				updatedColumns = append(updatedColumns, column)
			}
		} else {
			updatedColumns = append(updatedColumns, column)
		}
	}
	return updatedColumns, shouldBeUpdated
}

func MapColumnAssetByColumnName(columnAssets []qdc.Data) map[string]qdc.Data {
	mapColumnAssetsByColumnName := make(map[string]qdc.Data)
	for _, columnAsset := range columnAssets {
		mapColumnAssetsByColumnName[columnAsset.PhysicalName] = columnAsset
	}
	return mapColumnAssetsByColumnName
}

func GetSpecifiedAssetFromPath(asset qdc.Data, pathLayer string) qdc.Path {
	path := asset.Path
	for _, p := range path {
		if p.PathLayer == pathLayer {
			return p
		}
	}
	return qdc.Path{}
}

func MapDBAssetByDBName(databases []types.Database) map[string]types.Database {
	mapDBAssetByDBName := make(map[string]types.Database)
	for _, database := range databases {
		var dbName string
		if database.Name != nil {
			dbName = *database.Name
		}
		mapDBAssetByDBName[dbName] = database
	}
	return mapDBAssetByDBName
}

func MapTableAssetByTableName(tables []types.Table) map[string]types.Table {
	mapTableAssetByTableName := make(map[string]types.Table)
	for _, table := range tables {
		var tableName string
		if table.Name != nil {
			tableName = *table.Name
		}
		mapTableAssetByTableName[tableName] = table
	}
	return mapTableAssetByTableName
}

func GenUpdateMessage(tableUpdated, columnUpdated bool) string {
	var message string
	switch {
	case tableUpdated && columnUpdated:
		message = "Both table and column descriptions were updated."
	case tableUpdated && !columnUpdated:
		message = "Table description was updated."
	case !tableUpdated && columnUpdated:
		message = "Column descriptions were updated."
	default: // both false
		message = "Nothing was updated."
	}
	return message
}

func GenUpdateDatabaseInput(getDatabaseOutput types.Database) glueService.UpdateDatabaseInput {
	databaseInput := types.DatabaseInput{}
	databaseInputValueOf := reflect.ValueOf(&databaseInput).Elem()
	databaseInputTypeOf := reflect.TypeOf(databaseInput)
	getDatabaseOutputTypeOf := reflect.ValueOf(getDatabaseOutput)

	for i := 0; i < databaseInputTypeOf.NumField(); i++ {
		databaseInputField := databaseInputTypeOf.Field(i)
		valueOfGetDatabaseOutput := getDatabaseOutputTypeOf.FieldByName(databaseInputField.Name)
		if valueOfGetDatabaseOutput.IsValid() && valueOfGetDatabaseOutput.CanInterface() {
			databaseInputFieldValue := databaseInputValueOf.Field(i)
			databaseInputFieldValue.Set(valueOfGetDatabaseOutput)
		}
	}
	updateTableInput := glueService.UpdateDatabaseInput{
		DatabaseInput: &databaseInput,
		Name:          databaseInput.Name,
		CatalogId:     getDatabaseOutput.CatalogId,
	}
	return updateTableInput
}

func GenUpdateTableInput(getTableOutput *glueService.GetTableOutput) glueService.UpdateTableInput {
	tableInput := types.TableInput{}
	tableInputValueOf := reflect.ValueOf(&tableInput).Elem()
	tableInputTypeOf := reflect.TypeOf(tableInput)
	getTableOutputTypeOf := reflect.ValueOf(*getTableOutput.Table)

	for i := 0; i < tableInputTypeOf.NumField(); i++ {
		tableInputField := tableInputTypeOf.Field(i)
		valueOfGetTableOutput := getTableOutputTypeOf.FieldByName(tableInputField.Name)
		if valueOfGetTableOutput.IsValid() && valueOfGetTableOutput.CanInterface() {
			tableInputFieldValue := tableInputValueOf.Field(i)
			tableInputFieldValue.Set(valueOfGetTableOutput)
		}
	}
	updateTableInput := glueService.UpdateTableInput{
		CatalogId:    getTableOutput.Table.CatalogId,
		DatabaseName: getTableOutput.Table.DatabaseName,
		TableInput:   &tableInput,
	}
	return updateTableInput
}

func ShouldDatabaseBeUpdated(glueDB types.Database, dbAsset qdc.Data) bool {
	if (glueDB.Description == nil || *glueDB.Description == "") && dbAsset.Description != "" {
		return true
	}
	return false
}

func ShouldTableBeUpdated(glueTable types.Table, tableAsset qdc.Data) bool {
	if (glueTable.Description == nil || *glueTable.Description == "") && tableAsset.Description != "" {
		return true
	}
	return false
}

func ShouldColumnBeUpdated(glueColumn types.Column, columnAsset qdc.Data) bool {
	if (glueColumn.Comment == nil || *glueColumn.Comment == "") && columnAsset.Description != "" {
		return true
	}
	return false
}
