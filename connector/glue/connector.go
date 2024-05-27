package glue

import (
	"errors"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/repository/glue"
	"quollio-reverse-agent/repository/glue/code"
	"quollio-reverse-agent/repository/qdc"
	"reflect"
	"strings"

	glueService "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

type GlueConnector struct {
	QDCExternalAPIClient qdc.QDCExternalAPI
	GlueRepo             glue.GlueClient
	AssetCreatedBy       string
	AthenaAccountID      string
	OverwriteMode        string
	PrefixForUpdate      string
	Logger               *logger.BuiltinLogger
}

func NewGlueConnector(prefixForUpdate, overwriteMode string, logger *logger.BuiltinLogger) (GlueConnector, error) {
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
	assetCreatedBy := os.Getenv("QDC_ASSET_CREATED_BY")
	externalAPI := qdc.NewQDCExternalAPI(qdcBaseURL, qdcClientID, qdcClientSecret, logger)
	connector := GlueConnector{
		QDCExternalAPIClient: externalAPI,
		GlueRepo:             glueClient,
		AssetCreatedBy:       assetCreatedBy,
		AthenaAccountID:      athenaAccountID,
		OverwriteMode:        overwriteMode,
		PrefixForUpdate:      prefixForUpdate,
		Logger:               logger,
	}

	return connector, nil
}

func (g *GlueConnector) ReflectDatabaseDescToAthena(dbAssets []qdc.Data) error {
	allGlueDBs, err := g.GetAllDatabases()
	if err != nil {
		return err
	}
	mapDBAssetByDBName := mapDBAssetByDBName(allGlueDBs)

	for _, dbAsset := range dbAssets {
		if glueDB, ok := mapDBAssetByDBName[dbAsset.PhysicalName]; ok {
			updateDatabaseInput := genUpdateDatabaseInput(glueDB)

			if shouldDatabaseBeUpdated(g.PrefixForUpdate, g.OverwriteMode, glueDB, dbAsset) {
				g.Logger.Debug("Database will be updated. name %s", *glueDB.Name)
				descWithPrefix := utils.AddPrefixToStringIfNotHas(g.PrefixForUpdate, dbAsset.Description)
				updateDatabaseInput.DatabaseInput.Description = &descWithPrefix
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
				g.Logger.Debug("Update database. name %s", *glueDB.Name)
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
		databaseAsset := utils.GetSpecifiedAssetFromPath(tableAsset, "schema3")

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
		updateTableInput := genUpdateTableInput(glueTable)
		if shouldTableBeUpdated(g.PrefixForUpdate, g.OverwriteMode, glueTable.Table, tableAsset) {
			descWithPrefix := utils.AddPrefixToStringIfNotHas(g.PrefixForUpdate, tableAsset.Description)
			g.Logger.Debug("Table will be updated: %s", *glueTable.Table.Name)
			updateTableInput.TableInput.Description = &descWithPrefix
			tableShouldBeUpdated = true
		}
		columnAssets, err := g.QDCExternalAPIClient.GetChildAssetsByParentAsset(tableAsset)
		if err != nil {
			return err
		}
		updatedColumns, columnShouldBeUpdated := getDescUpdatedColumns(g.PrefixForUpdate, g.OverwriteMode, glueTable, columnAssets)
		if columnShouldBeUpdated {
			updateTableInput.TableInput.StorageDescriptor.Columns = updatedColumns
		}
		if tableShouldBeUpdated || columnShouldBeUpdated {
			_, err = g.GlueRepo.UpdateTable(g.AthenaAccountID, databaseAsset.Name, updateTableInput)
			if err != nil {
				return err
			}
			msg := genUpdateMessage(tableShouldBeUpdated, columnShouldBeUpdated)
			g.Logger.Debug("Update table. msg: %s table name %s", msg, tableAsset.PhysicalName)
		}
		// Todo: validate table def by compare the output and previous version.
	}
	return nil
}

func (g *GlueConnector) ReflectMetadataToDataCatalog() error {
	g.Logger.Info("List Athena database assets")
	rootAssets, err := g.QDCExternalAPIClient.GetAllRootAssets("athena", g.AssetCreatedBy)
	if err != nil {
		g.Logger.Error("Failed to GetAllAthenaRootAssets: %s", err.Error())
		return err
	}

	g.Logger.Info("List Athena schema assets")
	schemaAssets, err := g.QDCExternalAPIClient.GetAllChildAssetsByID(rootAssets)
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
	tableAssets, err := g.QDCExternalAPIClient.GetAllChildAssetsByID(schemaAssets)
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

func getDescUpdatedColumns(prefixForUpdate, overwriteMode string, glueTable *glueService.GetTableOutput, columnAssets []qdc.Data) ([]types.Column, bool) {
	var updatedColumns []types.Column
	shouldBeUpdated := false
	mapColumnAssetByColumnName := mapColumnAssetByColumnName(columnAssets)
	if glueTable.Table.StorageDescriptor == nil {
		return []types.Column{}, false
	}
	for _, column := range glueTable.Table.StorageDescriptor.Columns {
		var columnName string
		if column.Name != nil {
			columnName = *column.Name
		}
		if columnAsset, ok := mapColumnAssetByColumnName[columnName]; ok {
			if shouldColumnBeUpdated(prefixForUpdate, overwriteMode, column, columnAsset) {
				updatedColumn := column
				descWithPrefix := utils.AddPrefixToStringIfNotHas(prefixForUpdate, columnAsset.Description)
				updatedColumn.Comment = &descWithPrefix
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

func mapColumnAssetByColumnName(columnAssets []qdc.Data) map[string]qdc.Data {
	mapColumnAssetsByColumnName := make(map[string]qdc.Data)
	for _, columnAsset := range columnAssets {
		mapColumnAssetsByColumnName[columnAsset.PhysicalName] = columnAsset
	}
	return mapColumnAssetsByColumnName
}

func mapDBAssetByDBName(databases []types.Database) map[string]types.Database {
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

func genUpdateMessage(tableUpdated, columnUpdated bool) string {
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

func genUpdateDatabaseInput(getDatabaseOutput types.Database) glueService.UpdateDatabaseInput {
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

func genUpdateTableInput(getTableOutput *glueService.GetTableOutput) glueService.UpdateTableInput {
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

func shouldDatabaseBeUpdated(prefixForUpdate, overwriteMode string, glueDB types.Database, dbAsset qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && dbAsset.Description != "" {
		return true
	}

	if (glueDB.Description == nil || *glueDB.Description == "") && dbAsset.Description != "" {
		return true
	}
	if (glueDB.Description == nil || strings.HasPrefix(*glueDB.Description, prefixForUpdate)) && dbAsset.Description != "" {
		return true
	}
	return false
}

func shouldTableBeUpdated(prefixForUpdate, overwriteMode string, glueTable *types.Table, tableAsset qdc.Data) bool {
	if glueTable == nil {
		return false
	}
	if overwriteMode == utils.OverwriteAll && tableAsset.Description != "" {
		return true
	}

	if (glueTable.Description == nil || *glueTable.Description == "") && tableAsset.Description != "" {
		return true
	}
	if (glueTable.Description == nil || strings.HasPrefix(*glueTable.Description, prefixForUpdate)) && tableAsset.Description != "" {
		return true
	}
	return false
}

func shouldColumnBeUpdated(prefixForUpdate, overwriteMode string, glueColumn types.Column, columnAsset qdc.Data) bool {
	if overwriteMode == utils.OverwriteAll && columnAsset.Description != "" {
		return true
	}

	if (glueColumn.Comment == nil || *glueColumn.Comment == "") && columnAsset.Description != "" {
		return true
	}
	if (glueColumn.Comment == nil || strings.HasPrefix(*glueColumn.Comment, prefixForUpdate)) && columnAsset.Description != "" {
		return true
	}
	return false
}
