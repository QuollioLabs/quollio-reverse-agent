package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/common/utils"
	"quollio-reverse-agent/connector/bigquery"

	"quollio-reverse-agent/connector/denodo"
	"quollio-reverse-agent/connector/glue"

	"github.com/joho/godotenv"
)

func init() {
	log.Println("Load environment variables")
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Failed to load dot env. Use local environment variables")
	} else {
		log.Println("Environment variable will be loaded from dot env.")
	}
}

func runReverseAgent(systemName *string) error {
	logger := logger.NewBuiltinLogger()
	logger.Debug("System name: %s", *systemName)

	var overwriteMode string
	switch os.Getenv("OVERWRITE_MODE") {
	case utils.OverwriteAll:
		overwriteMode = utils.OverwriteAll
	default:
		overwriteMode = utils.OverwriteIfEmpty
	}
	var prefixForUpdate string
	switch os.Getenv("PREFIX_FOR_UPDATE") {
	case "":
		prefixForUpdate = utils.DefaultPrefix
	default:
		prefixForUpdate = os.Getenv("PREFIX_FOR_UPDATE")
	}
	logger.Debug("Overwrite mode: %s", overwriteMode)
	logger.Debug("PrefixForUpdate: %s", prefixForUpdate)

	logger.Info("Start ReflectMetadataToDataCatalog")
	switch *systemName {
	case "bigquery":
		logger.Info("Start to create NewBigQueryConnector.")
		BqConnector, err := bigquery.NewBigqueryConnector(prefixForUpdate, overwriteMode, logger)
		if err != nil {
			logger.Error("Failed to NewBigqueryConnector")
			return fmt.Errorf("Failed to NewBigqueryConnector")
		}
		logger.Info("Start to create ReflectMetadataToDataCatalog.")
		err = BqConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			logger.Error("Failed to ReflectMetadataToDataCatalog")
			return fmt.Errorf("Failed to ReflectMetadataToDataCatalog for BigQuery")
		}
	case "athena":
		logger.Info("Start to create NewGlueConnector.")
		GlueConnector, err := glue.NewGlueConnector(prefixForUpdate, overwriteMode, logger)
		if err != nil {
			logger.Error("Failed to NewGlueConnector")
			return fmt.Errorf("Failed to NewGlueConnector")
		}
		logger.Info("Start to create ReflectMetadataToDataCatalog.")
		err = GlueConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			logger.Error("Failed to ReflectMetadataToDataCatalog")
			return fmt.Errorf("Failed to ReflectMetadataToDataCatalog for Athena")
		}
	case "denodo":
		logger.Info("Start to create DenodoConnector.")
		DenodoConnector, err := denodo.NewDenodoConnector(prefixForUpdate, overwriteMode, logger)
		if err != nil {
			logger.Error("Failed to NewDenodoConnector")
			return fmt.Errorf("Failed to NewDenodoConnector")
		}
		defer DenodoConnector.DenodoDBClient.Conn.Close()
		logger.Info("Finish creating DenodoConnector.")
		logger.Info("Start to run ReflectMetadataToDataCatalog.")
		err = DenodoConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			logger.Error("Failed to ReflectMetadataToDataCatalog, %s", err.Error())
			return fmt.Errorf("Failed to ReflectMetadataToDataCatalog for Denodo")
		}
	default:
		return fmt.Errorf("You chose invalid service name.")
	}
	logger.Info("Done ReflectMetadataToDataCatalog")
	return nil
}

func main() {
	systemName := flag.String("system-name", os.Getenv("SYSTEM_NAME"), "You need to choose which connector to use.")
	flag.Parse()

	err := runReverseAgent(systemName)
	if err != nil {
		log.Fatal()
	}
}
