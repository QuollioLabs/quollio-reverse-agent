package main

import (
	"flag"
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

func main() {
	systemName := flag.String("system-name", os.Getenv("SYSTEM_NAME"), "You need to choose which connector to use.")
	flag.Parse()

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
		BqConnector, err := bigquery.NewBigqueryConnector(prefixForUpdate, overwriteMode, logger)
		if err != nil {
			logger.Error("Failed to NewBigqueryConnector")
			log.Fatal(err)
			return
		}
		err = BqConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			logger.Error("Failed to ReflectMetadataToDataCatalog")
			log.Fatal(err)
			return
		}
	case "athena":
		GlueConnector, err := glue.NewGlueConnector(prefixForUpdate, overwriteMode, logger)
		if err != nil {
			logger.Error("Failed to NewGlueConnector")
			log.Fatal(err)
			return
		}
		err = GlueConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			logger.Error("Failed to ReflectMetadataToDataCatalog")
			log.Fatal(err)
			return
		}
	case "denodo":
		DenodoConnector, err := denodo.NewDenodoConnector(prefixForUpdate, overwriteMode, logger)
		if err != nil {
			logger.Error("Failed to NewDenodoConnector")
			log.Fatal(err)
			return
		}
		err = DenodoConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			logger.Error("Failed to ReflectMetadataToDataCatalog")
			log.Fatal(err)
			return
		}
	default:
		log.Fatal("You chose invalid service name.")
		return
	}
	logger.Info("Done ReflectMetadataToDataCatalog")
}
