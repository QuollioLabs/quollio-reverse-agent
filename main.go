package main

import (
	"flag"
	"log"
	"os"
	"quollio-reverse-agent/common/logger"
	"quollio-reverse-agent/connector/bigquery"
<<<<<<< HEAD
	"quollio-reverse-agent/connector/denodo"
	"quollio-reverse-agent/connector/glue"
=======
>>>>>>> 530cb55e4a0e681a50d3e0076b14fc44107bda58

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
	log.Println("Start ReflectMetadataToDataCatalog")
	switch *systemName {
	case "bigquery":
		BqConnector, err := bigquery.NewBigqueryConnector(logger)
		if err != nil {
			log.Println("Failed to NewBigqueryConnector")
			log.Fatal(err)
			return
		}
		err = BqConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			log.Println("Failed to ReflectMetadataToDataCatalog")
			log.Fatal(err)
			return
		}
	case "athena":
		GlueConnector, err := glue.NewGlueConnector(logger)
		if err != nil {
			log.Println("Failed to NewGlueConnector")
			log.Fatal(err)
			return
		}
		err = GlueConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			log.Println("Failed to ReflectMetadataToDataCatalog")
			log.Fatal(err)
			return
		}
	case "denodo":
		DenodoConnector, err := denodo.NewDenodoConnector(logger)
		if err != nil {
			log.Println("Failed to NewDenodoConnector")
			log.Fatal(err)
			return
		}
		err = DenodoConnector.ReflectMetadataToDataCatalog()
		if err != nil {
			log.Println("Failed to ReflectMetadataToDataCatalog")
			log.Fatal(err)
			return
		}
	default:
		log.Fatal("You chose invalid service name.")
		return
	}
	log.Println("Done ReflectMetadataToDataCatalog")
}
