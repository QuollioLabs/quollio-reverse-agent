package glue

import (
	"context"
	"errors"
	"fmt"
	"quollio-reverse-agent/repository/glue/code"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	awsHttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

type GlueClient struct {
	GlueClient *glue.Client
}

func NewGlueClient(roleARN string, profileName string) (GlueClient, error) {
	switch profileName {
	case "":
		cfg, err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithRegion("ap-northeast-1"),
		)
		if err != nil {
			return GlueClient{}, err
		}
		glueClient := GlueClient{
			GlueClient: returnGlueClient(cfg, roleARN),
		}
		return glueClient, nil
	default:
		cfg, err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithRegion("ap-northeast-1"),
			config.WithSharedConfigProfile(profileName),
		)
		if err != nil {
			return GlueClient{}, err
		}
		glueClient := GlueClient{
			GlueClient: returnGlueClient(cfg, roleARN),
		}
		return glueClient, nil
	}
}

func returnGlueClient(cfg aws.Config, roleARN string) *glue.Client {
	stsSvc := sts.NewFromConfig(cfg)
	creds := stscreds.NewAssumeRoleProvider(stsSvc, roleARN)
	cfg.Credentials = aws.NewCredentialsCache(creds)
	glueClient := glue.NewFromConfig(cfg)
	return glueClient
}

func (g *GlueClient) GetDatabases(accountID, nextToken string) (*glue.GetDatabasesOutput, error) {
	ctx := context.Background()
	glueDBsInput := glue.GetDatabasesInput{
		CatalogId:         &accountID,
		NextToken:         &nextToken,
		ResourceShareType: "ALL",
	}
	dbs, err := g.GlueClient.GetDatabases(ctx, &glueDBsInput)
	if err != nil {
		var re *awsHttp.ResponseError
		if errors.As(err, &re) {
			switch {
			case strings.Contains(re.Err.Error(), "InvalidGrantException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.NOT_AUTHORIZED,
					Message:     fmt.Sprintf("Failed to glue.GetDatabases. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			case strings.Contains(re.Err.Error(), "EntityNotFoundException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.RESOURCE_NOT_FOUND,
					Message:     fmt.Sprintf("Failed to glue.GetDatabases. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			default:
				return nil, err
			}
		}
		return nil, err
	}
	return dbs, nil
}

func (g *GlueClient) UpdateDatabase(dbInput types.DatabaseInput, accountID, databaseName string) (*glue.UpdateDatabaseOutput, error) {
	ctx := context.Background()
	updateDBsInput := glue.UpdateDatabaseInput{
		DatabaseInput: &dbInput,
		CatalogId:     &accountID,
		Name:          &databaseName,
	}
	output, err := g.GlueClient.UpdateDatabase(ctx, &updateDBsInput)
	if err != nil {
		var re *awsHttp.ResponseError
		if errors.As(err, &re) {
			switch {
			case strings.Contains(re.Err.Error(), "InvalidGrantException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.NOT_AUTHORIZED,
					Message:     fmt.Sprintf("Failed to glue.UpdateDatabase. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			case strings.Contains(re.Err.Error(), "EntityNotFoundException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.RESOURCE_NOT_FOUND,
					Message:     fmt.Sprintf("Failed to glue.UpdateDatabase. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			default:
				return nil, err
			}
		}
		return nil, err
	}
	return output, nil
}

func (g *GlueClient) GetTable(catalogID, dbName, tableName string) (*glue.GetTableOutput, error) {
	ctx := context.Background()
	glueTableInput := glue.GetTableInput{
		CatalogId:    &catalogID,
		DatabaseName: &dbName,
		Name:         &tableName,
	}
	table, err := g.GlueClient.GetTable(ctx, &glueTableInput)
	if err != nil {
		var re *awsHttp.ResponseError
		if errors.As(err, &re) {
			switch {
			case strings.Contains(re.Err.Error(), "InvalidGrantException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.NOT_AUTHORIZED,
					Message:     fmt.Sprintf("Failed to glue.GetTable. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			case strings.Contains(re.Err.Error(), "EntityNotFoundException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.RESOURCE_NOT_FOUND,
					Message:     fmt.Sprintf("Failed to glue.GetTable. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			default:
				return nil, err
			}
		}
		return nil, err
	}
	return table, nil
}

func (g *GlueClient) UpdateTable(catalogID, dbName string, tableInput types.TableInput) (*glue.UpdateTableOutput, error) {
	ctx := context.Background()
	updateTableInput := glue.UpdateTableInput{
		CatalogId:    &catalogID,
		TableInput:   &tableInput,
		DatabaseName: &dbName,
	}
	output, err := g.GlueClient.UpdateTable(ctx, &updateTableInput)
	if err != nil {
		var re *awsHttp.ResponseError
		if errors.As(err, &re) {
			switch {
			case strings.Contains(re.Err.Error(), "InvalidGrantException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.NOT_AUTHORIZED,
					Message:     fmt.Sprintf("Failed to glue.UpdateTable Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			case strings.Contains(re.Err.Error(), "EntityNotFoundException:"):
				ge := code.GlueError{
					Number:      re.HTTPStatusCode(),
					ErrorReason: code.RESOURCE_NOT_FOUND,
					Message:     fmt.Sprintf("Failed to glue.UpdateTable. Error %s. %s", re.Err.Error(), err.Error()),
					Err:         re,
				}
				return nil, &ge
			default:
				return nil, err
			}
		}
		return nil, err
	}
	return output, nil
}
