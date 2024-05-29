# quollio-reverse-agent

## 説明

このライブラリは、Quollio Data Intelligence Cloud (QDIC)上のメタデータを取得し、各クラウドサービスのデータカタログに反映させます。  
現在対応しているデータカタログサービスは以下となっております。
- Google BigQuery(Dataplex)
- Amazon Athena(Glue DataCatalog)
- Denodo Virtual DataPort(Denodo DataCatalog)


## 前提条件
このシステムを使用する前に、以下の手順を実行する必要があります。
- Metadata Agentを使用して、データカタログにアセットを登録する。
- 外部API用の、QDIC上で認証に必要なクライアントIDとシークレットを発行する。


## 実行方法
ローカル環境で実行する際の説明を記載します。

1. リポジトリのルートレベルに`.env`ファイルを作成します。
1. 必要な環境変数をファイルに定義していきます。
1. 設定完了後、`make run`コマンドを実行します。

必要な環境変数は次のとおりです。
```
# システム共通
SYSTEM_NAME=<(Required) システム名。次のうちから一つ選択する。`bigquery`, `athena` or `denodo`>
COMPANY_ID=<(Required) QDICログインに使用するテナントID>
OVERWRITE_MODE=<(Required) OVERWRITE_IF_EMPTY or OVERWRITE_ALL。説明は下部に記載しています。デフォルト値は`OVERWRITE_IF_EMPTY`となります。>
QDC_BASE_URL=<(Required) QDIC EXternalAPIのBase URL>
QDC_CLIENT_ID=<(Required) QDIC EXternalAPIのクライアントID>
QDC_CLIENT_SECRET=<(Required) QDIC EXternalAPIのクライアントシークレット>
QDC_ASSET_CREATED_BY=<(Optional) QDICにアセットを登録したユーザー名。入力することで、更新するアセットをフィルタすることができます。>
LOG_LEVEL=<(Optional)ログレベル。デフォルトは`INFO`で、`DEBUG`に切り替えることで開発用のログを確認できます。>

# BigQuery
GOOGLE_CLOUD_SERVICE_ACCOUNT_CREDENTIALS=<(Required) サービスアカウントのJSON値>

# Athena
AWS_IAM_ROLE_FOR_GLUE_TABLE=<(Required) IAMロール名>
ATHENA_ACCOUNT_ID=<(Required) Athenaの存在するアカウントID>
PROFILE_NAME=<(Optional) ローカル実行する場合に必要となるプロファイル名>

# Denodo
DENODO_HOST_NAME=<(Required) VDPホスト名>
DENODO_CLIENT_ID=<(Required) VDPユーザー名>
DENODO_CLIENT_SECRET=<(Required) VDPユーザーパスワード>
DENODO_DEFUALT_DB_NAME=<(Required) VDPデフォルトデータベース>
DENODO_ODBC_PORT=<(Required) VDP ODBCポート>
DENODO_REST_API_PORT=<(Required) VDP REST APIポート>
```

OVERWRITE_MODEの値は次の条件に従って設定してください。
- OVERWRITE_IF_EMPTY: クラウドデータカタログ内のアセットの値がnull、空文字の場合に、QDICアセットの値で上書きする
- OVERWRITE_ALL: クラウドデータカタログ内のアセットの値がnull、空文字であるかに関わらず、QDICアセットの値で上書きする

## 開発

### ユニットテスト

To run unit tests, run the following command.

ユニットテストを実行するには、下記のコマンドを実行してください。

```
$ make test
```


## Description
This library retrieves metadata from the Quollio Data Intelligence Cloud (QDIC) and reflects it in the data catalog of each cloud service.
Currently supported data catalog services are:
- Google BigQuery (Dataplex)
- Amazon Athena (Glue DataCatalog)
- Denodo Virtual DataPort (Denodo DataCatalog)


## Prerequisite
Before using this system, you need to perform the following steps:

- Register assets in the data catalog using the Metadata Agent.
- Issue the client ID and secret required for authentication on QDIC for external APIs.

## Execution
The instructions for running locally are described below.

- Create a .env file at the root level of the repository.
- Define the necessary environment variables in the file.
- After completing the setup, run the `make run` command.

The required environment variables are as follows:
```
# System common
SYSTEM_NAME=<(Required) System name. Choose one from the following: `bigquery`, `athena`, or `denodo`>
COMPANY_ID=<(Required) Tenant ID used for QDIC login>
OVERWRITE_MODE=<(Required) OVERWRITE_IF_EMPTY or OVERWRITE_ALL. Descriptions are provided below. The default value is `OVERWRITE_IF_EMPTY`>
QDC_BASE_URL=<(Required) Base URL for QDIC External API>
QDC_CLIENT_ID=<(Required) Client ID for QDIC External API>
QDC_CLIENT_SECRET=<(Required) Client Secret for QDIC External API>
QDC_ASSET_CREATED_BY=<(Optional) Username of the user who registered the asset in QDIC. By entering this, you can filter the assets to be updated.>
LOG_LEVEL=<(Optional)Log level。`INFO` is set as default value. You can see debug log by switching it to `DEBUG`>

# BigQuery
GOOGLE_CLOUD_SERVICE_ACCOUNT_CREDENTIALS=<(Required) JSON value of the service account>

# Athena
AWS_IAM_ROLE_FOR_GLUE_TABLE=<(Required) IAM role name>
ATHENA_ACCOUNT_ID=<(Required) Account ID where Athena exists>
PROFILE_NAME=<(Optional) Profile name required for local execution>

# Denodo
DENODO_HOST_NAME=<(Required) VDP host name>
DENODO_CLIENT_ID=<(Required) VDP username>
DENODO_CLIENT_SECRET=<(Required) VDP user password>
DENODO_DEFAULT_DB_NAME=<(Required) VDP default database>
DENODO_ODBC_PORT=<(Required) VDP ODBC port>
DENODO_REST_API_PORT=<(Required) VDP REST API port>
```

Set the value of OVERWRITE_MODE according to the following conditions
- OVERWRITE_IF_EMPTY: Overwrite with the QDIC asset value if the asset value in the cloud data catalog is null or empty string.
- OVERWRITE_ALL: Overwrite with the QDIC asset value regardless of whether the asset value in the cloud data catalog is null or empty string.

## Development
### Unit Test
To run unit tests, run the following command
```
$ make test
```
