# quollio-reverse-agent

## Description (説明)

This Python library collects metadata from QDIC and ingests it into data catalog corresponding to each cloud service.

このライブラリは、QDIC上のメタデータを取得し、各クラウドサービスのデータカタログに反映させます。


## Prerequisite (前提条件)
Before you begin to use this, you need to do the following.
- Add your assets to QDC with metadata agent.
- Issue client id and client secret on QDC for External API.

このシステムを使用する前に、以下の手順を実行する必要があります。
- Metadata Agentを使用して、データカタログにアセットを登録する。
- 外部API用の、データカタログ上で認証に必要なクライアントIDとシークレットを発行する。


## Development (開発)

### Install (インストール)

Create `.env` file in the root level of repository and set the following environment variables.

リポジトリのルートレベルに`.env`ファイルを作成し、下記の環境変数を設定してください。

```
SYSTEM_NAME=<dwh name like bigquery or athena>
GOOGLE_CLOUD_SERVICE_ACCOUNT_CREDENTIALS=<if you use google cloud service, you need to specify it>
QDC_BASE_URL=<quollio external base url>
QDC_CLIENT_ID=<quollio client id>
QDC_CLIENT_SECRET=<quollio client secret>
```

### Build (ビルド)

To build Docker image, run the following command.

Docker imageをビルドするには、下記のコマンドを実行してください。

```
$ make build
```

### Unit test (ユニットテスト)

To run unit tests, run the following command.

ユニットテストを実行するには、下記のコマンドを実行してください。

```
$ make test
```
