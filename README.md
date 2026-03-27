# dragonfly5_client_go

Dragonfly5 サーバーに HTTP 経由で接続し、RDB 操作（Query / Execute / Transaction）を行う Go クライアントライブラリ。

## パッケージ構成

```
client_go/
├── go.mod                    # module: github.com/cycau/dragonfly5_client_go
├── rdb/
│   ├── client.go             # Init, Client (Query/Execute)
│   ├── clientTx.go           # TxClient (トランザクション操作)
│   ├── types.go              # 型定義 (Params, ValueType, QueryOptions, etc.)
│   ├── switcher.go           # ノード選択・ヘルスチェック・リトライ・リダイレクト
│   └── parseQueryResult.go   # レスポンス解析 (JSON / Binary プロトコル v4)
└── README.md
```

## インストール

```bash
go get github.com/cycau/dragonfly5_client_go
```

## 初期化

### YAML ファイルから初期化

```go
import rdbclient "github.com/cycau/dragonfly5_client_go/rdb"

err := rdbclient.InitWithYamlFile("config.yaml")
```

**config.yaml の例:**

```yaml
maxConcurrency: 100
defaultSecretKey: "secret-key"
defaultDatabase: "crm-system"

clusterNodes:
  - baseUrl: "http://localhost:5678"
    secretKey: "secret-key"
  - baseUrl: "http://localhost:5679"
    # secretKey 省略時は defaultSecretKey が適用される
```

| フィールド | 説明 |
|---|---|
| `maxConcurrency` | HTTP 同時リクエスト上限（最小 10） |
| `defaultSecretKey` | ノード個別の secretKey 未指定時に適用される共通キー |
| `defaultDatabase` | `rdbclient.GetDefault()` 時に使用されるデフォルト DB 名 |
| `clusterNodes` | 接続先ノードの一覧 |

### コードから直接初期化

```go
nodes := []rdbclient.NodeEntry{
    {BaseURL: "http://localhost:5678", SecretKey: "secret-key"},
    {BaseURL: "http://localhost:5679", SecretKey: "secret-key"},
}
err := rdbclient.Init(nodes, "defaultDatabase", 100)
```

## 基本的な使い方

### Query（SELECT）

```go
dbClient := rdbclient.GetDefault()
// または: dbClient := rdbclient.Get("crm-system", "")

params := rdbclient.NewParams().
    Add("user-001", rdbclient.ValueType_STRING)

records, err := dbClient.Query(
    context.Background(),
    "SELECT * FROM users WHERE id = $1",
    params,
    nil, // QueryOptions (省略可)
)
if err != nil {
    log.Fatal(err)
}

for i := 0; i < records.Size(); i++ {
    row := records.Get(i)
    fmt.Println(row.Get("id"), row.Get("email"))
}
```

### Query オプション（ページネーション・タイムアウト）

```go
records, err := dbClient.Query(
    ctx,
    "SELECT * FROM users WHERE status = $1",
    params,
    &rdbclient.QueryOptions{
        OffsetRows: 0,
        LimitRows:  100,
        TimeoutSec: 30,
    },
)
// 注意！ OffsetRows, LimitRows はSQLに組み込まれるわけではなく、全件取得が行われるので、
// データ量が少ない場合、records.TotalCountで一気に総件数まで取得したい場合のみ使ってください。
```

### Execute（INSERT / UPDATE / DELETE）

```go
dbClient := rdbclient.GetDefault()

params := rdbclient.NewParams().
    Add("user-001", rdbclient.ValueType_STRING).
    Add("new@example.com", rdbclient.ValueType_STRING)

result, err := dbClient.Execute(
    context.Background(),
    "UPDATE users SET email = $2 WHERE id = $1",
    params,
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("affected rows: %d\n", result.EffectedRows)
```

## トランザクション

### 基本的な使い方（NewTxDefault）

最もシンプルな方法。デフォルト DB・デフォルト分離レベル・自動 traceId で開始する。

```go
txClient, err := rdbclient.NewTxDefault()
if err != nil {
    log.Fatal(err)
}
defer txClient.Close() // 必ず Close する

// トランザクション内で Execute
_, err = txClient.Execute(
    "UPDATE users SET email = $2 WHERE id = $1",
    rdbclient.NewParams().
        Add("user-001", rdbclient.ValueType_STRING).
        Add("tx@example.com", rdbclient.ValueType_STRING),
)

// トランザクション内で Query
records, err := txClient.Query(
    "SELECT * FROM users WHERE id = $1",
    rdbclient.NewParams().Add("user-001", rdbclient.ValueType_STRING),
    nil,
)

// Commit
err = txClient.Commit()
```

### オプション指定（NewTx）

DB 名・分離レベル・タイムアウトを明示的に指定する場合は `NewTx` を使う。

```go
txClient, err := rdbclient.NewTx(
    context.Background(),
    "crm-system",                      // DB 名（空文字でデフォルト）
    rdbclient.Isolation_ReadCommitted,  // 分離レベル（空文字でサーバーデフォルト）
    60,                                // 最大タイムアウト秒（0 でサーバーデフォルト）
    "",                                // traceId（空文字で自動生成）
)
```

| 引数 | 型 | 説明 |
|---|---|---|
| `ctx` | `context.Context` | コンテキスト |
| `databaseName` | `string` | 対象 DB 名。空文字で `defaultDatabase` を使用 |
| `isolationLevel` | `IsolationLevel` | トランザクション分離レベル。空文字でサーバーデフォルト |
| `maxTxTimeoutSec` | `int` | トランザクションの最大有効秒数。0 でサーバーデフォルト（アクセスする度にN秒延長） |
| `traceId` | `string` | トレース ID。空文字で `"d5" + nanoid(10)` を自動生成 |

### TxClient のメソッド一覧

| メソッド | 説明 |
|---|---|
| `Query(sql, params, opts)` | トランザクション内で SELECT を実行。戻り値は `Client.Query` と同じ |
| `Execute(sql, params)` | トランザクション内で INSERT/UPDATE/DELETE を実行 |
| `BulkInsert(tableName, columnNames, records)` | トランザクション内で一括 INSERT を実行（自動チャンク分割） |
| `Commit()` | コミット |
| `Rollback()` | ロールバック |
| `Close()` | クローズ |
| `GetTxId()` | シリアライズ可能なトランザクション ID を返す |

### BulkInsert（一括 INSERT）

トランザクション内で大量レコードを一括 INSERT する。パラメータ数が `20,000` を超える場合は自動的にチャンク分割して実行される。

```go
txClient, err := rdbclient.NewTxDefault()
if err != nil {
    log.Fatal(err)
}
defer txClient.Close()

// レコードを構築
records := make([]*rdbclient.Params, 0, 5000)
for i := 0; i < 5000; i++ {
    records = append(records, rdbclient.NewParams().
        Add(fmt.Sprintf("user-%05d", i), rdbclient.ValueType_STRING).
        Add(fmt.Sprintf("user%d@example.com", i), rdbclient.ValueType_STRING).
        Add(i%100, rdbclient.ValueType_INT),
    )
}

result, err := txClient.BulkInsert(
    "users",
    []string{"id", "email", "age"},
    records,
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("inserted rows: %d\n", result.EffectedRows)

txClient.Commit()
```

| 引数 | 型 | 説明 |
|---|---|---|
| `tableName` | `string` | 対象テーブル名 |
| `columnNames` | `[]string` | カラム名の一覧 |
| `records` | `[]*Params` | 各レコードのパラメータ。各要素の長さは `columnNames` と一致する必要がある |

- パラメータ数 20,000 を上限にチャンク分割（例: 10 カラムなら 2,000 行/チャンク）
- 途中でエラーが発生した場合はそこで中断し、呼び出し側で `Rollback()` / `Close()` できる

### トランザクション ID の復元（RestoreTx）

別プロセス・別リクエストでトランザクションを引き継ぐ場合:

```go
// トランザクション ID を取得して保存
txId := txClient.GetTxId()

// 別の場所で復元
restoredTx, err := rdbclient.RestoreTx(txId, context.Background())
defer restoredTx.Release()

// 復元した TxClient で操作を続行
restoredTx.Commit()
```

## パラメータの型 (ValueType)

| ValueType | Go の型 | 説明 |
|---|---|---|
| `ValueType_NULL` | `nil` | NULL |
| `ValueType_BOOL` | `bool` | 真偽値 |
| `ValueType_INT` | `int16` / `int32` / `int64` | 整数 |
| `ValueType_LONG` | `int64` | 長整数 |
| `ValueType_FLOAT` | `float32` | 単精度浮動小数 |
| `ValueType_DOUBLE` | `float64` | 倍精度浮動小数 |
| `ValueType_DECIMAL` | `decimal.Decimal` | 任意精度小数（内部で文字列として送信） |
| `ValueType_DATE` | `time.Time` | 日付 |
| `ValueType_DATETIME` | `time.Time` | 日時 |
| `ValueType_STRING` | `string` | 文字列 |
| `ValueType_BYTES` | `[]byte` | バイナリ（Base64 エンコードで送信） |
| `ValueType_AS_IS` | `any` | そのまま JSON シリアライズ |

## クエリ結果の型

レスポンスは JSON またはバイナリプロトコル (v4) で返却され、DB 型に応じて Go の型に自動変換される。

| DB 型 | Go の型 |
|---|---|
| `BOOL`, `BOOLEAN` | `bool` |
| `INT`, `INT2`, `INT4`, `SMALLINT`, `TINYINT`, `MEDIUMINT` | `int32` |
| `BIGINT`, `INT8` | `int64` |
| `FLOAT`, `FLOAT4` | `float32` |
| `DOUBLE`, `FLOAT8` | `float64` |
| `NUMERIC`, `DECIMAL` | `decimal.Decimal` |
| `VARCHAR`, `CHAR`, `TEXT` 等 | `string` |
| `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `DATETIME` | `time.Time` |
| `BYTEA` | `[]byte` |
| その他 (`UUID`, `INTERVAL`, `BIT` 等) | `string`（そのまま） |

## トランザクション分離レベル

| 定数 | 値 |
|---|---|
| `Isolation_ReadUncommitted` | `READ_UNCOMMITTED` |
| `Isolation_ReadCommitted` | `READ_COMMITTED` |
| `Isolation_RepeatableRead` | `REPEATABLE_READ` |
| `Isolation_Serializable` | `SERIALIZABLE` |

## クラスタ・ノード管理

- 初期化時に `/healz` で全ノードのヘルスチェックを実行し、データソース情報を取得する
- リクエスト時は、DB 名とエンドポイント種別に基づいて重み付きランダムでノードを選択する
- ノード障害時は自動リトライ（最大 2 回）し、別ノードへフォールオーバーする
- サーバーからの `307 Temporary Redirect` に追従し、指定ノードへリクエストを転送する
- トランザクション操作は BEGIN 時に決定されたノードに固定される
- 同時実行数はセマフォで制御され、通常リクエスト 80% / トランザクション 20% で配分される

## エラーハンドリング

すべての操作は `*HaveError` を返す。`nil` でなければエラー。

```go
type HaveError struct {
    ErrCode string `json:"errcode"`
    Message string `json:"message"`
}
```

主なエラーコード:

| ErrCode | 説明 |
|---|---|
| `NETWORK_ERROR` | ネットワーク障害 |
| `NodeSelectionError` | 利用可能なノードなし |
| `RedirectCountExceeded` | リダイレクト回数超過 |

## ライセンス

Apache License 2.0
