# custom-simple-salesforce
`custom-simple-salesforce`は、Salesforce Bulk APIを扱うためのPythonライブラリです。

`simple-salesforce`をベースに、認証プロセスを簡素化し、Bulk APIの操作をより扱いやすくするための機能を提供します。

## インストール

このライブラリは、`uv`または`pip`でインストールできます。

```bash
uv pip install git+https://github.com/seiji-kawaharajo/custom-simple-salesforce.git
# または
pip install git+https://github.com/seiji-kawaharajo/custom-simple-salesforce.git
```

## 使い方

### 1.接続

ライブラリの`Sf`クラスを使って、YamlやDictからSalesforceに接続出来ます。パスワード認証とクライアント認証に対応しています。

または、simple-salesforceのsalesforceを継承しているので、同じ用にパラメータを渡せば使用することが出来ます

`config.yaml`
```yml
auth_method: password
username: your_username@example.com
password: your_password
security_token: your_security_token
domain: login
```

`connect.py`
```py
import yaml
from custom_simple_salesforce import Sf

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

sf_connection = Sf.connection(config)
print("接続成功！")
```

### 2.bulk queryの実行例

`bulk`モジュールを使って、大規模なデータを効率的にクエリできます。結果は辞書のリストとして返されます。

```py
from custom_simple_salesforce import Sf, SfBulk

# 認証済みSfオブジェクトを取得（上記参照）
sf_connection = ...

# SfBulkクライアントを作成
bulk_client = SfBulk(sf_connection)

# バルククエリジョブを作成し、実行
query_job = bulk_client.create_job_query(
    "SELECT Id, Name FROM Account"
)

# ジョブの完了を待機
query_job.poll_status()

# 結果を辞書のリストとして取得
results = query_job.get_results()

# 取得したデータを表示
for record in results[:3]: # 先頭3件を表示
    print(record)
```

### 3.Bulk Insertの実行例

大規模なデータを一括で作成（Insert）する例です。

`records.csv`

```
Name,Industry
Test Account 1,Technology
Test Account 2,Finance
```

```py
from custom_simple_salesforce import Sf, SfBulk

# 認証済みSfオブジェクトを取得（上記参照）
sf_connection = ...

# SfBulkクライアントを作成
bulk_client = SfBulk(sf_connection)

# CSVファイルを読み込み、文字列として取得
with open("records.csv", "r") as f:
    csv_data = f.read()

# Insertジョブを作成
insert_job = bulk_client.create_job_insert("Account")

# データをアップロード
insert_job.upload_data(csv_data)

# ジョブの完了を待機
insert_job.poll_status()

# 成功したレコードの結果を取得
successful_results = insert_job.get_successful_results()
print("成功レコード:", successful_results)
```