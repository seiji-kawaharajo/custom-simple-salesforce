#!/bin/bash

# スクリプトの実行に失敗した場合、すぐに終了
set -e

# --- 変数設定 ---
# ビルドディレクトリ
DIST_DIR="dist"
# 最終的なZIPファイル名
ZIP_FILE="my-awesome-layer.zip"
# Lambdaレイヤーの内部構造ディレクトリ
LAYER_BUILD_DIR="$DIST_DIR/lambda_layer/python"
# requirements.txtのパス
REQUIREMENTS_FILE="$DIST_DIR/requirements.txt"

echo "1. パッケージのビルドと依存関係の準備を開始します..."

# 既存のビルドディレクトリを削除し、再作成
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# 1. pyproject.tomlからrequirements.txtを作成
echo "  - requirements.txtを生成中..."
uv pip compile pyproject.toml -o "$REQUIREMENTS_FILE"

# 2. パッケージの依存関係をインストール
echo "  - 依存関係をインストール中..."
uv pip install -r "$REQUIREMENTS_FILE" --target "$LAYER_BUILD_DIR"

# 3. 独自パッケージの配置
echo "  - 独自パッケージを配置中..."
uv pip install . --target "$LAYER_BUILD_DIR"

# --- 2. ZIPファイルを作成 ---
echo "2. レイヤー用ZIPファイルを作成しています..."
# dist/lambda_layer の中身をZIP化し、distディレクトリの直下に配置
# ここでZIPファイルの出力先を修正
cd "$DIST_DIR/lambda_layer" # 階層を一つ上に移動
zip -r "../$ZIP_FILE" .
cd ../.. # プロジェクトルートに戻る

# --- 3. 後処理（一時ファイルの削除） ---
echo "3. 一時ビルドディレクトリをクリーンアップしています..."
# distディレクトリ内の、最終的なZIPファイル以外の不要なファイルを削除
find "$DIST_DIR" -mindepth 1 -not -name "$ZIP_FILE" -delete

echo "✅ 完了: レイヤーのZIPファイルが dist/ ディレクトリに作成されました。"