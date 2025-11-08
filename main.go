package main

import (
	"go-ObuZen/cmd" // (パッケージ名を go-ObuZen に修正)
)

// version はビルド時に ldflags によって設定されます
// go build -ldflags="-X 'main.version=1.0.0'"
var version = "dev" // 値が設定されなかった場合のデフォルト値

func main() {
	// cmd パッケージの Execute 関数にバージョン情報を渡す
	cmd.Execute(version)
}
