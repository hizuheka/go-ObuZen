package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// (check01.go などで定義される check01Input, check01Output などの
// フラグ変数は、cobraの慣習に従い、
// このファイルではなく各サブコマンドの .go ファイル内で定義することを推奨します。)

// rootCmd はアプリケーションのルートコマンドです。
var rootCmd = &cobra.Command{
	Use:   "go-ObuZen", // (パッケージ名に修正)
	Short: "データチェックプログラム",
	Long: `データチェックプログラム
複数のチェック項目をサブコマンドとして実行します。
(例: go-ObuZen check01 -i input.csv)`,
}

// Execute はルートコマンドを実行します。
// main.go からバージョン文字列を受け取るように変更します。
func Execute(version string) {
	// main から渡されたバージョンを cobra の rootCmd に設定
	rootCmd.Version = version

	// (オプション) バージョン表示のテンプレートをカスタマイズ
	// rootCmd.SetVersionTemplate(fmt.Sprintf("go-ObuZen version %s\n", version))

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// (init() は変更なし)
}
