package cmd

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"go-ObuZen/data"

	"github.com/spf13/cobra"
)

var (
	inputPath  string
	outputPath string
)

// check01Cmd は 'check01' サブコマンドを定義
var check01Cmd = &cobra.Command{
	Use:   "check01",
	Short: "同じ世帯番号と住定日で前住所が異なるレコードを抽出します。",
	RunE:  runCheck01,
}

func init() {
	rootCmd.AddCommand(check01Cmd)
	check01Cmd.Flags().StringVarP(&inputPath, "input", "i", "", "入力CSVファイルのパス (必須)")
	check01Cmd.MarkFlagRequired("input")
	check01Cmd.Flags().StringVarP(&outputPath, "output", "o", "output_check01.csv", "結果を出力するCSVファイルのパス")
}

// runCheck01 は I/O (ファイル) のセットアップとロジックの呼び出しを担当
func runCheck01(cmd *cobra.Command, args []string) error {
	fmt.Printf("✅ チェック01を開始します: %s -> %s\n", inputPath, outputPath)

	// --- パス 1: ファイルを開いてキーを特定 ---
	inputFile1, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("入力ファイル(パス1)を開けません: %w", err)
	}
	defer inputFile1.Close()

	duplicateAddresses, err := findDuplicateKeys(inputFile1)
	if err != nil {
		return fmt.Errorf("パス1 (キー特定) エラー: %w", err)
	}

	problemKeys := make(map[string]bool)
	for key, addresses := range duplicateAddresses {
		if len(addresses) > 1 {
			problemKeys[key] = true
		}
	}

	if len(problemKeys) == 0 {
		fmt.Println("ℹ️ チェック条件に該当するレコードは見つかりませんでした。")
		return nil
	}

	// --- パス 2: ファイルを再度開いて抽出と出力 ---
	inputFile2, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("入力ファイル(パス2)を開けません: %w", err)
	}
	defer inputFile2.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("出力ファイルを作成できません: %w", err)
	}
	defer outputFile.Close()

	count, err := extractAndWriteRecords(inputFile2, outputFile, problemKeys)
	if err != nil {
		return fmt.Errorf("パス2 (データ抽出) エラー: %w", err)
	}

	fmt.Printf("🎉 チェック01が完了しました。問題のあるキー %d 件に該当するレコード %d 件を %s に出力しました。\n", len(problemKeys), count, outputPath)
	return nil
}

// findDuplicateKeys はパス1の処理を行います。
func findDuplicateKeys(r io.Reader) (map[string]map[string]bool, error) {
	csvReader := csv.NewReader(r)
	csvReader.Comma = ','

	duplicateAddresses := make(map[string]map[string]bool)

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if errors.Is(err, csv.ErrFieldCount) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("CSV読み取りエラー: %w", err)
		}

		if len(row) < 4 {
			continue
		}

		key := row[1] + "_" + row[2] // [1]:世帯番号, [2]:住定日
		motoJusho := row[3]          // [3]:前住所

		if _, ok := duplicateAddresses[key]; !ok {
			duplicateAddresses[key] = make(map[string]bool)
		}
		duplicateAddresses[key][motoJusho] = true
	}
	return duplicateAddresses, nil
}

// extractAndWriteRecords はパス2の処理を行います。
func extractAndWriteRecords(in io.Reader, out io.Writer, problemKeys map[string]bool) (int, error) {
	csvReader := csv.NewReader(in)
	csvReader.Comma = ','

	csvWriter := csv.NewWriter(out)
	csvWriter.Comma = ','

	// 出力ファイルにはヘッダーを書き込む
	if err := csvWriter.Write(data.Header); err != nil {
		// Write は error を返すので、ここでチェック
		return 0, fmt.Errorf("出力ヘッダーの書き込みエラー: %w", err)
	}

	var recordsWritten int
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if errors.Is(err, csv.ErrFieldCount) {
			continue
		}

		if err != nil {
			return 0, fmt.Errorf("CSV読み取りエラー: %w", err)
		}

		if len(row) < 4 {
			continue
		}

		key := row[1] + "_" + row[2] // [1]:世帯番号, [2]:住定日

		if problemKeys[key] {
			if err := csvWriter.Write(row); err != nil {
				// Write は error を返すので、ここでチェック
				return recordsWritten, fmt.Errorf("CSV書き込みエラー: %w", err)
			}
			recordsWritten++
		}
	}

	// === 修正箇所 ===
	// 1. Flush() を呼び出す (戻り値はない)
	csvWriter.Flush()

	// 2. Flush() またはそれ以前の Write() で発生したエラーを Error() メソッドで確認する
	if err := csvWriter.Error(); err != nil {
		return recordsWritten, fmt.Errorf("CSV書き込みFlush/Errorエラー: %w", err)
	}

	return recordsWritten, nil
}
