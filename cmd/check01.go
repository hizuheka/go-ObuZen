package cmd

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort" // --sort のためにインポート

	"go-ObuZen/data"

	"github.com/spf13/cobra"
)

var (
	inputPath  string
	outputPath string
	// check01 専用の --sort フラグ用変数
	sortFlag bool
)

// check01Cmd は 'check01' サブコマンドを定義
var check01Cmd = &cobra.Command{
	Use:   "check01",
	Short: "同じ世帯番号と住定日で前住所が異なるレコードを抽出します。",
	RunE:  runCheck01,
}

func init() {
	rootCmd.AddCommand(check01Cmd)

	// フラグ定義
	check01Cmd.Flags().StringVarP(&inputPath, "input", "i", "", "入力CSVファイルのパス (必須)")
	check01Cmd.MarkFlagRequired("input")
	check01Cmd.Flags().StringVarP(&outputPath, "output", "o", "output_check01.csv", "結果を出力するCSVファイルのパス")

	// --sort フラグを追加
	check01Cmd.Flags().BoolVar(&sortFlag, "sort", false, "出力を 世帯番号, 宛名番号 の順でソートします (メモリを消費します)")
}

// runCheck01 は I/O とロジックの呼び出しを担当
func runCheck01(cmd *cobra.Command, args []string) error {
	fmt.Printf("✅ チェック01を開始します: %s -> %s\n", inputPath, outputPath)
	if sortFlag {
		fmt.Println("⚠️ --sort オプションが有効です。出力対象の全データをメモリに読み込みます。")
	}

	// --- パス 1: キーの特定 ---
	inputFile1, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("入力ファイル(パス1)を開けません: %w", err)
	}
	defer inputFile1.Close()

	// (修正) 1. 変数名を duplicateAddresses に戻す
	// (findDuplicateKeys は map[string]map[string]bool を返す)
	duplicateAddresses, err := findDuplicateKeys(inputFile1)
	if err != nil {
		return fmt.Errorf("パス1 (キー特定) エラー: %w", err)
	}

	// (修正) 2. map[string]bool への変換ロジックをここに追加
	problemKeys := make(map[string]bool)
	for key, addresses := range duplicateAddresses {
		if len(addresses) > 1 {
			problemKeys[key] = true
		}
	}

	// (修正) 3. problemKeys (map[string]bool) の件数で判定
	if len(problemKeys) == 0 {
		fmt.Println("ℹ️ チェック条件に該当するレコードは見つかりませんでした。")
		return nil
	}

	// --- パス 2: データ抽出 ---
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

	// (修正) 4. 正しい型 (map[string]bool) の problemKeys を渡す
	var count int
	if sortFlag {
		// ソートする (メモリ消費)
		count, err = extractAndWriteSort(inputFile2, outputFile, problemKeys)
	} else {
		// ソートしない (メモリ効率優先)
		count, err = extractAndWriteStream(inputFile2, outputFile, problemKeys)
	}

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

// extractAndWriteStream は、--sort がない場合のデフォルトの動作 (メモリ効率優先)
func extractAndWriteStream(in io.Reader, out io.Writer, problemKeys map[string]bool) (int, error) {
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

		key := row[1] + "_" + row[2]

		if problemKeys[key] {
			if err := csvWriter.Write(row); err != nil {
				// Write は error を返すので、ここでチェック
				return recordsWritten, fmt.Errorf("CSV書き込みエラー: %w", err)
			}
			recordsWritten++
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return recordsWritten, fmt.Errorf("CSV書き込みFlush/Errorエラー: %w", err)
	}

	return recordsWritten, nil
}

// extractAndWriteSort は、--sort が指定された場合の動作 (ソート優先)
func extractAndWriteSort(in io.Reader, out io.Writer, problemKeys map[string]bool) (int, error) {
	csvReader := csv.NewReader(in)
	csvReader.Comma = ','

	var results [][]string

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

		key := row[1] + "_" + row[2]

		if problemKeys[key] {
			rowCopy := make([]string, len(row))
			copy(rowCopy, row)
			results = append(results, rowCopy)
		}
	}

	// ソート処理 (世帯番号[1] -> 宛名番号[0])
	sort.Slice(results, func(i, j int) bool {
		if results[i][1] != results[j][1] {
			return results[i][1] < results[j][1]
		}
		return results[i][0] < results[j][0]
	})

	// 一括書き出し処理
	csvWriter := csv.NewWriter(out)
	csvWriter.Comma = ','

	if err := csvWriter.Write(data.Header); err != nil {
		return 0, fmt.Errorf("出力ヘッダーの書き込みエラー: %w", err)
	}

	if err := csvWriter.WriteAll(results); err != nil {
		return len(results), fmt.Errorf("CSV一括書き込み/Flushエラー: %w", err)
	}

	return len(results), nil
}
