package cmd

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"go-ObuZen/data"

	"github.com/spf13/cobra"
)

var (
	inputPath  string
	outputPath string
	sortFlag   bool
)

// check01Cmd は 'check01' サブコマンドを定義
var check01Cmd = &cobra.Command{
	Use:   "check01",
	Short: "同じ世帯・住定日・住定届出日で前住所が異なるレコードを抽出します。",
	RunE:  runCheck01,
}

func init() {
	rootCmd.AddCommand(check01Cmd)

	// フラグ定義
	check01Cmd.Flags().StringVarP(&inputPath, "input", "i", "", "入力CSVファイルのパス (必須)")
	check01Cmd.MarkFlagRequired("input")
	check01Cmd.Flags().StringVarP(&outputPath, "output", "o", "output_check01.csv", "結果を出力するCSVファイルのパス")
	check01Cmd.Flags().BoolVar(&sortFlag, "sort", false, "出力を 世帯番号, 宛名番号 の順でソートします (メモリを消費します)")
}

// runCheck01 は I/O とロジックの呼び出しを担当
func runCheck01(cmd *cobra.Command, args []string) error {
	fmt.Printf("チェック01を開始します: %s -> %s\n", inputPath, outputPath)
	if sortFlag {
		fmt.Println("--sort オプションが有効です。出力対象の全データをメモリに読み込みます。")
	}

	// --- パス 1: キーの特定 ---
	inputFile1, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("入力ファイル(パス1)を開けません: %w", err)
	}
	defer inputFile1.Close()

	problemKeys, err := findProblemGroups(inputFile1)
	if err != nil {
		return fmt.Errorf("パス1 (キー特定) エラー: %w", err)
	}

	if len(problemKeys) == 0 {
		fmt.Println("チェック条件に該当するレコードは見つかりませんでした。")
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

	var count int
	if sortFlag {
		// ソートする
		count, err = extractAndWriteSort(inputFile2, outputFile, problemKeys)
	} else {
		// ソートしない
		count, err = extractAndWriteStream(inputFile2, outputFile, problemKeys)
	}

	if err != nil {
		return fmt.Errorf("パス2 (データ抽出) エラー: %w", err)
	}

	fmt.Printf("🎉 チェック01が完了しました。問題のあるキー %d 件に該当するレコード %d 件を %s に出力しました。\n", len(problemKeys), count, outputPath)
	return nil
}

// グループごとの情報を集約する内部構造体
type groupInfo struct {
	// (修正) key: 住定届出日, value: (set of 前住所)
	addressesPerDate map[string]map[string]bool
	allExcluded      bool
}

// findProblemGroups はパス1の処理を行います
func findProblemGroups(r io.Reader) (map[string]bool, error) {
	csvReader := csv.NewReader(r)
	csvReader.Comma = ','

	// key: 世帯番号 + "_" + 住定日, value: groupInfo
	groupMap := make(map[string]*groupInfo)

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

		// 新レイアウト: 0:宛名, 1:世帯, 2:住定日, 3:住定届出日, 4:前住所, 5:出力有無
		if len(row) < 6 {
			continue
		}
		setaiBango := row[1]
		juteiHi := row[2]
		juteiTodokeHi := row[3] // 新しい列
		motoJusho := row[4]
		outputFlag := row[5]

		key := setaiBango + "_" + juteiHi // グループ化キーは変更なし

		info, ok := groupMap[key]
		if !ok {
			// このグループの最初のレコード
			info = &groupInfo{
				addressesPerDate: make(map[string]map[string]bool),
				allExcluded:      true,
			}
			groupMap[key] = info
		}

		// (修正) 住定届出日ごとに、前住所のセットを記録
		if _, ok := info.addressesPerDate[juteiTodokeHi]; !ok {
			info.addressesPerDate[juteiTodokeHi] = make(map[string]bool)
		}
		info.addressesPerDate[juteiTodokeHi][motoJusho] = true

		// 1件でも「出力対象外」で *ない* ものがあれば、フラグをfalseにする
		if outputFlag != "出力対象外" {
			info.allExcluded = false
		}
	}

	// パス1の集計結果から、問題のあるグループキー (map[string]bool) を作成
	problemGroups := make(map[string]bool)
	for key, info := range groupMap {
		// グループ全員が「出力対象外」の場合はスキップ
		if info.allExcluded {
			continue
		}

		// このグループ内の「住定届出日」ごとに前住所のバリエーションをチェック
		isProblem := false
		for _, addresses := range info.addressesPerDate {
			// (解釈) 同じ住定届出日 (addresses) の中で、前住所が2種類以上あるか
			if len(addresses) > 1 {
				isProblem = true
				break // このグループは問題ありと確定
			}
		}

		if isProblem {
			problemGroups[key] = true
		}
	}

	return problemGroups, nil
}

// extractAndWriteStream は、--sort がない場合のデフォルトの動作 (メモリ効率優先)
func extractAndWriteStream(in io.Reader, out io.Writer, problemKeys map[string]bool) (int, error) {
	csvReader := csv.NewReader(in)
	csvReader.Comma = ','
	csvWriter := csv.NewWriter(out)
	csvWriter.Comma = ','

	// 出力ヘッダーも新しいレイアウトに合わせる (data.Headerを変更)
	if err := csvWriter.Write(data.Header); err != nil {
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

		if len(row) < 6 {
			continue
		}
		key := row[1] + "_" + row[2] // キーは複合キー

		if problemKeys[key] {
			if err := csvWriter.Write(row); err != nil {
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

		if len(row) < 6 {
			continue
		}
		key := row[1] + "_" + row[2] // キーは複合キー

		if problemKeys[key] {
			rowCopy := make([]string, len(row))
			copy(rowCopy, row)
			results = append(results, rowCopy)
		}
	}

	// ソート処理 (世帯番号[1] -> 宛名番号[0]) (変更なし)
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
