package cmd

import (
	"bytes" // bytes.Buffer (メモリ上の io.Writer)
	// エラー比較用
	"io"      // io.EOF, io.Reader, io.Writer
	"strings" // strings.Reader (メモリ上の io.Reader)
	"testing" // テストフレームワーク

	"go-ObuZen/data" // data.Header を参照するため
)

// === TestFindDuplicateKeys ===

func TestFindDuplicateKeys(t *testing.T) {
	// テストケースを定義
	testCases := []struct {
		name     string   // テストケース名
		inputCSV string   // 入力CSVデータ (ヘッダーなし)
		wantKeys []string // 期待する問題キー (世帯番号_住定日)
		wantErr  bool     // エラーを期待するか
	}{
		{
			name: "正常: 1件の重複 (世帯1)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B
A003,世帯2,2025-01-01,住所C
A004,世帯2,2025-01-01,住所C
`,
			wantKeys: []string{"世帯1_2025-01-01"},
			wantErr:  false,
		},
		{
			name: "正常: 2件の重複 (世帯1, 世帯3)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B
A003,世帯2,2025-01-01,住所C
A005,世帯3,2025-02-02,東京
A006,世帯3,2025-02-02,大阪
`,
			wantKeys: []string{"世帯1_2025-01-01", "世帯3_2025-02-02"},
			wantErr:  false,
		},
		{
			name: "正常: 重複なし (日付違い)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-02,住所B
`,
			wantKeys: []string{},
			wantErr:  false,
		},
		{
			name:     "正常: データが空",
			inputCSV: ``,
			wantKeys: []string{},
			wantErr:  false,
		},
		{
			name: "正常: 列が足りない (スキップされる)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B
A003,世帯1,2025-01-01
`,
			wantKeys: []string{"世帯1_2025-01-01"}, // A003は無視される
			wantErr:  false,
		},
		{
			name: "正常: フィールド数が不正 (ErrFieldCount, スキップされる)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B,"余分な列"
A003,世帯2,2025-01-01,住所C
`,
			// csv.Reader は最初の行のフィールド数を基準にするため、
			// 2行目が ErrFieldCount になる (check01の実装ではスキップ)
			wantKeys: []string{}, // 1行目しか処理されない (2行目でエラーになるため)
			wantErr:  false,      // スキップするのでエラーにはしない
		},
		{
			name:     "異常系: CSVフォーマット不正 (クォート)",
			inputCSV: `A001,"世帯1,2025-01-01,住所A`, // 閉じクォートがない
			wantKeys: nil,
			wantErr:  true, // csv.Reader がエラーを返す
		},
	}

	for _, tc := range testCases {
		// t.Run でサブテストを実行
		t.Run(tc.name, func(t *testing.T) {
			// TrimSpace で前後の不要な改行を除去
			r := strings.NewReader(strings.TrimSpace(tc.inputCSV))

			duplicateAddresses, err := findDuplicateKeys(r)

			if (err != nil) != tc.wantErr {
				t.Fatalf("findDuplicateKeys() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return // エラーを期待していた場合はここで終了
			}

			// 問題キーを抽出
			problemKeys := make(map[string]bool)
			for key, addresses := range duplicateAddresses {
				if len(addresses) > 1 {
					problemKeys[key] = true
				}
			}

			if len(problemKeys) != len(tc.wantKeys) {
				t.Errorf("キーの数が異なります。 got = %v, wantKeys = %v", problemKeys, tc.wantKeys)
			}
			for _, wantKey := range tc.wantKeys {
				if !problemKeys[wantKey] {
					t.Errorf("期待するキーが見つかりません: %s", wantKey)
				}
			}
		})
	}
}

// === TestExtractAndWriteRecords ===

func TestExtractAndWriteRecords(t *testing.T) {
	// 共通のテストデータ
	const inputCSV = `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B
A003,世帯2,2025-01-01,住所C
A004,世帯1,2025-01-02,住所A
A005,世帯3,2025-02-02,東京
A006,世帯3,2025-02-02,大阪
`
	// 期待されるヘッダー (data.Header が []string{"宛名番号", "世帯番号", "住定日", "前住所"} の場合)
	header := strings.Join(data.Header, ",") + "\n"

	testCases := []struct {
		name        string
		problemKeys map[string]bool // パス1で見つかったと仮定するキー
		wantOutput  string          // 期待するCSV出力 (ヘッダー含む)
		wantCount   int             // 期待する書き込み件数
		wantErr     bool
	}{
		{
			name: "正常: 世帯1と世帯3が該当",
			problemKeys: map[string]bool{
				"世帯1_2025-01-01": true,
				"世帯3_2025-02-02": true,
			},
			wantOutput: header + `A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B
A005,世帯3,2025-02-02,東京
A006,世帯3,2025-02-02,大阪
`,
			wantCount: 4,
			wantErr:   false,
		},
		{
			name:        "正常: 該当なし (problemKeysが空)",
			problemKeys: map[string]bool{},
			wantOutput:  header, // ヘッダーのみ
			wantCount:   0,
			wantErr:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(strings.TrimSpace(inputCSV))
			out := new(bytes.Buffer) // メモリ上の Writer

			count, err := extractAndWriteRecords(r, out, tc.problemKeys)

			if (err != nil) != tc.wantErr {
				t.Fatalf("extractAndWriteRecords() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}

			if count != tc.wantCount {
				t.Errorf("書き込み件数が異なります。 got = %d, want = %d", count, tc.wantCount)
			}

			// 出力内容チェック (改行コードの違いを吸収するため TrimSpace を使用)
			gotOutput := strings.TrimSpace(out.String())
			wantOutput := strings.TrimSpace(tc.wantOutput)

			if gotOutput != wantOutput {
				t.Errorf("出力内容が異なります。\n--- GOT ---\n%s\n--- WANT ---\n%s", gotOutput, wantOutput)
			}
		})
	}
}

// === I/Oエラーのカバレッジテスト ===
// (カバレッジ100%を目指すためのモック)

// errorReader は Read 呼び出しで強制的にエラーを返す io.Reader
type errorReader struct{}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF // 何らかのI/Oエラー
}

// errorWriter は Write 呼び出しで強制的にエラーを返す io.Writer
type errorWriter struct{}

func (w *errorWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrShortWrite // ディスクフルなどのI/Oエラー
}

func TestFindDuplicateKeys_ReadError(t *testing.T) {
	// 読み取りの途中でエラーが発生するケース
	_, err := findDuplicateKeys(&errorReader{})
	if err == nil {
		t.Fatal("エラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteRecords_ReadError(t *testing.T) {
	// 読み取りの途中でエラーが発生するケース
	_, err := extractAndWriteRecords(&errorReader{}, new(bytes.Buffer), map[string]bool{})
	if err == nil {
		t.Fatal("エラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteRecords_HeaderWriteError(t *testing.T) {
	// ヘッダー書き込みでエラーが発生するケース
	_, err := extractAndWriteRecords(strings.NewReader(""), &errorWriter{}, map[string]bool{})
	if err == nil {
		t.Fatal("ヘッダー書き込みエラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteRecords_RowWriteError(t *testing.T) {
	// データ行の書き込みでエラーが発生するケース
	problemKeys := map[string]bool{"世帯1_2025-01-01": true}
	r := strings.NewReader("A001,世帯1,2025-01-01,住所A")

	_, err := extractAndWriteRecords(r, &errorWriter{}, problemKeys)
	if err == nil {
		t.Fatal("データ行書き込みエラーが返されるべきところで、nil が返されました")
	}
}
