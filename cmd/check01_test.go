package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"go-ObuZen/data" // (修正) パッケージパス
)

// === (修正) TestFindProblemGroups ===
func TestFindProblemGroups(t *testing.T) {
	// (修正) 6列のテストケース (0:宛名, 1:世帯, 2:住定日, 3:住定届出日, 4:前住所, 5:出力有無)
	testCases := []struct {
		name     string   // テストケース名
		inputCSV string   // 入力CSVデータ (ヘッダーなし)
		wantKeys []string // 期待する問題キー (世帯番号_住定日)
		wantErr  bool     // エラーを期待するか
	}{
		{
			name: "正常: 問題あり (同一届出日で前住所不一致)",
			inputCSV: `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
A002,世帯1,2025-01-01,2025-01-10,住所B,出力対象
`,
			wantKeys: []string{"世帯1_2025-01-01"},
			wantErr:  false,
		},
		{
			name: "正常: 問題なし (異なる届出日で前住所不一致)",
			inputCSV: `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
A002,世帯1,2025-01-01,2025-01-11,住所B,出力対象
`,
			wantKeys: []string{}, // 届出日が異なるためOK
			wantErr:  false,
		},
		{
			name: "正常: 問題あり (複数の届出日が混在し、片方が不一致)",
			inputCSV: `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
A002,世帯1,2025-01-01,2025-01-10,住所B,出力対象
A003,世帯1,2025-01-01,2025-01-11,住所C,出力対象
`,
			wantKeys: []string{"世帯1_2025-01-01"}, // 01-10 のグループがNG
			wantErr:  false,
		},
		{
			name: "除外: 問題ありだが、全て出力対象外",
			inputCSV: `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象外
A002,世帯1,2025-01-01,2025-01-10,住所B,出力対象外
`,
			wantKeys: []string{}, // グループ自体が除外
			wantErr:  false,
		},
		{
			name: "正常: 問題あり (一部出力対象外)",
			inputCSV: `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
A002,世帯1,2025-01-01,2025-01-10,住所B,出力対象外
`,
			wantKeys: []string{"世帯1_2025-01-01"}, // グループはチェック対象
			wantErr:  false,
		},
		{
			name: "正常: 問題なし (同一届出日で前住所一致)",
			inputCSV: `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
A002,世帯1,2025-01-01,2025-01-10,住所A,出力対象
`,
			wantKeys: []string{},
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(strings.TrimSpace(tc.inputCSV))

			problemKeys, err := findProblemGroups(r)

			if (err != nil) != tc.wantErr {
				t.Fatalf("findProblemGroups() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
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

// === TestExtractAndWriteStream (ソートなし) ===
func TestExtractAndWriteStream(t *testing.T) {
	// (修正) 6列のCSV
	const inputCSV = `
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
C001,世帯3,2025-02-02,2025-02-10,東京,出力対象
B001,世帯1,2025-01-01,2025-01-10,住所B,出力対象外
`
	// (修正) data.Header は data/record.go の6列定義を参照
	header := strings.Join(data.Header, ",") + "\n"
	problemKeys := map[string]bool{
		"世帯1_2025-01-01": true,
		"世帯3_2025-02-02": true,
	}

	// (修正) 6列の出力
	wantOutput := header + `A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
C001,世帯3,2025-02-02,2025-02-10,東京,出力対象
B001,世帯1,2025-01-01,2025-01-10,住所B,出力対象外
`
	r := strings.NewReader(strings.TrimSpace(inputCSV))
	out := new(bytes.Buffer)

	count, err := extractAndWriteStream(r, out, problemKeys)
	if err != nil {
		t.Fatalf("extractAndWriteStream() error = %v", err)
	}
	if count != 3 {
		t.Errorf("書き込み件数が異なります。 got = %d, want = 3", count)
	}
	if gotOutput := strings.TrimSpace(out.String()); gotOutput != strings.TrimSpace(wantOutput) {
		t.Errorf("出力内容が異なります。\n--- GOT ---\n%s\n--- WANT ---\n%s", gotOutput, wantOutput)
	}
}

// === TestExtractAndWriteSort (ソートあり) ===
func TestExtractAndWriteSort(t *testing.T) {
	// (修正) 6列のCSV
	const inputCSV = `
C001,世帯3,2025-02-02,2025-02-10,東京,出力対象
B001,世帯1,2025-01-01,2025-01-10,住所B,出力対象外
A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
D001,世帯2,2025-01-01,2025-01-10,住所C,出力対象
`
	header := strings.Join(data.Header, ",") + "\n"
	problemKeys := map[string]bool{
		"世帯1_2025-01-01": true,
		"世帯3_2025-02-02": true,
	}
	// (修正) 6列の出力 (ソート順は変わらず)
	wantOutput := header + `A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象
B001,世帯1,2025-01-01,2025-01-10,住所B,出力対象外
C001,世帯3,2025-02-02,2025-02-10,東京,出力対象
`
	r := strings.NewReader(strings.TrimSpace(inputCSV))
	out := new(bytes.Buffer)

	count, err := extractAndWriteSort(r, out, problemKeys)
	if err != nil {
		t.Fatalf("extractAndWriteSort() error = %v", err)
	}
	if count != 3 {
		t.Errorf("書き込み件数が異なります。 got = %d, want = 3", count)
	}
	if gotOutput := strings.TrimSpace(out.String()); gotOutput != strings.TrimSpace(wantOutput) {
		t.Errorf("出力内容が異なります。\n--- GOT ---\n%s\n--- WANT ---\n%s", gotOutput, wantOutput)
	}
}

// === I/Oエラーのカバレッジテスト ===

// (errorWriterMock, errorReader は変更なし)
type errorWriterMock struct {
	failAfterN int
	writes     int
}

func (w *errorWriterMock) Write(p []byte) (n int, err error) {
	if w.writes >= w.failAfterN {
		return 0, io.ErrShortWrite
	}
	w.writes++
	return len(p), nil
}

type errorReader struct{}

func (r *errorReader) Read(p []byte) (n int, err error) { return 0, io.ErrUnexpectedEOF }

func TestFindProblemGroups_ReadError(t *testing.T) { // (修正) 関数名
	_, err := findProblemGroups(&errorReader{})
	if err == nil {
		t.Fatal("エラーが返されるべきところで、nil が返されました")
	}
}

// --- extractAndWriteStream のエラーテスト ---

func TestExtractAndWriteStream_ReadError(t *testing.T) {
	_, err := extractAndWriteStream(&errorReader{}, new(bytes.Buffer), map[string]bool{})
	if err == nil {
		t.Fatal("エラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteStream_HeaderWriteError(t *testing.T) {
	out := &errorWriterMock{failAfterN: 0}
	_, err := extractAndWriteStream(strings.NewReader(""), out, map[string]bool{})
	if err == nil {
		t.Fatal("ヘッダー書き込みエラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteStream_RowWriteError(t *testing.T) {
	out := &errorWriterMock{failAfterN: 0}
	// (修正) 6列の入力
	r := strings.NewReader("A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象")
	problemKeys := map[string]bool{"世帯1_2025-01-01": true}

	_, err := extractAndWriteStream(r, out, problemKeys)
	if err == nil {
		t.Fatal("データ行書き込み(Flush)エラーが返されるべきところで、nil が返されました")
	}
}

// --- extractAndWriteSort のエラーテスト ---

func TestExtractAndWriteSort_ReadError(t *testing.T) {
	_, err := extractAndWriteSort(&errorReader{}, new(bytes.Buffer), map[string]bool{})
	if err == nil {
		t.Fatal("エラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteSort_HeaderWriteError(t *testing.T) {
	out := &errorWriterMock{failAfterN: 0}
	_, err := extractAndWriteSort(strings.NewReader(""), out, map[string]bool{})
	if err == nil {
		t.Fatal("ヘッダー書き込みエラーが返されるべきところで、nil が返されました")
	}
}

func TestExtractAndWriteSort_WriteAllError(t *testing.T) {
	out := &errorWriterMock{failAfterN: 0}
	// (修正) 6列の入力
	r := strings.NewReader("A001,世帯1,2025-01-01,2025-01-10,住所A,出力対象")
	problemKeys := map[string]bool{"世帯1_2025-01-01": true}

	_, err := extractAndWriteSort(r, out, problemKeys)
	if err == nil {
		t.Fatal("WriteAll (Flush/Error) エラーが返されるべきところで、nil が返されました")
	}
}
