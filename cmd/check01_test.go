package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"go-ObuZen/data"
)

// === TestFindDuplicateKeys ===
// (変更なし)
func TestFindDuplicateKeys(t *testing.T) {
	testCases := []struct {
		name     string
		inputCSV string
		wantKeys []string
		wantErr  bool
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
			name: "正常: 列が足りない (スキップされる)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B
A003,世帯1,2025-01-01
`,
			wantKeys: []string{"世帯1_2025-01-01"},
			wantErr:  false,
		},
		{
			name: "正常: フィールド数が不正 (ErrFieldCount, スキップされる)",
			inputCSV: `
A001,世帯1,2025-01-01,住所A
A002,世帯1,2025-01-01,住所B,"余分な列"
A003,世帯2,2025-01-01,住所C
`,
			wantKeys: []string{},
			wantErr:  false,
		},
		{
			name:     "異常系: CSVフォーマット不正 (クォート)",
			inputCSV: `A001,"世帯1,2025-01-01,住所A`,
			wantKeys: nil,
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(strings.TrimSpace(tc.inputCSV))
			duplicateAddresses, err := findDuplicateKeys(r)
			if (err != nil) != tc.wantErr {
				t.Fatalf("findDuplicateKeys() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
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

// === TestExtractAndWriteStream (ソートなし) ===
// (変更なし)
func TestExtractAndWriteStream(t *testing.T) {
	const inputCSV = `
A001,世帯1,2025-01-01,住所A
C001,世帯3,2025-02-02,東京
B001,世帯1,2025-01-01,住所B
`
	header := strings.Join(data.Header, ",") + "\n"
	problemKeys := map[string]bool{
		"世帯1_2025-01-01": true,
		"世帯3_2025-02-02": true,
	}

	wantOutput := header + `A001,世帯1,2025-01-01,住所A
C001,世帯3,2025-02-02,東京
B001,世帯1,2025-01-01,住所B
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
// (変更なし)
func TestExtractAndWriteSort(t *testing.T) {
	const inputCSV = `
C001,世帯3,2025-02-02,東京
B001,世帯1,2025-01-01,住所B
A001,世帯1,2025-01-01,住所A
D001,世帯2,2025-01-01,住所C
`
	header := strings.Join(data.Header, ",") + "\n"
	problemKeys := map[string]bool{
		"世帯1_2025-01-01": true,
		"世帯3_2025-02-02": true,
	}
	wantOutput := header + `A001,世帯1,2025-01-01,住所A
B001,世帯1,2025-01-01,住所B
C001,世帯3,2025-02-02,東京
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

// errorWriterMock は、N回成功した後に書き込みエラーを発生させる io.Writer
type errorWriterMock struct {
	failAfterN int // N回成功した後に失敗
	writes     int
}

func (w *errorWriterMock) Write(p []byte) (n int, err error) {
	if w.writes >= w.failAfterN {
		return 0, io.ErrShortWrite // 失敗
	}
	w.writes++
	return len(p), nil // 成功
}

// errorReader は Read 呼び出しで強制的にエラーを返す
type errorReader struct{}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestFindDuplicateKeys_ReadError(t *testing.T) {
	_, err := findDuplicateKeys(&errorReader{})
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
	// 1回目の書き込み (bufio が Header と Row を結合して Flush) で失敗
	out := &errorWriterMock{failAfterN: 0}
	r := strings.NewReader("A001,世帯1,2025-01-01,住所A")
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
	// (修正)
	// WriteAll がトリガーする Flush/Write が 1回目 であることを考慮し、
	// failAfterN: 0 に修正する。
	out := &errorWriterMock{failAfterN: 0}
	r := strings.NewReader("A001,世帯1,2025-01-01,住所A")
	problemKeys := map[string]bool{"世帯1_2025-01-01": true}

	_, err := extractAndWriteSort(r, out, problemKeys)
	if err == nil {
		t.Fatal("WriteAll (Flush/Error) エラーが返されるべきところで、nil が返されました")
	}
}
