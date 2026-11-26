package v1alpha1

import (
	"os"
	"regexp"
	"testing"
)

func TestValidateFlakeRefDir(t *testing.T) {
	rev := "0123456789abcdef0123456789abcdef01234567"
	cases := []struct {
		ref string
		ok  bool
	}{
		{"github:o/r/" + rev, true},
		{"github:o/r/" + rev + "?dir=tools/knight", true},
		{"github:o/r/" + rev + "?dir=a..b/c", true},
		{"github:o/r/" + rev + "?dir=...", true},
		{"git+https://h/r.git?rev=" + rev + "&dir=sub", true},
		{"github:o/r/" + rev + "?dir=../x", false},
		{"github:o/r/" + rev + "?dir=a/../../x", false},
		{"github:o/r/" + rev + "?dir=..", false},
		{"github:o/r/" + rev + "?dir=a/..", false},
		{"github:o/r/" + rev + "?dir=/etc", false},
		{"github:o/r/" + rev + "?dir=%2e%2e/x", false},
		{"github:o/r/" + rev + "?dir=a%2F..%2Fx", false},
		{"github:o/r/" + rev + "?dir=%2Fetc", false},
		{"github:o/r/" + rev + "?dir=a%5C..%5Cx", false},
		{"git+https://h/r.git?dir=../x&rev=" + rev, false},
		{"git+https://h/r.git?rev=" + rev + "&dir=../x", false},
		{"github:o/r/" + rev + "?dir=%zz", false},
	}
	for _, c := range cases {
		if err := ValidateFlakeRefDir(c.ref); (err == nil) != c.ok {
			t.Errorf("ValidateFlakeRefDir(%q) err=%v, want ok=%v", c.ref, err, c.ok)
		}
	}
}

// TestFlakeRefPatternMatchesDirValidator keeps the CRD pattern and the Go
// validator in agreement on dir handling.
func TestFlakeRefPatternMatchesDirValidator(t *testing.T) {
	src, err := os.ReadFile("knight_types.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile("(?m)Pattern=`(\\^\\(\\(github[^`]*)`").FindSubmatch(src)
	if m == nil {
		t.Fatal("flake ref pattern not found")
	}
	re := regexp.MustCompile(string(m[1]))
	rev := "0123456789abcdef0123456789abcdef01234567"
	for _, c := range []struct {
		ref string
		ok  bool
	}{
		{"github:o/r/" + rev, true},
		{"github:o/r/" + rev + "?dir=tools/knight", true},
		{"github:o/r/" + rev + "?dir=a..b/.hidden", true},
		{"github:o/r/" + rev + "?dir=sub/", true},
		{"git+https://h/r.git?ref=main&rev=" + rev, true},
		{"git+https://h/r.git?rev=" + rev + "&dir=sub/pkg", true},
		{"github:o/r/" + rev + "?dir=../x", false},
		{"github:o/r/" + rev + "?dir=a/../../x", false},
		{"github:o/r/" + rev + "?dir=..", false},
		{"github:o/r/" + rev + "?dir=a/..", false},
		{"github:o/r/" + rev + "?dir=/etc", false},
		{"github:o/r/" + rev + "?dir=%2e%2e/x", false},
		{"git+https://h/r.git?dir=../x&rev=" + rev, false},
		{"git+https://h/r.git?rev=" + rev + "&dir=a/../x", false},
		{"git+https://h/r.git?rev=" + rev + "&dir=/etc", false},
	} {
		if got := re.MatchString(c.ref); got != c.ok {
			t.Errorf("pattern match %q = %v, want %v", c.ref, got, c.ok)
		}
	}
}
