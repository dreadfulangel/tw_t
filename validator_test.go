package customerimporter

import (
	"testing"
)

func TestIsValidEmail(t *testing.T) {
	data := []struct {
		email   string
		isEmail bool
	}{
		{"email@example.com", true},
		{"emailexample.com", false},
	}

	for testNumber, d := range data {
		isEmail := IsValidEmail(d.email)
		if isEmail != d.isEmail {
			t.Fatalf("error%v", testNumber)
		}
	}
}
