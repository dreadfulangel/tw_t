package customerimporter

import (
	"strings"
	"testing"

	"bytes"

	"encoding/csv"

	"reflect"
)

func emptyOption() Option { return func(f *CustomerImporter) {} }

// test with reader
func TestImport(t *testing.T) {
	// set header of the csv file
	header := "first_name,last_name,email,gender,ip_address"

	// Test import with different inputs, options and results
	t.Log("Test import with different inputs, options and results")

	data := []struct {
		records []string
		option  Option
		err     error
		result  EmailsByDomainQtyList
	}{
		// working case without options
		{[]string{"Mildred,Hernandez,mhernandez@github.io,Female,38.194.51.128"},
			emptyOption(),
			nil,
			EmailsByDomainQtyList{{"github.io", 1}},
		},

		// working sorting case
		{[]string{
			"Mildred,Hernandez,email@b.io,Female,38.194.51.128",
			"Mildred,Hernandez,email@c.io,Female,38.194.51.128",
			"Mildred,Hernandez,email@d.io,Female,38.194.51.128",
			"Mildred,Hernandez,email@a.io,Female,38.194.51.128",
		},
			emptyOption(),
			nil,
			EmailsByDomainQtyList{
				{"a.io", 1},
				{"b.io", 1},
				{"c.io", 1},
				{"d.io", 1},
			},
		},

		// case with invalid email
		{[]string{"Mildred,Hernandez,mhernandezgithub.io,Female,38.194.51.128"},
			emptyOption(),
			ErrEmailIsNotValid,
			nil,
		},

		// case with empty email
		{[]string{"Mildred,Hernandez,,Female,38.194.51.128"},
			emptyOption(),
			ErrEmailIsNotValid,
			nil,
		},

		// case with invalid email but with SkipErrInvalidEmails option enabled
		{[]string{"Mildred,Hernandez,mhernandezgithub.io,Female,38.194.51.128"},
			SkipErrInvalidEmails(),
			ErrNoValidEmailsFound,
			nil,
		},

		// case with duplicate emails without options
		{[]string{"Mildred,Hernandez,mhernandez0@github.io,Female,38.194.51.128",
			"Mildred,Hernandez,mhernandez0@github.io,Female,38.194.51.128"},
			emptyOption(),
			ErrEmailDuplicate,
			nil,
		},

		// case with duplicate emails but with SkipErrDuplicateEmails option
		{[]string{"Mildred,Hernandez,mhernandez0@github.io,Female,38.194.51.128",
			"Mildred,Hernandez,mhernandez0@github.io,Female,38.194.51.128"},
			SkipErrDuplicateEmails(),
			nil,
			EmailsByDomainQtyList{{"github.io", 1}},
		},

		// case with wrong number of fields
		{[]string{"Mildred,Hernandez"},
			emptyOption(),
			csv.ErrFieldCount,
			nil,
		},

		// error should contain correct line and column
		{[]string{"Mildred,Hernandez,mhernandezgithub.io,Female,38.194.51.128"},
			emptyOption(),
			&csv.ParseError{2, 2, ErrEmailIsNotValid},
			nil,
		},
	}

	for testNumber, d := range data {
		t.Logf("Case: %v", testNumber)

		// put data to buffer
		b := bytes.NewBufferString(header + "\n")
		for i, r := range d.records {
			b.WriteString(r)
			if i < len(d.records)-1 {
				b.WriteString("\n")
			}
		}

		// import from buffer
		result, err := Import(b, "email", d.option)

		// check for correct error handling
		if err != nil && !strings.Contains(err.Error(), d.err.Error()) {
			t.Errorf("should raise error: %v, but got error %v ", d.err, err)
		}

		//check for correct results
		if result != nil && !reflect.DeepEqual(*result, d.result) {
			t.Errorf("should result with: %v, but got %v", *result, d.result)
		}
	}

	// test non existing email field
	t.Log("Test non existing email field")

	// put data to buffer
	b := bytes.NewBufferString(header + "\n")

	// import from buffer
	result, err := Import(b, "invalid field")

	// check for correct error handling
	if err != nil && !strings.Contains(err.Error(), ErrFieldNotExists.Error()) {
		t.Errorf("should raise error: %v, but got error %v ", err, ErrFieldNotExists)
	}
	// check for empty result
	if result != nil {
		t.Error("result should be empty")
	}
}

// test with files
func TestImportFromFile(t *testing.T) {
	// test with existing file
	t.Log("Test existing file")
	_, err := ImportFromFile(
		"./customers.csv",
		"email",
		SkipErrInvalidEmails(),
		SkipErrDuplicateEmails(),
	)
	if err != nil {
		t.Errorf("should pass the test")
	}

	// test with non existing file
	t.Log("Test non existing file")
	_, err = ImportFromFile(
		"./nonexisting.csv",
		"email",
		SkipErrInvalidEmails(),
		SkipErrDuplicateEmails(),
	)

	if !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("should raise the error")
	}
}
