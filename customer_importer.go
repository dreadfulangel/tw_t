package customerimporter

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

var (
	ErrFieldNotExists     = errors.New("CSV header doesn't contain field")
	ErrEmailIsNotValid    = errors.New("Email is not valid")
	ErrEmailDuplicate     = errors.New("Email already added")
	ErrEmptyFile          = errors.New("File is empty")
	ErrNoValidEmailsFound = errors.New("No valid emails found")
)

// Option sets an option of the customer importer
type Option func(f *CustomerImporter)

// Don't raise error if email is already counted, just skip it.
func SkipErrDuplicateEmails() Option { return func(f *CustomerImporter) { f.skipErrDupEmails = true } }

// Don't raise error if email is invalid, just skip it.
func SkipErrInvalidEmails() Option { return func(f *CustomerImporter) { f.skipErrInvalidEmails = true } }

// EmailsByDomainQtyList data structure is used to return data
type EmailsByDomainQtyList []EmailsByDomainQty

type EmailsByDomainQty struct {
	Domain      string // domain name
	EmailsCount int    // amount of emails counted
}

// EmailsByDomainQtyList sorting methods
func (p EmailsByDomainQtyList) Len() int           { return len(p) }
func (p EmailsByDomainQtyList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p EmailsByDomainQtyList) Less(i, j int) bool { return p[i].Domain < p[j].Domain }

// CustomerImporter stores data to operate with csv file
type CustomerImporter struct {
	emailFieldName   string          // name of the email field
	emailColumnIndex int             // index of the email column
	domainCounter    map[string]int  // used internally for fast increments
	countedEmails    map[string]bool // used to catch duplicates
	line             int             // used to keep track of the processing line
	reader           *csv.Reader     // csv reader

	// options
	skipErrDupEmails     bool // don't raise error if email is already counted
	skipErrInvalidEmails bool // don't raise error if email is invalid
}

// imports from the file and returns EmailsByDomainQtyList
func ImportFromFile(fileName string, emailFieldName string, options ...Option) (*EmailsByDomainQtyList, error) {
	// open file
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// import and get result
	result, err := Import(file, emailFieldName, options...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// imports from reader
func Import(r io.Reader, emailFieldName string, options ...Option) (*EmailsByDomainQtyList, error) {
	// initialize csv reader
	reader := csv.NewReader(r)

	// initialize CustomerImporter
	c := CustomerImporter{reader: reader, emailFieldName: emailFieldName}

	// initialize maps
	c.domainCounter = make(map[string]int, 10)
	c.countedEmails = make(map[string]bool, 10)

	// set options
	for _, option := range options {
		option(&c)
	}

	// parse records
	if err := c.parse(); err != nil {
		return nil, err
	}

	// get result
	result, err := c.getResult()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// parses csv and updates counter
func (c *CustomerImporter) parse() error {
	for {
		// increment line
		c.line++

		// read record
		record, err := c.reader.Read()

		// handle end of file
		if err == io.EOF {
			if c.line == 1 {
				return c.error(ErrEmptyFile)
			}
			return nil
		}

		// handle errors
		if err != nil {
			return err
		}

		// if it's the first line, read header
		if c.line == 1 {
			// determine email column index
			if err := c.determineEmailColumnIndex(record); err != nil {
				return c.error(err)
			}
			continue
		}

		// if it's not the first line, read records, update domain counter
		err = c.updateDomainCounter(record)
		if err != nil {
			return c.error(err)
		}
	}
}

// transforms domain counter to sorted EmailsByDomainQtyList data structure
func (c *CustomerImporter) getResult() (*EmailsByDomainQtyList, error) {
	var result EmailsByDomainQtyList

	// transform domain counter map to sortable list
	for domain, emailsQuantity := range c.domainCounter {
		result = append(result, EmailsByDomainQty{Domain: domain, EmailsCount: emailsQuantity})
	}

	// sort
	sort.Sort(result)

	// if there are no records return error
	if len(result) < 1 {
		return nil, c.error(ErrNoValidEmailsFound)
	}

	return &result, nil
}

// determine email column index by email field name
func (c *CustomerImporter) determineEmailColumnIndex(headerRecord []string) error {
	// try to get index of field by name
	for index, r := range headerRecord {
		if r == c.emailFieldName {
			c.emailColumnIndex = index
			return nil
		}
	}
	// if the field is not found, return an error
	return errors.New(ErrFieldNotExists.Error() + fmt.Sprintf(" %s field", c.emailFieldName))
}

// updates domain counter
func (c *CustomerImporter) updateDomainCounter(record []string) error {
	// retrieve email field from record
	email := record[c.emailColumnIndex]

	// check if email was already added
	err := c.handleDuplicates(email)
	if err != nil {
		if c.skipErrDupEmails {
			return nil
		}
		return err
	}

	// extract domain name from email
	domainName, err := getDomainNameFromEmail(email)
	if err != nil {
		if c.skipErrInvalidEmails {
			return nil
		}
		return err
	}

	// increment domain counter
	c.domainCounter[domainName]++

	return nil
}

// checks if email was counted and updates counted state
func (c *CustomerImporter) handleDuplicates(email string) error {
	// check if email was counted
	if _, isCounted := c.countedEmails[email]; isCounted {
		return ErrEmailDuplicate
	}

	// update email counted state
	c.countedEmails[email] = true

	return nil
}

// error creates new csv.ParseError based on err.
func (c *CustomerImporter) error(err error) error {
	return &csv.ParseError{
		Line:   c.line,
		Column: c.emailColumnIndex,
		Err:    err,
	}
}

// extracts domain name from email address
func getDomainNameFromEmail(email string) (string, error) {
	// validate email
	if !IsValidEmail(email) {
		return "", ErrEmailIsNotValid
	}
	// get domain part of the email
	domainName := strings.Split(email, "@")[1]

	return domainName, nil
}
