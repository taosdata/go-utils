// +build jsoniter

package json

import (
	"strconv"

	jsoniter "github.com/json-iterator/go"
)

var (
	_json         = jsoniter.ConfigCompatibleWithStandardLibrary
	Marshal       = _json.Marshal
	Unmarshal     = _json.Unmarshal
	MarshalIndent = _json.MarshalIndent
	NewDecoder    = _json.NewDecoder
	NewEncoder    = _json.NewEncoder
)

type Number string

// String returns the literal text of the number.
func (n Number) String() string { return string(n) }

// Float64 returns the number as a float64.
func (n Number) Float64() (float64, error) {
	return strconv.ParseFloat(string(n), 64)
}

// Int64 returns the number as an int64.
func (n Number) Int64() (int64, error) {
	return strconv.ParseInt(string(n), 10, 64)
}
