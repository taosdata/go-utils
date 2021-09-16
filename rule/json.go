package rule

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/util"
	"github.com/tidwall/gjson"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	RuleName string `json:"rule_name"`
	Topic    string `json:"topic"`
	Rule     *Rule  `json:"rule"`
}

type Rule struct {
	//规定超级表
	STable  string     `json:"s_table"`
	Tags    []*Field   `json:"tags"`
	Columns []*Field   `json:"columns"`
	Table   *TableName `json:"table"`
}

type TableName struct {
	DefaultValue string `json:"default_value"`
	Path         string `json:"path"`
}

type Field struct {
	Name         string      `json:"name"`
	ValueType    string      `json:"value_type"`
	Length       int         `json:"length"`
	DefaultValue interface{} `json:"default_value"`
	Path         string      `json:"path"`
	TimeLayout   string      `json:"time_layout"`
}

type Column struct {
	Name         string      `json:"name"`
	Index        int         `json:"index"`
	ValueType    ValueType   `json:"value_type"`
	TimeLayout   string      `json:"time_layout"`
	FieldType    FieldType   `json:"field_type"`
	DefaultValue interface{} `json:"default_value"`
	Path         string      `json:"path"`
}

type Table struct {
	STable    string `json:"s_table"`
	TagLen    int    `json:"tag_len"`
	ColumnLen int    `json:"column_len"`
	*TableName
}

type FieldType int

const (
	TagType FieldType = iota + 1
	ColumnType
)

const (
	IntType         = "int"
	FloatType       = "float"
	BoolType        = "bool"
	StringType      = "string"
	TimeString      = "timeString"
	TimeSecond      = "timeSecond"
	TimeMillisecond = "timeMillisecond"
	TimeMicrosecond = "timeMicrosecond"
	TimeNanoSecond  = "timeNanosecond"
)

type ValueType int

const (
	IIntType ValueType = iota + 1
	IDoubleType
	IBoolType
	IStringType
	ITimeString
	ITimeSecond
	ITimeMillisecond
	ITimeMicrosecond
	ITimeNanoSecond
)

var valueTypeMap = map[string]ValueType{
	IntType:         IIntType,
	FloatType:       IDoubleType,
	BoolType:        IBoolType,
	StringType:      IStringType,
	TimeString:      ITimeString,
	TimeSecond:      ITimeSecond,
	TimeMillisecond: ITimeMillisecond,
	TimeMicrosecond: ITimeMicrosecond,
	TimeNanoSecond:  ITimeNanoSecond,
}

func valueTypeConversion(strType string) (ValueType, error) {
	v, exist := valueTypeMap[strType]
	if exist {
		return v, nil
	} else {
		return 0, errors.New(`unknown value type`)
	}
}

var timeTypeMap = map[string]struct{}{
	TimeString:      {},
	TimeSecond:      {},
	TimeMillisecond: {},
	TimeMicrosecond: {},
	TimeNanoSecond:  {},
}

func verifyTimeType(field *Field) error {
	_, exist := timeTypeMap[field.ValueType]
	if !exist {
		return errors.New("field is not a time type")
	}
	if field.ValueType == TimeString {
		if field.TimeLayout == "" {
			return errors.New("must set TimeLayout if value type is timeString")
		}
	}
	return nil
}

func verifySTableName(sTableName string) error {
	if len(sTableName) == 0 {
		return errors.New("STable name could not be empty")
	}
	if !((sTableName[0] >= 'A' && sTableName[0] <= 'Z') || (sTableName[0] >= 'a' && sTableName[0] <= 'z') || sTableName[0] == '_') {
		return errors.New("STable name start with an illegal character")
	}
	if len(sTableName) > 1 {
		for _, charI := range sTableName {
			if !((charI >= 'A' && charI <= 'Z') || (charI >= 'a' && charI <= 'z') || (charI >= '0' && charI <= '9') || charI == '_') {
				return errors.New("STable name contains illegal character")
			}
		}
	}
	return nil
}

type Manage struct {
	// topic -> jsonPath -> Column
	topicMap map[string]map[string]*Column
	// topic -> jsonPath -> Table
	topicTableMap map[string]*Table
	config        []*Config
}

func NewRuleManage(rulePath string) (*Manage, error) {
	pathExist := util.PathExist(rulePath)
	if !pathExist {
		return nil, errors.New("path not exist")
	}
	f, err := os.Open(rulePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var config []*Config
	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		return nil, err
	}

	manager := &Manage{
		topicMap:      map[string]map[string]*Column{},
		topicTableMap: map[string]*Table{},
		config:        config,
	}

	for _, c := range config {
		rule := c.Rule
		topic := c.Topic
		if len(rule.Tags) == 0 {
			return nil, fmt.Errorf("topic %s :tags could not be empty", topic)
		}
		if len(rule.Columns) < 2 {
			return nil, fmt.Errorf("topic %s :must contains at least two columns", topic)
		}
		if rule.Columns[0].Name != "ts" {
			return nil, fmt.Errorf("topic %s :first column must named 'ts'", topic)
		}
		err := verifySTableName(rule.STable)
		if err != nil {
			return nil, err
		}
		err = verifyTimeType(rule.Columns[0])
		if err != nil {
			return nil, err
		}
		pathMap := map[string]*Column{}
		for index, field := range rule.Columns {
			vt, err := valueTypeConversion(field.ValueType)
			if err != nil {
				return nil, err
			}
			pathMap[field.Path] = &Column{
				Name:         field.Name,
				Index:        index,
				ValueType:    vt,
				TimeLayout:   field.TimeLayout,
				FieldType:    ColumnType,
				DefaultValue: field.DefaultValue,
				Path:         field.Path,
			}
		}
		for index, field := range rule.Tags {
			vt, err := valueTypeConversion(field.ValueType)
			if err != nil {
				return nil, err
			}
			pathMap[field.Path] = &Column{
				Name:         field.Name,
				Index:        index,
				ValueType:    vt,
				TimeLayout:   field.TimeLayout,
				FieldType:    TagType,
				DefaultValue: field.DefaultValue,
				Path:         field.Path,
			}
		}
		manager.topicMap[topic] = pathMap
		manager.topicTableMap[topic] = &Table{
			STable:    rule.STable,
			TagLen:    len(rule.Tags),
			ColumnLen: len(rule.Columns),
			TableName: &TableName{
				DefaultValue: rule.Table.DefaultValue,
				Path:         rule.Table.Path,
			},
		}
	}
	return manager, nil
}

func (r *Manage) GetPathMap(topic string) map[string]*Column {
	return r.topicMap[topic]
}

type Result struct {
	TableName  string
	STableName string
	Columns    []interface{}
	Tags       []interface{}
}

// Parse  json path to Result
// if *Result is nil means no rule on the topic
func (r *Manage) Parse(topic string, data []byte) (*Result, error) {
	pathMap := r.topicMap[topic]
	res := &Result{}
	if len(pathMap) > 0 {
		table := r.topicTableMap[topic]
		colLen := table.ColumnLen
		tagLen := table.TagLen
		res.STableName = table.STable
		res.Columns = make([]interface{}, colLen)
		res.Tags = make([]interface{}, tagLen)
		result := gjson.GetBytes(data, table.Path)
		if !result.Exists() {
			res.TableName = table.DefaultValue
		} else {
			b := pool.BytesPoolGet()
			b.WriteByte('_')
			b.WriteString(result.String())
			res.TableName = b.String()
			pool.BytesPoolPut(b)
		}
		for path, column := range pathMap {
			result = gjson.GetBytes(data, path)
			if !result.Exists() {
				if column.FieldType == ColumnType && column.Index == 0 {
					res.Columns[0] = time.Now()
				}
				switch column.FieldType {
				case ColumnType:
					res.Columns[column.Index] = column.DefaultValue
				case TagType:
					res.Tags[column.Index] = column.DefaultValue
				}
			} else {
				v, err := parseValue(column, result)
				if err != nil {
					return nil, err
				}
				switch column.FieldType {
				case ColumnType:
					res.Columns[column.Index] = v
				case TagType:
					res.Tags[column.Index] = v
				}
			}
		}
		return res, nil
	}
	return nil, nil
}

func (r *Manage) RuleExist(topic string) bool {
	_, exist := r.topicMap[topic]
	return exist
}

func parseValue(column *Column, value gjson.Result) (interface{}, error) {
	switch column.ValueType {
	case IIntType:
		return value.Int(), nil
	case IDoubleType:
		return value.Float(), nil
	case IBoolType:
		return value.Bool(), nil
	case IStringType:
		return value.String(), nil
	case ITimeString:
		return time.Parse(column.TimeLayout, value.String())
	case ITimeSecond:
		return time.Unix(value.Int(), 0), nil
	case ITimeMillisecond:
		return time.Unix(0, value.Int()*1e6), nil
	case ITimeMicrosecond:
		return time.Unix(0, value.Int()*1e3), nil
	case ITimeNanoSecond:
		return time.Unix(0, value.Int()), nil
	}
	return nil, errors.New("unknown error")
}

func (r *Manage) GenerateCreateSql() []string {
	result := make([]string, 0, len(r.config))
	for _, c := range r.config {
		b := pool.BytesPoolGet()
		b.WriteString("create stable if not exists ")
		b.WriteString(c.Rule.STable)
		b.WriteString(" (")
		columns := generateFieldSql(c.Rule.Columns)
		b.WriteString(strings.Join(columns, ","))
		b.WriteString(") tags(")
		tags := generateFieldSql(c.Rule.Tags)
		b.WriteString(strings.Join(tags, ","))
		b.WriteByte(')')
		result = append(result, b.String())
		pool.BytesPoolPut(b)
	}
	return result
}

func generateFieldSql(fields []*Field) []string {
	fieldSql := make([]string, 0, len(fields))
	for _, field := range fields {
		switch field.ValueType {
		case IntType:
			fieldSql = append(fieldSql, fmt.Sprintf("%s bigint", field.Name))
		case FloatType:
			fieldSql = append(fieldSql, fmt.Sprintf("%s double", field.Name))
		case BoolType:
			fieldSql = append(fieldSql, fmt.Sprintf("%s bool", field.Name))
		case StringType:
			fieldSql = append(fieldSql, fmt.Sprintf("%s binary(%d)", field.Name, field.Length))
		case TimeString, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanoSecond:
			fieldSql = append(fieldSql, fmt.Sprintf("%s timestamp", field.Name))
		}
	}
	return fieldSql
}

func (r *Result) ToSql() string {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("insert into ")
	b.WriteString(r.TableName)
	b.WriteString(" using ")
	b.WriteString(r.STableName)
	b.WriteString(" tags(")
	generateInsertSql(b, r.Tags)
	b.WriteString(") values(")
	generateInsertSql(b, r.Columns)
	b.WriteByte(')')
	return b.String()
}

func generateInsertSql(b *bytes.Buffer, values []interface{}) {
	for i, value := range values {
		if value == nil {
			b.WriteString("null")
		} else {
			switch v := value.(type) {
			case string:
				b.WriteByte('\'')
				b.WriteString(util.EscapeString(v))
				b.WriteByte('\'')
			case int64:
				b.WriteString(strconv.FormatInt(v, 10))
			case float64:
				b.WriteString(fmt.Sprintf("%f", v))
			case bool:
				b.WriteString(strconv.FormatBool(v))
			case time.Time:
				b.WriteByte('\'')
				b.WriteString(v.Format(time.RFC3339Nano))
				b.WriteByte('\'')
			default:
				b.WriteString(fmt.Sprintf("%s", v))
			}
		}
		if i != len(values)-1 {
			b.WriteByte(',')
		}
	}
}
