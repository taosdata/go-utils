package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/taosdata/go-utils/pool"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/connector"
)

type Logger interface {
	Info(args ...interface{})
}
type Executor struct {
	showSQL    bool
	connector  connector.TDengineConnector
	timeLayout string
	db         string
	logger     Logger
}

func NewExecutor(connector connector.TDengineConnector, db string, showSQL bool, logger Logger) *Executor {
	return &Executor{connector: connector, db: db, showSQL: showSQL, logger: logger}
}

type TableInfo struct {
	Fields []*FieldInfo
	Tags   []*FieldInfo
}

type FieldInfo struct {
	Name   string
	Type   string
	Length int
}

func (e *Executor) DescribeTable(ctx context.Context, tableName string) (*TableInfo, error) {
	data, err := e.DoQuery(ctx, fmt.Sprintf("describe %s", e.WithDBName(tableName)))
	if err != nil {
		return nil, err
	}
	var (
		FieldIndex  int
		TypeIndex   int
		LengthIndex int
		NoteIndex   int
	)
	var tags []*FieldInfo
	var fields []*FieldInfo
	for i, s := range data.Head {
		switch s {
		case "Field":
			FieldIndex = i
		case "Type":
			TypeIndex = i
		case "Length":
			LengthIndex = i
		case "Note":
			NoteIndex = i
		}
	}
	for _, d := range data.Data {
		f := &FieldInfo{
			Name:   d[FieldIndex].(string),
			Type:   d[TypeIndex].(string),
			Length: int(d[LengthIndex].(int32)),
		}
		if d[NoteIndex] == "TAG" {
			tags = append(tags, f)
		} else {
			fields = append(fields, f)
		}
	}
	return &TableInfo{
		Fields: fields,
		Tags:   tags,
	}, nil
}

func (e *Executor) CreateSTable(ctx context.Context, tableName string, info *TableInfo) error {
	fields := info.Fields
	tags := info.Tags
	if len(fields) == 0 {
		return errors.New("need fields info")
	}
	if len(tags) == 0 {
		return errors.New("need tags info")
	}
	fieldSqlList := []string{"ts timestamp"}
	for _, field := range fields {
		fieldSqlList = append(fieldSqlList, e.generateFieldSql(field))
	}
	var tagsSqlList []string
	for _, tag := range tags {
		tagsSqlList = append(tagsSqlList, e.generateFieldSql(tag))
	}
	sql := fmt.Sprintf(
		"create stable if not exists %s (%s) tags (%s)",
		e.WithDBName(tableName),
		strings.Join(fieldSqlList, ","),
		strings.Join(tagsSqlList, ","),
	)
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) InsertUsingSTable(ctx context.Context, tableName string, stableName string, tags string, values []string) error {
	b := pool.BytesPoolGet()
	b.WriteString("insert into ")
	b.WriteString(e.WithDBName(tableName))
	b.WriteString(" using ")
	b.WriteString(e.WithDBName(stableName))
	b.WriteString(" tags (")
	b.WriteString(tags)
	b.WriteString(") values ")
	for _, value := range values {
		b.WriteByte('(')
		b.WriteString(value)
		b.WriteString(") ")
	}
	sql := b.String()
	pool.BytesPoolPut(b)
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) AddColumn(ctx context.Context, tableType string, tableName string, info *FieldInfo) error {
	sql := fmt.Sprintf(
		"alter %s %s add column %s ",
		tableType,
		e.WithDBName(tableName),
		e.generateFieldSql(info),
	)
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) AddTag(ctx context.Context, tableName string, info *FieldInfo) error {
	sql := fmt.Sprintf(
		"alter stable %s add tag %s",
		e.WithDBName(tableName),
		e.generateFieldSql(info),
	)
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) ModifyTagLength(ctx context.Context, tableName string, info *FieldInfo) error {
	sql := fmt.Sprintf(
		"alert stable %s modify TAG %s",
		e.WithDBName(tableName),
		e.generateFieldSql(info))
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) ModifyColumnLength(ctx context.Context, tableType string, tableName string, info *FieldInfo) error {
	sql := fmt.Sprintf(
		"alert %s %s modify column %s",
		tableType,
		e.WithDBName(tableName),
		e.generateFieldSql(info))
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) CreateDatabase(ctx context.Context, keep int, update int) error {
	sql := fmt.Sprintf("create database if not exists %s keep %d update %d", e.db, keep, update)
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) GetPrecision(ctx context.Context) (string, error) {
	sql := "show databases"
	data, err := e.DoQuery(ctx, sql)
	if err != nil {
		return "", err
	}
	precisionIndex := -1
	nameIndex := -1
	for i, s := range data.Head {
		if s == "precision" {
			precisionIndex = i
		} else if s == "name" {
			nameIndex = i
		}
	}
	if precisionIndex == -1 {
		return "", errors.New("precision not exist")
	}
	if nameIndex == -1 {
		return "", errors.New("name not exist")
	}
	for _, rowData := range data.Data {
		if rowData[nameIndex].(string) == e.db {
			return rowData[precisionIndex].(string), nil
		}
	}
	return "", errors.New("precision not found")
}

func (e *Executor) AlterDatabase(ctx context.Context, parameter string, value int) error {
	sql := fmt.Sprintf("ALTER DATABASE %s %s %d", e.db, parameter, value)
	_, err := e.DoExec(ctx, sql)
	return err
}

func (e *Executor) SetTimeLayout(layout string) {
	e.timeLayout = layout
}

func (e *Executor) Query(ctx context.Context, request *common.QueryRequest) (*common.QueryResponse, error) {
	var resp common.QueryResponse
	if len(request.Tables) == 0 {
		return &resp, nil
	}
	wg := sync.WaitGroup{}
	resultChan := make(chan []*common.QueryResult, 20)
	errorChan := make(chan error)
	finishChan := make(chan struct{})
	cancelCtx, cancel := context.WithCancel(ctx)
	defer func() {
		finishChan <- struct{}{}
		cancel()
	}()
	var err error
	go func() {
		for {
			select {
			case result := <-resultChan:
				resp.Results = append(resp.Results, result...)
				wg.Done()
			case queryError := <-errorChan:
				if err == nil {
					// 只返回第一个错误
					err = queryError
				}
				cancel()
				wg.Done()
			case <-finishChan:
				return
			}
		}
	}()
	for tn, ti := range request.Tables {
		if err != nil {
			return nil, err
		}
		tableName := tn
		tableInfo := ti
		wg.Add(1)
		poolError := pool.GoroutinePool.Submit(func() {
			if err != nil {
				return
			}
			data, queryErr := e.queryTask(cancelCtx, tableName, tableInfo, request)
			if queryErr != nil {
				errorChan <- queryErr
			} else {
				resultChan <- data
			}
		})
		if poolError != nil {
			return nil, err
		}
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return &resp, err
}

func (e *Executor) queryTask(ctx context.Context, tableName string, tableInfo *common.Table, request *common.QueryRequest) ([]*common.QueryResult, error) {
	if len(tableInfo.ColumnList) == 0 {
		return nil, nil
	}
	var result []*common.QueryResult
	if len(tableInfo.Tags) != 0 {
		//超级表
		for _, tagMap := range tableInfo.Tags {
			//每一组tag进行一次查询
			sql, err := e.generateQuerySQL(&queryParameter{
				tableName:   tableName,
				aggregation: request.Aggregation,
				columnList:  tableInfo.ColumnList,
				tagMap:      tagMap,
				start:       request.Start,
				end:         request.End,
				interval:    request.Interval,
				fill:        request.Fill,
				limit:       request.Limit,
				offset:      request.Offset,
			})
			if err != nil {
				return nil, err
			}

			data, err := e.DoQuery(ctx, sql)
			if err != nil {
				return nil, err
			}

			r, err := e.marshalResult(data)
			if err != nil {
				return nil, err
			}

			for column, resultData := range r {
				result = append(result, &common.QueryResult{
					Table:  tableName,
					Tags:   tagMap,
					Column: column,
					Values: resultData,
				})
			}
		}
	} else {
		sql, err := e.generateQuerySQL(&queryParameter{
			tableName:   tableName,
			aggregation: request.Aggregation,
			columnList:  tableInfo.ColumnList,
			tagMap:      nil,
			start:       request.Start,
			end:         request.End,
			interval:    request.Interval,
			fill:        request.Fill,
			limit:       request.Limit,
			offset:      request.Offset,
		})
		if err != nil {
			return nil, err
		}
		data, err := e.DoQuery(ctx, sql)
		if err != nil {
			return nil, err
		}
		r, err := e.marshalResult(data)
		if err != nil {
			return nil, err
		}
		q4 := time.Now()
		for column, resultData := range r {
			result = append(result, &common.QueryResult{
				Table:  tableName,
				Tags:   nil,
				Column: column,
				Values: resultData,
			})
		}
		fmt.Println("q4", time.Since(q4).Microseconds())
	}
	return result, nil
}

func (e *Executor) QueryOneFromSTable(ctx context.Context, sTableName string, whereConditions []string, ts time.Time) (*connector.Data, error) {
	// select * from stable where ts = ? and tag1 = ? and tag2 = ?
	b := pool.BytesPoolGet()
	b.WriteString("select * from ")
	b.WriteString(e.WithDBName(sTableName))
	b.WriteString(" where ts = '")
	b.WriteString(e.formatTime(ts))
	b.WriteByte('\'')
	for _, v := range whereConditions {
		b.WriteString(" and ")
		b.WriteString(v)
	}
	sql := b.String()
	pool.BytesPoolPut(b)
	data, err := e.DoQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	return data, err
}

func (e *Executor) QueryOneFromTable(ctx context.Context, tableName string, ts time.Time) (*connector.Data, error) {
	// select * from table where ts = ?
	b := pool.BytesPoolGet()
	b.WriteString("select * from ")
	b.WriteString(e.WithDBName(tableName))
	b.WriteString(" where ts = '")
	b.WriteString(e.formatTime(ts))
	b.WriteByte('\'')
	sql := b.String()
	pool.BytesPoolPut(b)
	data, err := e.DoQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	return data, err
}

func (e *Executor) DoQuery(ctx context.Context, sql string) (*connector.Data, error) {
	if e.showSQL {
		e.logger.Info(sql)
	}
	return e.connector.Query(ctx, sql)
}

func (e *Executor) DoExec(ctx context.Context, sql string) (int64, error) {
	if e.showSQL {
		e.logger.Info(sql)
	}
	return e.connector.Exec(ctx, sql)
}

type ShowTablesInfo struct {
	Name        string
	CreatedTime time.Time
	columns     int16
	StableName  string
	Uid         int64
	Tid         int32
	VgId        int32
}

type ShowSTablesInfo struct {
	Name        string
	CreatedTime time.Time
	Columns     int16
	tags        int16
	tables      int32
}

func (e *Executor) ShowStables(ctx context.Context) ([]*ShowSTablesInfo, error) {
	b := pool.BytesPoolGet()
	b.WriteString("show ")
	b.WriteString(e.WithDBName("stables"))
	sql := b.String()
	pool.BytesPoolPut(b)
	data, err := e.DoQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	result := make([]*ShowSTablesInfo, 0, len(data.Data))
	for _, d := range data.Data {
		result = append(result, &ShowSTablesInfo{
			Name:        d[0].(string),
			CreatedTime: d[1].(time.Time),
			Columns:     d[2].(int16),
			tags:        d[3].(int16),
			tables:      d[4].(int32),
		})
	}
	return result, nil
}

func (e *Executor) ShowTables(ctx context.Context) ([]*ShowTablesInfo, error) {
	b := pool.BytesPoolGet()
	b.WriteString("show ")
	b.WriteString(e.WithDBName("tables"))
	sql := b.String()
	pool.BytesPoolPut(b)
	data, err := e.DoQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	result := make([]*ShowTablesInfo, 0, len(data.Data))
	for _, d := range data.Data {
		result = append(result, &ShowTablesInfo{
			Name:        d[0].(string),
			CreatedTime: d[1].(time.Time),
			columns:     d[2].(int16),
			StableName:  d[3].(string),
			Uid:         d[4].(int64),
			Tid:         d[5].(int32),
			VgId:        d[6].(int32),
		})
	}
	return result, nil
}

func (e *Executor) GetAllStableNames(ctx context.Context) ([]string, error) {
	b := pool.BytesPoolGet()
	b.WriteString("show ")
	b.WriteString(e.WithDBName("stables"))
	sql := b.String()
	pool.BytesPoolPut(b)
	data, err := e.DoQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(data.Data))
	for _, d := range data.Data {
		result = append(result, d[0].(string))
	}
	return result, nil
}

type info struct {
	index  int
	result map[string][]*common.DataItem
}

func (e *Executor) marshalResult(data *connector.Data) (map[string][]*common.DataItem, error) {
	//速度太慢，尝试分片处理
	var err error
	tsIndex := -1
	wg := sync.WaitGroup{}
	indexMap := make(map[int]string, len(data.Head))
	for i, columnName := range data.Head {
		if columnName == "ts" {
			tsIndex = i
		} else {
			indexMap[i] = columnName
		}
	}
	result := make(map[string][]*common.DataItem)
	//分片
	batch := 7000
	partitionCount := len(data.Data) / batch
	rest := len(data.Data) % batch
	if rest > 0 {
		partitionCount += 1
	}
	partitionChan := make(chan *info, partitionCount)
	errChan := make(chan error)
	finishChan := make(chan struct{})
	defer func() {
		finishChan <- struct{}{}
	}()
	go func() {
		for {
			select {
			case partitionInfo := <-partitionChan:
				for resultColumn, items := range partitionInfo.result {
					if len(result[resultColumn]) == 0 {
						result[resultColumn] = make([]*common.DataItem, len(data.Data))
					}
					for i := 0; i < len(items); i++ {
						result[resultColumn][partitionInfo.index*batch+i] = items[i]
					}
				}
				wg.Done()
			case partitionError := <-errChan:
				if partitionError != nil {
					err = partitionError
					wg.Done()
				}
			case <-finishChan:
				return
			}
		}
	}()
	for i := 0; i < partitionCount; i++ {
		if err != nil {
			return nil, err
		}
		var d [][]interface{}
		if i == partitionCount-1 {
			d = data.Data[batch*i:]
		} else {
			d = data.Data[batch*i : batch*(i+1)]
		}
		index := i
		wg.Add(1)
		poolError := pool.GoroutinePool.Submit(func() {
			tmp := map[string][]*common.DataItem{}
			for _, rowData := range d {
				ts := rowData[tsIndex]
				var t time.Time
				switch ts := ts.(type) {
				case string:
					t, err = time.Parse(e.timeLayout, ts)
					if err != nil {
						errChan <- err
						return
					}
				case time.Time:
					t = ts
				}
				for rowIndex, columnValue := range rowData {
					if rowIndex == tsIndex {
						continue
					} else {
						tmp[indexMap[rowIndex]] = append(tmp[indexMap[rowIndex]], &common.DataItem{
							Value: columnValue,
							Time:  t,
						})
					}
				}
			}
			partitionChan <- &info{
				index:  index,
				result: tmp,
			}
		})
		if poolError != nil {
			return nil, poolError
		}
	}
	wg.Wait()
	return result, err
}

type queryParameter struct {
	tableName   string
	aggregation string
	columnList  []string
	tagMap      map[string]interface{}
	start       time.Time
	end         time.Time
	interval    string
	fill        string
	limit       int
	offset      int
}

func (e *Executor) generateQuerySQL(parameter *queryParameter) (string, error) {
	b := pool.BytesPoolGet()
	//检查聚合参数
	var columns []string
	if parameter.aggregation != "" {
		for _, column := range parameter.columnList {
			columns = append(columns, fmt.Sprintf("%s(%s) as %s", parameter.aggregation, column, column))
		}
	} else {
		columns = parameter.columnList
	}
	b.WriteString("select ")
	b.WriteString(strings.Join(columns, ","))
	b.WriteString(" from ")
	b.WriteString(e.WithDBName(parameter.tableName))
	b.WriteString(" where ts ")
	alreadyHaveWhere := false
	if !parameter.start.IsZero() && !parameter.end.IsZero() {
		alreadyHaveWhere = true
		b.WriteString(">= '")
		b.WriteString(e.formatTime(parameter.start))
		b.WriteString("' and ts <= '")
		b.WriteString(e.formatTime(parameter.end))
		b.WriteString("' ")
	} else if !parameter.start.IsZero() {
		alreadyHaveWhere = true
		b.WriteString(">= '")
		b.WriteString(e.formatTime(parameter.start))
		b.WriteString("' ")
	} else if !parameter.end.IsZero() {
		alreadyHaveWhere = true
		b.WriteString("<= '")
		b.WriteString(e.formatTime(parameter.end))
		b.WriteString("' ")
	}
	if len(parameter.tagMap) != 0 {
		if alreadyHaveWhere {
			b.WriteString("and ")
		} else {
			b.WriteString("where ")
			alreadyHaveWhere = true
		}
	}
	var tagList []string
	for tag, tagValue := range parameter.tagMap {
		switch tagValue.(type) {
		case string:
			tagList = append(tagList, fmt.Sprintf("%s = '%s'", tag, tagValue))
		default:
			tagList = append(tagList, fmt.Sprintf("%s = %s", tag, tagValue))
		}
	}
	for i := 0; i < len(tagList); i++ {
		b.WriteString(tagList[i])
		if i != len(tagList)-1 {
			b.WriteString(" and ")
		} else {
			b.WriteByte(' ')
		}
	}

	if parameter.interval != "" {
		if parameter.aggregation == "" {
			return "", errors.New("aggregation is empty")
		}
		if parameter.fill == "" {
			parameter.fill = "none"
		}
		b.WriteString("interval(")
		b.WriteString(parameter.interval)
		b.WriteString(") fill(")
		b.WriteString(parameter.fill)
		b.WriteByte(')')
	}

	if parameter.limit > 0 {
		_, _ = fmt.Fprintf(b, " limit %d", parameter.limit)
	}
	if parameter.offset > 0 {
		_, _ = fmt.Fprintf(b, " offset %d", parameter.offset)
	}
	sql := b.String()
	pool.BytesPoolPut(b)
	return sql, nil
}

func (e *Executor) WithDBName(source string) string {
	return fmt.Sprintf("%s.%s", e.db, source)
}

func (e *Executor) generateFieldSql(info *FieldInfo) string {
	if info.Type == common.NCHARType || info.Type == common.BINARYType {
		return fmt.Sprintf("%s %s(%d)", info.Name, info.Type, info.Length)
	}
	return fmt.Sprintf("%s %s", info.Name, info.Type)
}

func (e *Executor) formatTime(t time.Time) string {
	return t.In(time.Local).Format(time.RFC3339Nano)
}
