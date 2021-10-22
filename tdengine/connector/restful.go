package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/taosdata/go-utils/json"
	"github.com/taosdata/go-utils/tdengine/common"
	"github.com/taosdata/go-utils/tdengine/config"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"time"
)

const Layout = "2006-01-02T15:04:05.999999999-0700"

type TDEngineRestfulResp struct {
	Status     string          `json:"status"`
	Head       []string        `json:"-"` //从 2.0.17 版本开始，建议不要依赖 head 返回值来判断数据列类型，而推荐使用 column_meta。在未来版本中，有可能会从返回值中去掉 head 这一项
	Data       [][]interface{} `json:"data"`
	ColumnMeta [][]interface{} `json:"column_meta"` //从 2.0.17 版本开始，返回值中增加这一项来说明 data 里每一列的数据类型。具体每个列会用三个值来说明，分别为：列名、列类型、类型长度
	Rows       int             `json:"rows"`
	Code       int             `json:"code"`
	Desc       string          `json:"desc"`
}

type RestfulConnector struct {
	address    string
	authType   string
	username   string
	password   string
	token      string
	url        *url.URL
	httpClient *http.Client
	queryUrl   string
}

func NewRestfulConnector(conf *config.TDengineRestful) (*RestfulConnector, error) {
	var err error
	connector := &RestfulConnector{address: conf.Address, authType: conf.AuthType, username: conf.Username, password: conf.Password}
	connector.url, err = url.Parse(conf.Address)
	if err != nil {
		return nil, err
	}
	var transport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxConnsPerHost:       conf.MaxConnsPerHost,
	}
	connector.httpClient = &http.Client{
		Transport: transport,
	}
	switch conf.AuthType {
	case common.BasicAuthType:
		connector.token = base64.StdEncoding.EncodeToString([]byte(conf.Username + ":" + conf.Password))
	case common.TaosdAuthType:
		loginUrl := path.Join(connector.url.String(), "/rest/login", conf.Username, conf.Password)
		resp, err := connector.httpClient.Get(loginUrl)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("get taos token error statusCode: %d,body: %s", resp.StatusCode, string(body))
		}
		var respData TDEngineRestfulResp
		err = json.Unmarshal(body, &respData)
		if err != nil {
			return nil, err
		}
		if respData.Status == "succ" {
			connector.token = respData.Desc
		} else {
			return nil, fmt.Errorf("get taos token error statusCode: %d,body: %s", resp.StatusCode, body)
		}
	default:
		return nil, fmt.Errorf("unsupported auth type %s", conf.AuthType)
	}

	connector.url.Path = path.Join(connector.url.Path, "/rest/sqlutc")
	connector.queryUrl = connector.url.String()
	return connector, nil
}

func (h *RestfulConnector) Query(ctx context.Context, sql string) (*Data, error) {
	data, err := h.query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &Data{
		Head: data.Head,
		Data: data.Data,
	}, nil
}

func (h *RestfulConnector) Exec(ctx context.Context, sql string) (int64, error) {
	data, err := h.query(ctx, sql)
	if err != nil {
		return 0, err
	}
	return int64(data.Rows), nil
}

func (h *RestfulConnector) query(ctx context.Context, sql string) (*TDEngineRestfulResp, error) {
	contentReader := bytes.NewReader([]byte(sql))
	request, _ := http.NewRequestWithContext(ctx, "POST", h.queryUrl, contentReader)
	if h.token != "" {
		request.Header.Set("Authorization", fmt.Sprintf("%s %s", h.authType, h.token))
	}
	resp, err := h.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 400 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(body))
	}
	var data *TDEngineRestfulResp
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, err
	}
	if data.Status != "succ" {
		if data.Desc != "" {
			return data, &common.TDengineError{Code: data.Code, Desc: data.Desc}
		}
		return data, fmt.Errorf("query: %s error,response body: %#v", sql, data)
	}
	return formatResponse(data)
}

func formatResponse(data *TDEngineRestfulResp) (*TDEngineRestfulResp, error) {
	data.Head = make([]string, 0, len(data.ColumnMeta))
	types := make([]int, len(data.ColumnMeta))
	for index, columnMessage := range data.ColumnMeta {
		colName := columnMessage[0].(string)
		colType := columnMessage[1].(float64)
		types[index] = int(colType)
		data.Head = append(data.Head, colName)
	}
	for rowIndex, row := range data.Data {
		for columnIndex, value := range row {
			if value == nil {
				continue
			}
			switch value.(type) {
			case string:
				if types[columnIndex] == 9 {
					t, _ := time.Parse(Layout, value.(string))
					data.Data[rowIndex][columnIndex] = t
				}
			case float64:
				switch types[columnIndex] {
				case 1:
					data.Data[rowIndex][columnIndex] = int(value.(float64)) == 1
				case 2:
					data.Data[rowIndex][columnIndex] = int8(value.(float64))
				case 3:
					data.Data[rowIndex][columnIndex] = int16(value.(float64))
				case 4:
					data.Data[rowIndex][columnIndex] = int32(value.(float64))
				case 5:
					data.Data[rowIndex][columnIndex] = int64(value.(float64))
				case 6:
					data.Data[rowIndex][columnIndex] = float32(value.(float64))
				case 7:
					data.Data[rowIndex][columnIndex] = value.(float64)
				case 11:
					data.Data[rowIndex][columnIndex] = uint8(value.(float64))
				case 12:
					data.Data[rowIndex][columnIndex] = uint16(value.(float64))
				case 13:
					data.Data[rowIndex][columnIndex] = uint32(value.(float64))
				case 14:
					data.Data[rowIndex][columnIndex] = uint64(value.(float64))
				}
			}
		}
	}
	return data, nil
}

type RawTDEngineRestfulResp struct {
	Status     string              `json:"status"`
	Data       [][]json.RawMessage `json:"data"`
	ColumnMeta [][]interface{}     `json:"column_meta"` //从 2.0.17 版本开始，返回值中增加这一项来说明 data 里每一列的数据类型。具体每个列会用三个值来说明，分别为：列名、列类型、类型长度
	Rows       int                 `json:"rows"`
	Code       int                 `json:"code"`
	Desc       string              `json:"desc"`
}
