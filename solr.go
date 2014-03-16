package gosolr

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "strconv"
)

type Solr struct {
    Host   string
    Port   int
    Core   string
    client *http.Client
}

func Connect(host string, port int, core string) (*Solr, error) {
    if len(host) == 0 {
        return nil, fmt.Errorf("Invalid hostname provided: [%v]", host)
    }
    if port < 1 || port > 65535 {
        return nil, fmt.Errorf("Invalid port provided: [%v]", port)
    }
    s := Solr{
        Host: host,
        Port: port,
        Core: core,
    }
    return &s, nil
}

func (s *Solr) Ping(handler string) (map[string]interface{}, error) {
    url := s.getSolrUrl(handler)

    resp, err := s.httpRequest("GET", url, nil, nil)
    if err != nil {
        return nil, err
    }

    jsonData, err := bytesAsJson(&resp)

    return (*jsonData).(map[string]interface{}), nil
}

func (s *Solr) Update(handler string, updateData *[]byte, commit bool, headers map[string]string) (map[string]interface{}, error) {
    url := s.getSolrUrl(handler)
    if commit {
        url += "?commit=true&wt=json"
    } else {
        url += "?commit=false&wt=json"
    }

    resp, err := s.httpRequest("POST", url, headers, updateData)
    if err != nil {
        return nil, err
    }

    jsonData, err := bytesAsJson(&resp)

    return (*jsonData).(map[string]interface{}), nil
}

func (s *Solr) httpRequest(method string, url string, headers map[string]string, body *[]byte) ([]byte, error) {
    if s.client == nil {
        s.client = &http.Client{}
    }

    var requestBody *bytes.Reader
    var request *http.Request
    var err error

    if body == nil {
        request, err = http.NewRequest(method, url, nil)
    } else {
        requestBody = bytes.NewReader(*body)
        request, err = http.NewRequest(method, url, requestBody)
    }

    if err != nil {
        return nil, err
    }

    if headers != nil {
        for key, value := range headers {
            request.Header.Add(key, value)
        }
    }

    response, err := s.client.Do(request)
    if err != nil {
        return nil, err
    }
    defer response.Body.Close()

    data, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return nil, err
    }

    return data, nil
}

func (s *Solr) getSolrUrl(handler string) string {
    return "http://" + s.Host + ":" + strconv.Itoa(s.Port) + "/solr/" + s.Core + "/" + handler
}

func bytesAsJson(b *[]byte) (*interface{}, error) {
    var jsonData interface{}
    err := json.Unmarshal(*b, &jsonData)

    if err != nil {
        return nil, fmt.Errorf("Error processing as json")
    }

    return &jsonData, nil
}

func jsonAsBytes(m map[string]interface{}) (*[]byte, error) {
    b, err := json.Marshal(&m)

    if err != nil {
        return nil, fmt.Errorf("Error processing json")
    }

    return &b, nil
}
