package core

import (
    "encoding/json"
    "errors"
    "strconv"
    "time"
)

type DspRequest struct {
    ID       int
    Name     int
    GID      string
    OID      *string
    Duration *int
}

type DspStatistic struct {
    ID           int
    DspRequestID int
    Date         time.Time
    Hour         int
    MinBid       float64
    MaxBid       float64
    AvgBid       float64
    BidCount     int
    Closed       bool
}

// Кастомный тип для полей, которые могут быть числами или строками
type StringOrNumber string

func (son *StringOrNumber) UnmarshalJSON(data []byte) error {
    // Пытаемся распарсить как строку
    var s string
    if err := json.Unmarshal(data, &s); err == nil {
        *son = StringOrNumber(s)
        return nil
    }

    // Пытаемся распарсить как число
    var f float64
    if err := json.Unmarshal(data, &f); err == nil {
        *son = StringOrNumber(strconv.FormatFloat(f, 'f', -1, 64))
        return nil
    }

    return errors.New("не удалось распарсить как строку или число")
}

type Payload struct {
    Name         string        `json:"name"`
    MinPrice     float64       `json:"min_price"`
    ShowTimeTs   int64         `json:"show_time_ts"`
    GID          *string       `json:"gid"`
    OID          *string       `json:"oid"`
    Duration     *int          `json:"duration"`
    CodeFromOwner *StringOrNumber `json:"code_from_owner"`
}
