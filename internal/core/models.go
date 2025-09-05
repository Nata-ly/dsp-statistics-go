package core

import "time"

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
    AvgBid       float64
    BidCount     int
    Closed       bool
}

type Payload struct {
    Name         string  `json:"name"`
    MinPrice     float64 `json:"min_price"`
    ShowTimeTs   int64   `json:"show_time_ts"`
    GID          *string `json:"gid"`
    OID          *string `json:"oid"`
    Duration     *int    `json:"duration"`
    CodeFromOwner *string `json:"code_from_owner"`
}
