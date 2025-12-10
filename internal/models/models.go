package models

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
    MaxBid       float64
    AvgBid       float64
    BidCount     int
    Closed       bool
}
