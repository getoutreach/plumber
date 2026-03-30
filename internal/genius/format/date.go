// Copied from SmartStore convert package but using protoapi's own Date struct
package format

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TimestampValueToTime converts *timestamp.Timestamp to time.Time, nil converted zero Time.
func DurationValuePtrToDuration(v *durationpb.Duration) time.Duration {
	if v == nil {
		// Note: v.AsTime leads to an epoch time if v is nil or an empty timestamppb.Timestamp{}.
		// For an empty timestamppb.Timestamp{}, it is the right thing to do: empty is a perfectly
		// valid day in the not-so-long-ago history (1970-01-01) and it can be somebody's birthday.
		// However, doing so for nil can lead to undesired conseqvenses: if somebody moves a table
		// with a time column from one DB to another, with NULL-able source and NOT NULL target, we
		// may accidentally auto-convert a 'technically missing time/day' to a valid epoch time,
		// leaving no space for the service owners to know if origin was present or NULL.
		return 0
	}

	return v.AsDuration()
}

func DurationToDurationValue(t time.Duration) *durationpb.Duration {
	return durationpb.New(t)
}

func ToSecondDurationFromInt64(v int64) *durationpb.Duration {
	return durationpb.New(time.Second * time.Duration(v))
}

// TimeToTimestampValue converts time.Time to *timestamp.Timestamp.
func TimeToTimestampValue(v time.Time) *timestamppb.Timestamp {
	if v.IsZero() {
		return nil
	}
	// unlike TimestampValueToTime, we cannot convert zero Time{} to a nil value, because
	// we may violate non-nil constraint on the receiver side. Thus, zero Time will be sent
	// as Seconds: -62135596800, which is the delta between Day 1 in AD (0001/01/01) to an
	// epoch time (1970/01/01), as calculated by time.Time.
	return timestamppb.New(v)
}

// StringPtrToTimestamp converts an apiv2 formatted string to timestamp
func StringPtrToTimestamp(s *string) (*timestamppb.Timestamp, error) {
	if s == nil || *s == "" {
		return nil, nil
	}

	supportedLayouts := []string{
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000-07:00",
	}

	for _, layout := range supportedLayouts {
		t, err := time.Parse(layout, *s)
		if err == nil {
			if t.IsZero() {
				return nil, nil
			}

			return timestamppb.New(t), nil
		}
	}

	return nil, fmt.Errorf("Can't parse timestamp %s", *s)
}

// TimestampToString converts a timestamp to an apiv2 formatted string
func TimestampToString(ts *timestamppb.Timestamp) (*string, error) {
	if ts == nil {
		return nil, nil
	}

	t := ts.AsTime()
	if t.IsZero() {
		return nil, nil
	}

	s := t.Format("2006-01-02T15:04:05.000Z")
	return &s, nil
}
