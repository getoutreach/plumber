package format

import (
	"errors"
	"fmt"
	"strconv"
)

func StringPtrToInt32(s *string) (int32, error) {
	if s == nil {
		return 0, errors.New("Provided value can't be null")
	}
	return StringToInt32(*s)
}

func StringToInt32(s string) (int32, error) {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("Can't convert to %s int %w", s, err)
	}
	return int32(i), nil
}
