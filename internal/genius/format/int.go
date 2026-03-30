package format

import "strconv"

func Int32ToString(i int32) string {
	return strconv.FormatInt(int64(i), 10)
}
