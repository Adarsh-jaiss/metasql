package utils

func Nullif(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}

func Coalesce(strs ...*string) string {
	for _, str := range strs {
		if str != nil {
			return *str
		}
	}
	return ""
}