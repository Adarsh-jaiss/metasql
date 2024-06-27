package utils

func Nullif(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}