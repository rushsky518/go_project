package conf

type Task struct {
	TaskPeriod int
	IndexPattern string
	RetainDays int
	EsUrl string
	Username string
	Password string
}
