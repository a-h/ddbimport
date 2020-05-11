package log

import (
	"go.uber.org/zap"
)

// Default logger of the system.
var Default *zap.Logger

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initilize logger: " + err.Error())
	}
	Default = logger
}
