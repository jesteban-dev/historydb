package handlers

import "historydb/src/internal/usecases"

type LogHandler struct {
	logUc usecases.LogUsecases
}

func NewLogHandler(logUc usecases.LogUsecases) *LogHandler {
	return &LogHandler{logUc}
}

func (handler *LogHandler) ListSnapshots() {
	handler.logUc.ListSnapshots()
}
