package logger

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/lumberjack.v2"
)

// rotatingWriter — io.WriteCloser с ротацией логов через lumberjack.
type rotatingWriter struct {
	lj *lumberjack.Logger
}

func (r *rotatingWriter) Write(p []byte) (n int, err error) {
	return r.lj.Write(p)
}

func (r *rotatingWriter) Close() error {
	return r.lj.Close()
}

// Init инициализирует структурированное логирование с ротацией файлов.
// Логи пишутся одновременно в stdout и в файл с автоматической ротацией:
//   - максимальный размер файла: 50 MB
//   - хранится 3 архивных файла
//   - архивы старше 30 дней удаляются
func Init(logFile string) (io.Closer, error) {
	lj := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    50, // MB
		MaxBackups: 3,
		MaxAge:     30, // дней
		Compress:   true,
	}

	rw := &rotatingWriter{lj: lj}

	// пишем одновременно в терминал и в файл
	multi := io.MultiWriter(os.Stdout, rw)

	handler := slog.NewTextHandler(multi, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	slog.SetDefault(slog.New(handler))
	return rw, nil
}
