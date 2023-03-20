package logging

import (
	"bytes"
	"fmt"
	"io"
)

type Color string

const (
	ColorNRM Color = "\x1B[0m"
	ColorRED       = "\x1B[31m"
	ColorGRN       = "\x1B[32m"
	ColorYEL       = "\x1B[33m"
	ColorBLU       = "\x1B[34m"
	ColorMAG       = "\x1B[35m"
	ColorCYN       = "\x1B[36m"
	ColorWHT       = "\x1B[37m"
)

func FprintfColor(writor io.Writer, c Color, f string, args ...interface{}) {
	s := fmt.Sprintf(f, args...)
	fmt.Fprint(writor, string(c)+s+string(ColorNRM))
}

func SprintfColor(c Color, f string, args ...interface{}) string {
	var buf bytes.Buffer
	FprintfColor(&buf, c, f, args...)

	return buf.String()
}
