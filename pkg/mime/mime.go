package mime

import (
	"bytes"
	"io"
	"net/http"
)

const sniffLen = 512

// returns the MIME type of input and a new reader containing the whole data from input
func DetectReader(input io.Reader) (string, io.Reader, error) {
	header := new(bytes.Buffer)
	reader := io.TeeReader(input, header)

	_, err := io.CopyN(header, reader, sniffLen)
	// io.UnexpectedEOF means input is smaller than sniffLen, it's not an error in this case
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", nil, err
	}

	ctype := http.DetectContentType(header.Bytes())
	return ctype, io.MultiReader(header, input), nil
}
