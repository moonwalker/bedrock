// $ op run --env-file=pkg/cloudflare/r2/test.env -- go test -v ./pkg/cloudflare/r2

package r2

import (
	"fmt"
	"strings"
	"testing"
)

func TestUpload(t *testing.T) {
	s, err := Upload("test/test.txt", strings.NewReader("Hello World!"))
	if err != nil {
		t.Error(err)
	}
	fmt.Println(s)
}

func TestUploadURL(t *testing.T) {
	s, err := UploadURL("test/mystery-box_video.mp4", "https://videos.ctfassets.net/xd8g9hulrobw/5GQ4uCKmpZJFeZJe8tIQgM/55a6de322194cba4fcb3032c9fe0936a/Mystery-box_video.mp4")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(s)
}
