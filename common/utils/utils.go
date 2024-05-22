package utils

import (
	"encoding/hex"
	"fmt"
	"quollio-reverse-agent/repository/qdc"
	"strings"
	"unicode"

	hash "lukechampine.com/blake3"
)

const (
	DefaultPrefix    = "【QDIC】"             // Default prefix for update
	OverwriteIfEmpty = "OVERWRITE_IF_EMPTY" // (Default)only assets whose description is empty string or nil will be updated.
	OverwriteAll     = "OVERWRITE_ALL"      // all asset description will be updated.
)

func GetSpecifiedAssetFromPath(asset qdc.Data, pathLayer string) qdc.Path {
	path := asset.Path
	for _, p := range path {
		if p.PathLayer == pathLayer {
			return p
		}
	}
	return qdc.Path{}
}

func SplitArrayToChunks(arr []string, size int) [][]string {
	var chunks [][]string

	for i := 0; i < len(arr); i += size {
		end := i + size
		if end > len(arr) {
			end = len(arr)
		}
		chunks = append(chunks, arr[i:end])
	}
	return chunks
}

func GetGlobalId(companyId string, clusterId string, dataId string, dataType string) string {
	var prefix string
	switch dataType {
	case "schema":
		prefix = "schm-"
	case "table":
		prefix = "tbl-"
	case "column":
		prefix = "clmn-"
	case "biproject":
		prefix = "bprj-"
	case "workspace":
		prefix = "wksp-"
	case "dashboard":
		prefix = "dsbd-"
	case "sheet":
		prefix = "sht-"
	}
	hash := hash.Sum512([]byte(companyId + clusterId + dataId))
	ret := prefix + hex.EncodeToString(hash[:16])
	return ret
}

func AddPrefixToStringIfNotHas(prefixForUpdate, input string) string {
	if strings.HasPrefix(input, prefixForUpdate) {
		return input
	}
	return fmt.Sprint(prefixForUpdate, input)
}

func IsStringContainJapanese(s string) bool {
	for _, r := range s {
		if unicode.In(r, unicode.Hiragana, unicode.Katakana, unicode.Han) {
			return true
		}
	}
	return false
}
