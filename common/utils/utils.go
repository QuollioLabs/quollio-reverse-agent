package utils

import (
	"encoding/hex"
	"fmt"
	"quollio-reverse-agent/repository/qdc"

	hash "lukechampine.com/blake3"
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

func AddQDICToStringAsPrefix(input string) string {
	return fmt.Sprint("【QDIC】", input)
}
