package utils

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
