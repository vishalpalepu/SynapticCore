package extractor

import (
	"fmt"
	"strconv"
	"strings"
)

func ChuckText(docID, text string, approxTokensPerChunk int, overlapPercent float64) []Chunk {
	paras := splitParagraphs(text)

	var chunks []Chunk
	var current strings.Builder
	currentTokens := 0
	chunkIndex := 0
	parentID := docID + "-parent"

	flush := func() {
		content := strings.TrimSpace(current.String())
		if content == "" {
			return
		}
		fmt.Printf("Creating chunk %d with %d tokens\n", chunkIndex, currentTokens) // debug log to see the chunk creation and token count
		// Creating single Chunk
		chunk := Chunk{
			DocID:      docID,
			ChunkID:    docID + "-chunk-" + itoa(chunkIndex),
			ParentID:   parentID,
			Index:      chunkIndex,
			Text:       content,
			TokenCount: currentTokens,
		}
		chunks = append(chunks, chunk) // adding created chunk into the array

		chunkIndex++                                       // for the next chunk
		overlaptext := getOverlap(content, overlapPercent) // to apply the condition of having 10% of previous chunk in the next chunk

		current.Reset()                             // empty
		current.WriteString(overlaptext)            // add at the top of next chunk
		currentTokens = estimateTokens(overlaptext) // adding the tokens 10%
	}

	for _, p := range paras {
		p = strings.TrimSpace(p)

		if p == "" {
			continue
		}

		est := estimateTokens(p)

		if est > 16 {
			if currentTokens > 0 {
				flush()
			}

			parts := splitParagraphsByTokens(p, approxTokensPerChunk)

			for _, sp := range parts {
				current.WriteString(sp)
				currentTokens += estimateTokens(sp)
				flush()
			}
			continue
		}

		// if 10% or previous + tokens of current > limit create a chunk
		if currentTokens+est > approxTokensPerChunk && currentTokens > 0 {
			flush()
		}

		if current.Len() > 0 {
			current.WriteString("\n\n")
		}

		current.WriteString(p)
		currentTokens += est
	}

	flush()

	return chunks
}

func splitParagraphs(text string) []string {
	return strings.Split(text, "\n\n")
}

func getOverlap(text string, percent float64) string {
	if len(text) == 0 || percent <= 0 {
		return ""
	}
	size := int(float64(len(text)) * percent) // 10% of text len
	if size <= 0 {
		return ""
	}

	return text[len(text)-size:]
}

func estimateTokens(s string) int {
	if len(s) == 0 {
		return 0
	}
	return len(s) / 4
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func splitParagraphsByTokens(text string, approxTokensPerChunk int) []string {
	var parts []string

	words := strings.Fields(text)
	var current strings.Builder
	currentTokens := 0

	for _, word := range words {
		est := estimateTokens(word)
		if currentTokens+est > approxTokensPerChunk && currentTokens > 0 {
			parts = append(parts, current.String())
			current.Reset()
			currentTokens = 0
		}

		if current.Len() > 0 {
			current.WriteString(" ")
		}
		current.WriteString(word)
		currentTokens += est
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}
