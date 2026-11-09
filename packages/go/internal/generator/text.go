package main

import (
	"strings"
)

const commentWidth = 100

func wrapWords(text string, width int) []string {
	var lines []string
	var current strings.Builder
	for _, word := range strings.Fields(text) {
		if current.Len() > 0 && current.Len()+1+len(word) > width {
			lines = append(lines, current.String())
			current.Reset()
		}
		if current.Len() > 0 {
			current.WriteByte(' ')
		}
		current.WriteString(word)
	}
	if current.Len() > 0 {
		lines = append(lines, current.String())
	}
	return lines
}

type commentBlock struct {
	paragraphs [][]string
}

func (c *commentBlock) paragraph(text string) {
	text = strings.Join(strings.Fields(text), " ")
	if text != "" {
		c.paragraphs = append(c.paragraphs, wrapWords(text, commentWidth))
	}
}

func (c *commentBlock) list(heading string, items []string) {
	if len(items) == 0 {
		return
	}
	lines := []string{heading}
	for _, item := range items {
		wrapped := wrapWords(item, commentWidth-4)
		for position, line := range wrapped {
			if position == 0 {
				lines = append(lines, "  - "+line)
			} else {
				lines = append(lines, "    "+line)
			}
		}
	}
	c.paragraphs = append(c.paragraphs, lines)
}

func (c *commentBlock) render(indent string) string {
	var out strings.Builder
	for position, paragraph := range c.paragraphs {
		if position > 0 {
			out.WriteString(indent + "//\n")
		}
		for _, line := range paragraph {
			out.WriteString(indent + "// " + line + "\n")
		}
	}
	return out.String()
}

func docComment(indent, lead, description string) string {
	block := &commentBlock{}
	block.paragraph(strings.TrimSpace(lead + " " + description))
	return block.render(indent)
}

func quoteGo(value string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\n':
			out.WriteString(`\n`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 0x20 {
				out.WriteString(`\u00`)
				out.WriteByte("0123456789abcdef"[r>>4])
				out.WriteByte("0123456789abcdef"[r&0xf])
				continue
			}
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}

func oneOfText(values []any) string {
	var quoted []string
	for _, value := range values {
		if text, isString := value.(string); isString {
			quoted = append(quoted, "`"+text+"`")
		}
	}
	if len(quoted) == 0 {
		return ""
	}
	return "One of " + strings.Join(quoted, ", ") + "."
}
