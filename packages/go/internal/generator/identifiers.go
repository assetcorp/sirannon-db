package main

import (
	"go/token"
	"strings"
	"unicode"
)

var initialisms = map[string]string{
	"api":  "API",
	"cpu":  "CPU",
	"db":   "DB",
	"hlc":  "HLC",
	"http": "HTTP",
	"id":   "ID",
	"ids":  "IDs",
	"ip":   "IP",
	"json": "JSON",
	"ms":   "Ms",
	"sql":  "SQL",
	"tcp":  "TCP",
	"tls":  "TLS",
	"ttl":  "TTL",
	"uri":  "URI",
	"url":  "URL",
	"urls": "URLs",
	"uuid": "UUID",
	"ws":   "WS",
}

var reservedArguments = map[string]bool{
	"answer": true, "body": true, "ctx": true, "err": true, "exchange": true, "o": true, "options": true,
	"params": true, "payload": true, "query": true, "request": true, "result": true,
}

func splitWords(text string) []string {
	var words []string
	var current []rune
	runes := []rune(text)
	flush := func() {
		if len(current) > 0 {
			words = append(words, string(current))
			current = nil
		}
	}
	for position, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			previous := current[len(current)-1]
			nextIsLower := position+1 < len(runes) && unicode.IsLower(runes[position+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextIsLower) {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	return words
}

func capitalise(word string) string {
	if fixed, known := initialisms[strings.ToLower(word)]; known {
		return fixed
	}
	runes := []rune(strings.ToLower(word))
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func pascalWords(text string) string {
	var built strings.Builder
	for _, word := range splitWords(text) {
		built.WriteString(capitalise(word))
	}
	return built.String()
}

func exportedName(text string) string {
	name := pascalWords(text)
	if name == "" || !token.IsIdentifier(name) || !token.IsExported(name) {
		failf("The name %q gives no exported Go identifier", text)
	}
	return name
}

func argumentName(text string) string {
	words := splitWords(text)
	if len(words) == 0 {
		failf("The parameter name %q gives no Go identifier", text)
	}
	var built strings.Builder
	built.WriteString(strings.ToLower(words[0]))
	for _, word := range words[1:] {
		built.WriteString(capitalise(word))
	}
	name := built.String()
	if token.IsKeyword(name) || reservedArguments[name] || !token.IsIdentifier(name) {
		name += "Value"
	}
	return name
}

func constantName(typeName string, value string) string {
	suffix := pascalWords(strings.ReplaceAll(value, "[]", " array "))
	if suffix == "" {
		failf("The value %q of %s gives no Go constant name", value, typeName)
	}
	name := typeName + suffix
	if !token.IsIdentifier(name) {
		failf("The value %q of %s gives no Go constant name", value, typeName)
	}
	return name
}

var fileNameConstraintWords = map[string]bool{
	"test": true, "aix": true, "android": true, "darwin": true, "dragonfly": true, "freebsd": true, "hurd": true,
	"illumos": true, "ios": true, "js": true, "linux": true, "nacl": true, "netbsd": true, "openbsd": true,
	"plan9": true, "solaris": true, "wasip1": true, "windows": true, "zos": true,
	"386": true, "amd64": true, "amd64p32": true, "arm": true, "armbe": true, "arm64": true, "arm64be": true,
	"loong64": true, "mips": true, "mipsle": true, "mips64": true, "mips64le": true, "mips64p32": true,
	"mips64p32le": true, "ppc": true, "ppc64": true, "ppc64le": true, "riscv": true, "riscv64": true,
	"s390": true, "s390x": true, "sparc": true, "sparc64": true, "wasm": true,
}

func fileStem(tag string) string {
	words := splitWords(tag)
	for position, word := range words {
		words[position] = strings.ToLower(word)
	}
	if len(words) == 0 {
		failf("The tag %q gives no Go file name", tag)
	}
	if fileNameConstraintWords[words[len(words)-1]] {
		words = append(words, "routes")
	}
	return strings.Join(words, "_")
}
