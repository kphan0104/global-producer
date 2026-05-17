package com.global.producer.service;

import java.util.ArrayList;
import java.util.List;
import java.util.random.RandomGenerator;
import org.springframework.stereotype.Service;

@Service
public class SimplePatternGenerator {

    public String generate(String pattern, RandomGenerator randomGenerator) {
        StringBuilder builder = new StringBuilder();
        for (PatternToken token : parse(pattern)) {
            for (int index = 0; index < token.repeat(); index++) {
                builder.append(token.nextCharacter(randomGenerator));
            }
        }
        return builder.toString();
    }

    public void validate(String pattern) {
        parse(pattern);
    }

    private List<PatternToken> parse(String pattern) {
        List<PatternToken> tokens = new ArrayList<>();
        int index = 0;

        while (index < pattern.length()) {
            char current = pattern.charAt(index);
            PatternToken token;

            if (current == '[') {
                int endIndex = pattern.indexOf(']', index + 1);
                if (endIndex < 0) {
                    throw new IllegalArgumentException("Unclosed character class in pattern: " + pattern);
                }
                List<Character> characters = expandCharacterClass(pattern.substring(index + 1, endIndex), pattern);
                token = new PatternToken(characters, 1);
                index = endIndex + 1;
            } else if (current == '\\') {
                if (index + 1 >= pattern.length()) {
                    throw new IllegalArgumentException("Trailing escape sequence in pattern: " + pattern);
                }
                token = new PatternToken(List.of(pattern.charAt(index + 1)), 1);
                index += 2;
            } else if (isUnsupportedMetaCharacter(current)) {
                throw new IllegalArgumentException(
                        "Unsupported pattern syntax '" + current + "' in pattern: " + pattern);
            } else {
                token = new PatternToken(List.of(current), 1);
                index++;
            }

            if (index < pattern.length() && pattern.charAt(index) == '{') {
                int endIndex = pattern.indexOf('}', index + 1);
                if (endIndex < 0) {
                    throw new IllegalArgumentException("Unclosed repetition block in pattern: " + pattern);
                }
                int repeat = parseRepeatCount(pattern.substring(index + 1, endIndex), pattern);
                token = new PatternToken(token.characters(), repeat);
                index = endIndex + 1;
            }

            tokens.add(token);
        }

        return tokens;
    }

    private List<Character> expandCharacterClass(String specification, String pattern) {
        List<Character> characters = new ArrayList<>();
        for (int index = 0; index < specification.length(); index++) {
            char current = specification.charAt(index);

            if (current == '\\') {
                if (index + 1 >= specification.length()) {
                    throw new IllegalArgumentException("Trailing escape sequence in character class: " + pattern);
                }
                characters.add(specification.charAt(index + 1));
                index++;
                continue;
            }

            if (index + 2 < specification.length() && specification.charAt(index + 1) == '-') {
                char rangeEnd = specification.charAt(index + 2);
                if (rangeEnd < current) {
                    throw new IllegalArgumentException("Invalid descending range in pattern: " + pattern);
                }
                for (char rangeCharacter = current; rangeCharacter <= rangeEnd; rangeCharacter++) {
                    characters.add(rangeCharacter);
                }
                index += 2;
                continue;
            }

            characters.add(current);
        }

        if (characters.isEmpty()) {
            throw new IllegalArgumentException("Empty character class in pattern: " + pattern);
        }

        return characters;
    }

    private int parseRepeatCount(String repeatText, String pattern) {
        try {
            int repeat = Integer.parseInt(repeatText);
            if (repeat <= 0) {
                throw new IllegalArgumentException("Repeat count must be > 0 in pattern: " + pattern);
            }
            return repeat;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Unsupported repetition syntax {" + repeatText + "} in pattern: " + pattern,
                    exception);
        }
    }

    private boolean isUnsupportedMetaCharacter(char character) {
        return character == '('
                || character == ')'
                || character == '|'
                || character == '?'
                || character == '+'
                || character == '*'
                || character == '.'
                || character == '^'
                || character == '$';
    }

    private record PatternToken(List<Character> characters, int repeat) {
        private char nextCharacter(RandomGenerator randomGenerator) {
            return characters.get(randomGenerator.nextInt(characters.size()));
        }
    }
}

