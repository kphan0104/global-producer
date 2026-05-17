package com.global.producer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Random;
import org.junit.jupiter.api.Test;

class SimplePatternGeneratorTest {

    private final SimplePatternGenerator generator = new SimplePatternGenerator();

    @Test
    void generateShouldSupportCharacterClassesAndRepetitions() {
        String generated = generator.generate("[A-Z]{3}[0-9]{5}", new Random(1));

        assertThat(generated).matches("[A-Z]{3}[0-9]{5}");
    }

    @Test
    void generateShouldKeepLiteralCharacters() {
        String generated = generator.generate("ID-[A-F0-9]{4}", new Random(2));

        assertThat(generated).startsWith("ID-").matches("ID-[A-F0-9]{4}");
    }

    @Test
    void validateShouldRejectUnsupportedRegexFeatures() {
        assertThatThrownBy(() -> generator.validate("foo.*bar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported pattern syntax");
    }

    @Test
    void validateShouldRejectInvalidRanges() {
        assertThatThrownBy(() -> generator.validate("[Z-A]{3}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid descending range");
    }

    @Test
    void validateShouldRejectUnsupportedRepetitionSyntax() {
        assertThatThrownBy(() -> generator.validate("[0-9]{1,3}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported repetition syntax");
    }
}

