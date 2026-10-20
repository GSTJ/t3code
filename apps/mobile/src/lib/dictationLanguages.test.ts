import { describe, expect, it } from "vite-plus/test";

import { dictationLanguageLabel, isDictationLanguage } from "./dictationLanguages";

describe("isDictationLanguage", () => {
  it("accepts listed ids and rejects anything else", () => {
    expect(isDictationLanguage("pt-BR")).toBe(true);
    expect(isDictationLanguage("pt")).toBe(false);
    expect(isDictationLanguage("xx-XX")).toBe(false);
    expect(isDictationLanguage(42)).toBe(false);
  });
});

describe("dictationLanguageLabel", () => {
  it("names the chosen language and falls back to the device language", () => {
    expect(dictationLanguageLabel("pt-BR")).toBe("Português (Brasil)");
    expect(dictationLanguageLabel(undefined)).toBe("Device language");
  });
});
