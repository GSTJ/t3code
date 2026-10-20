// Every id is in SpeechTranscriber.supportedLocales on iOS 26. Labels are endonyms written out
// because Hermes's Intl.DisplayNames support is not guaranteed.
export const DICTATION_LANGUAGES: ReadonlyArray<{ readonly id: string; readonly label: string }> = [
  { id: "de-DE", label: "Deutsch (Deutschland)" },
  { id: "en-AU", label: "English (Australia)" },
  { id: "en-CA", label: "English (Canada)" },
  { id: "en-IN", label: "English (India)" },
  { id: "en-GB", label: "English (UK)" },
  { id: "en-US", label: "English (US)" },
  { id: "es-ES", label: "Español (España)" },
  { id: "es-US", label: "Español (Estados Unidos)" },
  { id: "es-MX", label: "Español (México)" },
  { id: "fr-CA", label: "Français (Canada)" },
  { id: "fr-FR", label: "Français (France)" },
  { id: "it-IT", label: "Italiano (Italia)" },
  { id: "pt-BR", label: "Português (Brasil)" },
  { id: "pt-PT", label: "Português (Portugal)" },
  { id: "hi-IN", label: "हिन्दी (भारत)" },
  { id: "ko-KR", label: "한국어(대한민국)" },
  { id: "zh-CN", label: "中文（中国大陆）" },
  { id: "zh-HK", label: "中文（香港）" },
  { id: "zh-TW", label: "中文（台灣）" },
  { id: "ja-JP", label: "日本語 (日本)" },
  { id: "yue-CN", label: "粤语（中国大陆）" },
];

export const DEVICE_DICTATION_LANGUAGE_LABEL = "Device language";

export function isDictationLanguage(value: unknown): value is string {
  return DICTATION_LANGUAGES.some((language) => language.id === value);
}

export function dictationLanguageLabel(id: string | undefined): string {
  return (
    DICTATION_LANGUAGES.find((language) => language.id === id)?.label ??
    DEVICE_DICTATION_LANGUAGE_LABEL
  );
}
