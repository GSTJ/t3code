import { ScreenScrollView as ScrollView } from "../../components/ui/ScreenScrollView";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { DEVICE_DICTATION_LANGUAGE_LABEL, DICTATION_LANGUAGES } from "../../lib/dictationLanguages";
import { mobilePreferencesAtom, updateMobilePreferencesAtom } from "../../state/preferences";
import { SettingsChoiceRow } from "./components/SettingsChoiceRow";
import { SettingsSection } from "./components/SettingsSection";

const OPTIONS = [{ id: null, label: DEVICE_DICTATION_LANGUAGE_LABEL }, ...DICTATION_LANGUAGES];

export function SettingsDictationLanguageRouteScreen() {
  const insets = useSafeAreaInsets();
  const preferencesResult = useAtomValue(mobilePreferencesAtom);
  const savePreferences = useAtomSet(updateMobilePreferencesAtom);
  const preferencesReady = AsyncResult.isSuccess(preferencesResult) && !preferencesResult.waiting;
  const selectedId = AsyncResult.isSuccess(preferencesResult)
    ? (preferencesResult.value.voiceInputLocale ?? null)
    : undefined;

  return (
    <View collapsable={false} className="flex-1 bg-sheet">
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        showsVerticalScrollIndicator={false}
        className="flex-1"
        contentContainerClassName="gap-3 px-5 pt-4"
        contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 18) + 18 }}
      >
        <SettingsSection footer="The language you speak when dictating.">
          {OPTIONS.map((option, index) => (
            <SettingsChoiceRow
              key={option.id ?? "device"}
              label={option.label}
              selected={selectedId === option.id}
              separated={index > 0}
              disabled={!preferencesReady}
              onPress={() => savePreferences({ voiceInputLocale: option.id })}
            />
          ))}
        </SettingsSection>
      </ScrollView>
    </View>
  );
}
