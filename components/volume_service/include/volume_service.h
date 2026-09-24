#pragma once

#include <cstdint>

#include "esp_err.h"

// Speaker volume setting: four levels, saved in NVS and applied to the ES8311 codec.
// Off also mutes the codec and switches the speaker amplifier off.
namespace volume_service {

enum class Level : uint8_t {
    kOff = 0,
    kLow = 1,
    kMedium = 2,
    kHigh = 3,
};

// Loads the saved level (High, the previous fixed volume, if none) and applies it.
// Call once at boot from a task with an internal-RAM stack.
esp_err_t Init();

Level GetLevel();

// Applies and saves a level. Saving is skipped (the level still applies) when called from a
// task whose stack is in PSRAM, since NVS writes need an internal-RAM stack.
void SetLevel(Level level);

// Off -> Low -> Medium -> High -> Off. Returns the new level.
Level CycleLevel();

const char* LevelLabel(Level level);

}  // namespace volume_service
