#include "volume_service.h"

#include <atomic>

#include "audio_codec.h"
#include "esp_log.h"
#include "esp_memory_utils.h"
#include "nvs.h"
#include "waveshare_board.h"

namespace volume_service {
namespace {

constexpr const char* kTag = "VolumeService";
constexpr const char* kNvsNamespace = "audio";
constexpr const char* kLevelKey = "volume_level";
// Codec output volume (0-100) for each level. High matches the old fixed setting.
constexpr int kLevelVolume[] = {0, 40, 70, 100};

std::atomic<Level> s_level{Level::kHigh};

bool IsValid(uint8_t raw)
{
    return raw <= static_cast<uint8_t>(Level::kHigh);
}

void Apply(Level level)
{
    AudioCodec* codec = waveshare_board::GetAudioCodec();
    if (codec == nullptr) {
        ESP_LOGW(kTag, "No audio codec; volume %s not applied", LevelLabel(level));
        return;
    }
    if (level == Level::kOff) {
        codec->SetOutputMuted(true);
        return;
    }
    codec->SetOutputVolume(kLevelVolume[static_cast<size_t>(level)]);
    codec->SetOutputMuted(false);
}

void Save(Level level)
{
    int probe = 0;
    if (esp_ptr_external_ram(&probe)) {
        ESP_LOGW(kTag, "Volume not saved: caller stack is in PSRAM");
        return;
    }
    nvs_handle_t handle = 0;
    esp_err_t err = nvs_open(kNvsNamespace, NVS_READWRITE, &handle);
    if (err == ESP_OK) {
        err = nvs_set_u8(handle, kLevelKey, static_cast<uint8_t>(level));
        if (err == ESP_OK) {
            err = nvs_commit(handle);
        }
        nvs_close(handle);
    }
    if (err != ESP_OK) {
        ESP_LOGW(kTag, "Saving volume failed: %s", esp_err_to_name(err));
    }
}

}  // namespace

esp_err_t Init()
{
    Level level = Level::kHigh;
    nvs_handle_t handle = 0;
    if (nvs_open(kNvsNamespace, NVS_READONLY, &handle) == ESP_OK) {
        uint8_t raw = 0;
        if (nvs_get_u8(handle, kLevelKey, &raw) == ESP_OK && IsValid(raw)) {
            level = static_cast<Level>(raw);
        }
        nvs_close(handle);
    }
    s_level = level;
    Apply(level);
    ESP_LOGI(kTag, "Speaker volume: %s", LevelLabel(level));
    return ESP_OK;
}

Level GetLevel()
{
    return s_level.load();
}

void SetLevel(Level level)
{
    s_level = level;
    Apply(level);
    Save(level);
}

Level CycleLevel()
{
    const auto next = static_cast<Level>((static_cast<uint8_t>(GetLevel()) + 1) %
                                         (static_cast<uint8_t>(Level::kHigh) + 1));
    SetLevel(next);
    return next;
}

const char* LevelLabel(Level level)
{
    switch (level) {
        case Level::kOff:
            return "Off";
        case Level::kLow:
            return "Low";
        case Level::kMedium:
            return "Medium";
        case Level::kHigh:
        default:
            return "High";
    }
}

}  // namespace volume_service
