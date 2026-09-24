#include "wifi_service.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iterator>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "cJSON.h"
#include "config_api.h"
#include "config_api_http.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_mac.h"
#include "esp_netif.h"
#include "esp_random.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "followup_task_config.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include "lwip/sockets.h"
#include "nvs.h"
#include "sdkconfig.h"

namespace wifi_service {
namespace {

constexpr const char* kTag = "WifiService";
constexpr const char* kNvsNamespace = "wifi";
constexpr const char* kSsidKey = "ssid";
constexpr const char* kPasswordKey = "password";
constexpr const char* kApUrl = "http://192.168.4.1";
constexpr int kConnectTimeoutSec = 60;
// Consecutive fast reconnect attempts before the loop drops to background retries. Every
// attempt is a full radio stop/start/connect cycle, so out of range a fast loop would churn
// the radio -- and the battery -- without ever succeeding.
constexpr int kMaxReconnectAttempts = 10;
// Background retry delays once the fast attempts are used up; the last value repeats. Each
// retry tries the next saved network, so a device carried between known networks finds
// whichever is in range. Only an explicit disconnect or turning Wi-Fi off stops retrying.
constexpr uint32_t kBackoffDelaysSec[] = {30, 60, 120, 300};
// Saved networks, most recently used first, stored as one NVS blob. The legacy single
// "ssid"/"password" keys are migrated on first load and kept in sync with the most recent
// network so an older firmware still finds a network after a downgrade.
constexpr const char* kNetworksKey = "networks";
// Setup-network (AP) WPA2 password: generated once per device, shown on screen when setup
// mode starts. Unambiguous characters only (no 0/o, 1/l/i), so it is easy to read and type.
constexpr const char* kApPasswordKey = "ap_password";
constexpr size_t kApPasswordLength = 10;
constexpr char kApPasswordAlphabet[] = "abcdefghjkmnpqrstuvwxyz23456789";
constexpr uint8_t kNetworksBlobVersion = 1;
constexpr size_t kMaxSavedNetworks = 5;
// Safety net for a scan that never reports. esp_wifi_stop() and esp_wifi_connect() both
// abort a running scan without delivering WIFI_EVENT_SCAN_DONE, and without this the
// snapshot would latch at kRunning and silently swallow every later scan request.
constexpr int64_t kScanTimeoutUs = 12 * 1000 * 1000;
// esp_wifi_scan_start() is rejected with ESP_ERR_WIFI_STATE while the station is still
// tearing down an association, so give the driver a few short beats to settle.
constexpr int kScanStartAttempts = 5;
constexpr uint32_t kScanStartRetryDelayMs = 100;
constexpr uint32_t kTransitionTaskStackWords = 8192;
constexpr UBaseType_t kTransitionQueueDepth = 4;
constexpr uint32_t kCallbackTaskStackWords = 6144;
constexpr size_t kMaxPendingCallbacks = 16;
constexpr const char* kPortalIndexJsUri = "/index.js";
constexpr const char* kPortalIndexCssUri = "/index.css";

// Captive-portal frontend, embedded from components/wifi_service/portal/ (built by the Vite app in
// //webserver; run `npm run build` there and copy dist/* into portal/). CMakeLists.txt gzips each
// file at build time and embeds the result, generating _binary_<basename>_gz_{start,end} symbols;
// the asm labels bind these declarations to them.
extern const uint8_t kIndexHtmlStart[] asm("_binary_index_html_gz_start");
extern const uint8_t kIndexHtmlEnd[] asm("_binary_index_html_gz_end");
extern const uint8_t kIndexJsStart[] asm("_binary_index_js_gz_start");
extern const uint8_t kIndexJsEnd[] asm("_binary_index_js_gz_end");
extern const uint8_t kIndexCssStart[] asm("_binary_index_css_gz_start");
extern const uint8_t kIndexCssEnd[] asm("_binary_index_css_gz_end");

enum class TransitionRequest : uint8_t {
    kStart,
    kStartStation,
    kEnterAccessPoint,
    kDisableAccessPoint,
    kDisconnectStation,
    kStopWifi,
    kStartScan,
    kForgetNetwork,
};

struct Credentials {
    std::string ssid;
    std::string password;

    bool valid() const { return !ssid.empty(); }
};

bool s_initialized = false;
bool s_stack_initialized = false;
bool s_wifi_enabled = true;
bool s_connected = false;
bool s_access_point_mode = false;
bool s_suppress_disconnect_event = false;
bool s_connect_timer_active = false;
bool s_reconnecting = false;
// Consecutive failed automatic reconnects, and whether the loop has stood down. Reset by
// anything that counts as user intent (connect, scan, Wi-Fi toggle) or a successful
// association; see kMaxReconnectAttempts.
int s_reconnect_attempts = 0;
bool s_reconnect_suspended = false;
bool s_persist_active_credentials_on_success = false;
bool s_clear_saved_credentials_on_disconnect = true;
int s_rssi = 0;
std::string s_current_ssid;
std::string s_ip_address;
std::string s_ap_ssid;
std::string s_ap_password;
std::string s_ap_url = kApUrl;
Credentials s_saved_credentials;  // most recently used saved network (s_saved_networks[0])
Credentials s_active_credentials;
std::vector<Credentials> s_saved_networks;  // most recently used first
// Set by an explicit disconnect; only user intent (connect, toggle) clears it. Distinct from
// s_reconnect_suspended, which a scan also sets temporarily.
bool s_user_disconnected = false;
// A scan paused the reconnect loop; resume (joining the best saved network the scan found)
// once it finishes.
bool s_resume_after_scan = false;
size_t s_backoff_step = 0;
size_t s_rotation_index = 0;
std::string s_forget_ssid;
ScanSnapshot s_scan_snapshot = {};
std::mutex s_state_mutex;
std::mutex s_callback_mutex;
std::deque<Event> s_pending_events;
EventHandler s_event_handler = nullptr;
void* s_event_context = nullptr;
ScanDeferProvider s_scan_defer_provider = nullptr;
void* s_scan_defer_context = nullptr;
constexpr int kScanDeferPollMs = 20;
constexpr int kScanDeferMaxMs = 4000;
// A scan queued by page entry is requested just before the screen-change refresh is, so
// the refresh may not have started yet when we first look. Give it this long to appear
// before deciding there is nothing to wait for.
constexpr int kScanDeferStartMs = 400;
// Let the panel finish settling after the refresh reports done before the radio goes on
// air, rather than starting the instant the last transaction completes.
constexpr int kScanDeferSettleMs = 150;
QueueHandle_t s_transition_queue = nullptr;
TaskHandle_t s_transition_task = nullptr;
TaskHandle_t s_callback_task = nullptr;
esp_timer_handle_t s_connect_timer = nullptr;
esp_timer_handle_t s_scan_timeout_timer = nullptr;
esp_timer_handle_t s_retry_timer = nullptr;
esp_netif_t* s_sta_netif = nullptr;
esp_netif_t* s_ap_netif = nullptr;
httpd_handle_t s_portal_server = nullptr;
esp_event_handler_instance_t s_wifi_event_handler = nullptr;
esp_event_handler_instance_t s_ip_event_handler = nullptr;

void CheckOrAbort(esp_err_t err, const char* operation)
{
    if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
        ESP_LOGE(kTag, "%s failed: %s", operation, esp_err_to_name(err));
        ESP_ERROR_CHECK(err);
    }
}

UiState BuildUiStateLocked()
{
    return UiState{
        .wifi_enabled = s_wifi_enabled,
        .connected = s_connected,
        .access_point_mode = s_access_point_mode,
        .reconnecting = s_reconnecting,
        .has_saved_credentials = s_saved_credentials.valid(),
        .ssid = s_current_ssid,
        .ip_address = s_ip_address,
        .ap_ssid = s_ap_ssid,
        .ap_password = s_ap_password,
        .ap_url = s_ap_url,
        .rssi = s_rssi,
    };
}

void CallbackTask(void*)
{
    while (true) {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY);

        while (true) {
            Event event = {};
            EventHandler handler = nullptr;
            void* context = nullptr;
            {
                std::lock_guard<std::mutex> lock(s_callback_mutex);
                if (s_pending_events.empty()) {
                    break;
                }
                event = std::move(s_pending_events.front());
                s_pending_events.pop_front();
                handler = s_event_handler;
                context = s_event_context;
            }
            if (handler != nullptr) {
                handler(event, context);
            }
        }
    }
}

void Notify(State state, const std::string& detail = {})
{
    if (s_callback_task == nullptr) {
        return;
    }

    Event event = {};
    event.state = state;
    event.detail = detail;
    {
        std::lock_guard<std::mutex> state_lock(s_state_mutex);
        event.ui_state = BuildUiStateLocked();
    }

    {
        std::lock_guard<std::mutex> lock(s_callback_mutex);
        if (s_pending_events.size() >= kMaxPendingCallbacks) {
            s_pending_events.pop_front();
        }
        s_pending_events.push_back(std::move(event));
    }
    xTaskNotifyGive(s_callback_task);
}

std::string BuildApSsid()
{
    uint8_t mac[6] = {};
    ESP_ERROR_CHECK(esp_read_mac(mac, ESP_MAC_WIFI_STA));

    char ssid[64] = {};
    std::snprintf(ssid, sizeof(ssid), "%s-%02X%02X%02X",
                  CONFIG_FOLLOWUP_WIFI_AP_PREFIX,
                  mac[3],
                  mac[4],
                  mac[5]);
    return ssid;
}

std::string IpInfoToUrl(esp_netif_t* netif)
{
    if (netif == nullptr) {
        return kApUrl;
    }

    esp_netif_ip_info_t ip_info = {};
    if (esp_netif_get_ip_info(netif, &ip_info) != ESP_OK) {
        return kApUrl;
    }

    char url[32] = {};
    std::snprintf(url, sizeof(url), "http://%u.%u.%u.%u", IP2STR(&ip_info.ip));
    return url;
}

std::string DisconnectReasonToString(uint8_t reason)
{
    switch (static_cast<wifi_err_reason_t>(reason)) {
        case WIFI_REASON_AUTH_FAIL:
            return "AUTH FAILED";
        case WIFI_REASON_NO_AP_FOUND:
            return "AP NOT FOUND";
        case WIFI_REASON_HANDSHAKE_TIMEOUT:
            return "HANDSHAKE TIMEOUT";
        case WIFI_REASON_ASSOC_FAIL:
            return "ASSOC FAILED";
        case WIFI_REASON_BEACON_TIMEOUT:
            return "BEACON TIMEOUT";
        default:
            break;
    }

    char detail[24] = {};
    std::snprintf(detail, sizeof(detail), "REASON %d", reason);
    return detail;
}

void ConfigureAccessPointConfig(const std::string& ap_ssid, wifi_config_t* config)
{
    if (config == nullptr) {
        return;
    }

    std::string password;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        password = s_ap_password;
    }

    *config = {};
    strlcpy(reinterpret_cast<char*>(config->ap.ssid), ap_ssid.c_str(), sizeof(config->ap.ssid));
    config->ap.ssid_len = ap_ssid.size();
    config->ap.channel = 1;
    config->ap.max_connection = 4;
    config->ap.pmf_cfg.required = false;
    if (password.size() >= 8) {
        strlcpy(reinterpret_cast<char*>(config->ap.password), password.c_str(),
                sizeof(config->ap.password));
        config->ap.authmode = WIFI_AUTH_WPA2_PSK;
    } else {
        // EnsureAccessPointPassword runs before every AP start, so this should not happen.
        ESP_LOGE(kTag, "No setup-network password; starting the setup network open");
        config->ap.authmode = WIFI_AUTH_OPEN;
    }
}

void ConfigureStationConfig(const Credentials& credentials, wifi_config_t* config)
{
    if (config == nullptr) {
        return;
    }

    *config = {};
    strlcpy(reinterpret_cast<char*>(config->sta.ssid), credentials.ssid.c_str(),
            sizeof(config->sta.ssid));
    strlcpy(reinterpret_cast<char*>(config->sta.password), credentials.password.c_str(),
            sizeof(config->sta.password));
    config->sta.scan_method = WIFI_ALL_CHANNEL_SCAN;
    config->sta.sort_method = WIFI_CONNECT_AP_BY_SIGNAL;
    config->sta.failure_retry_cnt = 0;
    config->sta.pmf_cfg.capable = true;
    config->sta.pmf_cfg.required = false;
}

bool LoadString(nvs_handle_t handle, const char* key, std::string* out)
{
    if (out == nullptr) {
        return false;
    }

    size_t size = 0;
    esp_err_t err = nvs_get_str(handle, key, nullptr, &size);
    if (err != ESP_OK || size <= 1) {
        out->clear();
        return false;
    }

    std::string value(size, '\0');
    err = nvs_get_str(handle, key, value.data(), &size);
    if (err != ESP_OK) {
        out->clear();
        return false;
    }
    if (!value.empty() && value.back() == '\0') {
        value.pop_back();
    }
    *out = std::move(value);
    return true;
}

struct SavedNetworkRecord {
    char ssid[33];
    char password[65];
};

std::vector<Credentials> LoadSavedNetworksFromNvs(nvs_handle_t handle)
{
    std::vector<Credentials> networks;
    size_t size = 0;
    if (nvs_get_blob(handle, kNetworksKey, nullptr, &size) == ESP_OK && size >= 2) {
        std::vector<uint8_t> blob(size);
        if (nvs_get_blob(handle, kNetworksKey, blob.data(), &size) == ESP_OK &&
            blob[0] == kNetworksBlobVersion) {
            const size_t count = blob[1];
            if (size == 2 + count * sizeof(SavedNetworkRecord)) {
                for (size_t index = 0; index < count && index < kMaxSavedNetworks; ++index) {
                    SavedNetworkRecord record = {};
                    std::memcpy(&record, blob.data() + 2 + index * sizeof(record),
                                sizeof(record));
                    record.ssid[sizeof(record.ssid) - 1] = '\0';
                    record.password[sizeof(record.password) - 1] = '\0';
                    Credentials credentials = {.ssid = record.ssid,
                                               .password = record.password};
                    if (credentials.valid()) {
                        networks.push_back(std::move(credentials));
                    }
                }
                return networks;
            }
        }
        ESP_LOGW(kTag, "Ignoring unreadable saved-networks blob (%u bytes)",
                 static_cast<unsigned>(size));
    }

    // Migration: firmware before the saved-networks list stored one network in two keys.
    Credentials legacy = {};
    LoadString(handle, kSsidKey, &legacy.ssid);
    LoadString(handle, kPasswordKey, &legacy.password);
    if (legacy.valid()) {
        networks.push_back(std::move(legacy));
    }
    return networks;
}

bool WriteSavedNetworksToNvs(const std::vector<Credentials>& networks)
{
    std::vector<uint8_t> blob(2 + networks.size() * sizeof(SavedNetworkRecord), 0);
    blob[0] = kNetworksBlobVersion;
    blob[1] = static_cast<uint8_t>(networks.size());
    for (size_t index = 0; index < networks.size(); ++index) {
        SavedNetworkRecord record = {};
        std::strncpy(record.ssid, networks[index].ssid.c_str(), sizeof(record.ssid) - 1);
        std::strncpy(record.password, networks[index].password.c_str(),
                     sizeof(record.password) - 1);
        std::memcpy(blob.data() + 2 + index * sizeof(record), &record, sizeof(record));
    }

    nvs_handle_t handle = 0;
    esp_err_t err = nvs_open(kNvsNamespace, NVS_READWRITE, &handle);
    if (err != ESP_OK) {
        ESP_LOGE(kTag, "Failed to open NVS for Wi-Fi networks: %s", esp_err_to_name(err));
        return false;
    }
    err = nvs_set_blob(handle, kNetworksKey, blob.data(), blob.size());
    // Keep the legacy keys mirroring the most recent network (see kNetworksKey).
    if (err == ESP_OK && !networks.empty()) {
        err = nvs_set_str(handle, kSsidKey, networks.front().ssid.c_str());
        if (err == ESP_OK) {
            err = nvs_set_str(handle, kPasswordKey, networks.front().password.c_str());
        }
    } else if (err == ESP_OK) {
        for (const char* key : {kSsidKey, kPasswordKey}) {
            const esp_err_t erase_err = nvs_erase_key(handle, key);
            if (erase_err != ESP_OK && erase_err != ESP_ERR_NVS_NOT_FOUND) {
                err = erase_err;
            }
        }
    }
    if (err == ESP_OK) {
        err = nvs_commit(handle);
    }
    nvs_close(handle);

    if (err != ESP_OK) {
        ESP_LOGE(kTag, "Failed to save Wi-Fi networks: %s", esp_err_to_name(err));
        return false;
    }
    return true;
}

// Caller holds s_state_mutex.
void SetSavedNetworksLocked(std::vector<Credentials> networks)
{
    s_saved_networks = std::move(networks);
    s_saved_credentials = s_saved_networks.empty() ? Credentials{} : s_saved_networks.front();
    if (s_rotation_index >= s_saved_networks.size()) {
        s_rotation_index = 0;
    }
}

const Credentials* FindSavedNetworkLocked(const std::string& ssid)
{
    for (const Credentials& network : s_saved_networks) {
        if (network.ssid == ssid) {
            return &network;
        }
    }
    return nullptr;
}

void ReloadSavedCredentials()
{
    std::vector<Credentials> networks;
    nvs_handle_t handle = 0;
    if (nvs_open(kNvsNamespace, NVS_READONLY, &handle) == ESP_OK) {
        networks = LoadSavedNetworksFromNvs(handle);
        nvs_close(handle);
    }

    if (networks.empty()) {
        Credentials built_in = {.ssid = CONFIG_FOLLOWUP_WIFI_STA_SSID,
                                .password = CONFIG_FOLLOWUP_WIFI_STA_PASSWORD};
        if (built_in.valid()) {
            networks.push_back(std::move(built_in));
        }
    }

    std::lock_guard<std::mutex> lock(s_state_mutex);
    SetSavedNetworksLocked(std::move(networks));
    if (!s_active_credentials.valid()) {
        s_active_credentials = s_saved_credentials;
    }
}

// Records a network that just connected as the most recently used, keeping at most
// kMaxSavedNetworks. Runs on the default event loop task (internal-RAM stack, as NVS writes
// require).
bool SaveCredentials(const std::string& ssid, const std::string& password)
{
    std::vector<Credentials> networks;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        networks = s_saved_networks;
    }
    networks.erase(std::remove_if(networks.begin(), networks.end(),
                                  [&](const Credentials& network) {
                                      return network.ssid == ssid;
                                  }),
                   networks.end());
    networks.insert(networks.begin(), Credentials{.ssid = ssid, .password = password});
    if (networks.size() > kMaxSavedNetworks) {
        networks.resize(kMaxSavedNetworks);
    }

    if (!WriteSavedNetworksToNvs(networks)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(s_state_mutex);
    SetSavedNetworksLocked(std::move(networks));
    s_rotation_index = 0;
    return true;
}

// Removes one saved network (all of them when ssid is empty). Runs on the transition worker.
bool ForgetSavedNetworkNow(const std::string& ssid)
{
    std::vector<Credentials> networks;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        networks = s_saved_networks;
    }
    if (ssid.empty()) {
        networks.clear();
    } else {
        networks.erase(std::remove_if(networks.begin(), networks.end(),
                                      [&](const Credentials& network) {
                                          return network.ssid == ssid;
                                      }),
                       networks.end());
    }

    if (!WriteSavedNetworksToNvs(networks)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(s_state_mutex);
    SetSavedNetworksLocked(std::move(networks));
    if (ssid.empty() || s_active_credentials.ssid == ssid) {
        s_active_credentials = {};
    }
    return true;
}

bool ClearCredentialsFromNvs()
{
    return ForgetSavedNetworkNow({});
}

Credentials ResolveStationCredentialsLocked()
{
    return s_active_credentials.valid() ? s_active_credentials : s_saved_credentials;
}

void LoadAccessPointPassword()
{
    std::string password;
    nvs_handle_t handle = 0;
    if (nvs_open(kNvsNamespace, NVS_READONLY, &handle) == ESP_OK) {
        (void)LoadString(handle, kApPasswordKey, &password);
        nvs_close(handle);
    }
    std::lock_guard<std::mutex> lock(s_state_mutex);
    s_ap_password = std::move(password);
}

// Creates the setup-network password on first use. Runs on the transition worker (NVS writes
// need its internal-RAM stack). The RNG gives true random numbers only while the radio is on
// (esp_random.h), so the radio is started first if it is not running yet.
void EnsureAccessPointPassword()
{
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (s_ap_password.size() >= 8) {
            return;
        }
    }

    const esp_err_t start_err = esp_wifi_start();
    if (start_err != ESP_OK) {
        ESP_LOGW(kTag, "Radio start for password entropy failed: %s",
                 esp_err_to_name(start_err));
    }

    constexpr size_t kAlphabetSize = sizeof(kApPasswordAlphabet) - 1;
    // Largest multiple of the alphabet size that fits in a byte, to avoid modulo bias.
    constexpr uint8_t kRejectFrom = static_cast<uint8_t>((256 / kAlphabetSize) * kAlphabetSize);
    std::string password;
    while (password.size() < kApPasswordLength) {
        uint8_t byte = 0;
        esp_fill_random(&byte, sizeof(byte));
        if (byte < kRejectFrom) {
            password.push_back(kApPasswordAlphabet[byte % kAlphabetSize]);
        }
    }

    nvs_handle_t handle = 0;
    esp_err_t err = nvs_open(kNvsNamespace, NVS_READWRITE, &handle);
    if (err == ESP_OK) {
        err = nvs_set_str(handle, kApPasswordKey, password.c_str());
        if (err == ESP_OK) {
            err = nvs_commit(handle);
        }
        nvs_close(handle);
    }
    if (err != ESP_OK) {
        // Still use it this session; a new one is generated after the next reboot.
        ESP_LOGW(kTag, "Saving setup-network password failed: %s", esp_err_to_name(err));
    }

    std::lock_guard<std::mutex> lock(s_state_mutex);
    s_ap_password = std::move(password);
    ESP_LOGI(kTag, "Setup-network password created");
}

void UpdateAccessPointIdentity()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    s_ap_ssid = BuildApSsid();
    s_ap_url = kApUrl;
}

void StopCaptiveDns();  // defined below, near the captive-portal handlers

void StopConfigPortal()
{
    StopCaptiveDns();
    if (s_portal_server == nullptr) {
        return;
    }
    httpd_stop(s_portal_server);
    s_portal_server = nullptr;
}

const char* AuthModeToString(wifi_auth_mode_t auth_mode)
{
    switch (auth_mode) {
        case WIFI_AUTH_OPEN:
            return "OPEN";
        case WIFI_AUTH_WEP:
            return "WEP";
        case WIFI_AUTH_WPA_PSK:
            return "WPA-PSK";
        case WIFI_AUTH_WPA2_PSK:
            return "WPA2-PSK";
        case WIFI_AUTH_WPA_WPA2_PSK:
            return "WPA/WPA2-PSK";
        case WIFI_AUTH_WPA3_PSK:
            return "WPA3-PSK";
        case WIFI_AUTH_WPA2_WPA3_PSK:
            return "WPA2/WPA3-PSK";
        default:
            return "UNKNOWN";
    }
}

config_api::JsonPtr BuildStatusJson(const UiState& ui_state, const ScanSnapshot* scan_snapshot)
{
    config_api::JsonPtr json(cJSON_CreateObject());
    cJSON* root = json.get();
    cJSON_AddBoolToObject(root, "wifi_enabled", ui_state.wifi_enabled);
    cJSON_AddBoolToObject(root, "connected", ui_state.connected);
    cJSON_AddBoolToObject(root, "access_point_mode", ui_state.access_point_mode);
    cJSON_AddBoolToObject(root, "has_saved_credentials", ui_state.has_saved_credentials);
    cJSON_AddStringToObject(root, "ssid", ui_state.ssid.c_str());
    cJSON_AddNumberToObject(root, "rssi", ui_state.rssi);
    cJSON_AddStringToObject(root, "ip_address", ui_state.ip_address.c_str());
    cJSON_AddStringToObject(root, "ap_ssid", ui_state.ap_ssid.c_str());
    cJSON_AddStringToObject(root, "ap_url", ui_state.ap_url.c_str());
    if (scan_snapshot != nullptr) {
        cJSON_AddBoolToObject(root, "scan_in_progress",
                              scan_snapshot->state == ScanState::kRunning);
    }

    cJSON* networks = cJSON_AddArrayToObject(root, "networks");
    if (scan_snapshot != nullptr) {
        for (const ScannedNetwork& network : scan_snapshot->networks) {
            cJSON* item = cJSON_CreateObject();
            cJSON_AddStringToObject(item, "ssid", network.ssid.c_str());
            cJSON_AddNumberToObject(item, "rssi", network.rssi);
            cJSON_AddNumberToObject(item, "encryption_type",
                                    static_cast<int>(network.auth_mode));
            cJSON_AddBoolToObject(item, "is_open", network.IsOpen());
            cJSON_AddStringToObject(item, "security", AuthModeToString(network.auth_mode));
            cJSON_AddItemToArray(networks, item);
        }
    }

    return json;
}

esp_err_t RegisterRoute(httpd_handle_t server, const httpd_uri_t* handler)
{
    if (server == nullptr || handler == nullptr) {
        return ESP_ERR_INVALID_ARG;
    }
    const esp_err_t err = httpd_register_uri_handler(server, handler);
    if (err != ESP_OK) {
        ESP_LOGE(kTag, "Failed to register Wi-Fi route %s [%d]: %s",
                 handler->uri != nullptr ? handler->uri : "<null>",
                 static_cast<int>(handler->method),
                 esp_err_to_name(err));
    }
    return err;
}

// --- Captive portal --------------------------------------------------------------------------
// A minimal DNS server answers every query with the AP IP (192.168.4.1), and an httpd 404 handler
// 302-redirects any unrecognised request (OS connectivity probes, unknown hosts) to the portal.
// Together these make phones/laptops auto-pop the "Sign in to network" browser on join, instead of
// the user having to type the IP.

constexpr uint16_t kCaptiveDnsPort = 53;
constexpr const char* kApRedirectUrl = "http://192.168.4.1/";

TaskHandle_t s_dns_task = nullptr;
int s_dns_socket = -1;
std::atomic<bool> s_dns_stop{false};

void CaptiveDnsTask(void*)
{
    uint8_t rx[512];
    uint8_t tx[512];
    while (!s_dns_stop.load(std::memory_order_relaxed)) {
        sockaddr_in client = {};
        socklen_t client_len = sizeof(client);
        const int len = recvfrom(s_dns_socket, rx, sizeof(rx), 0,
                                 reinterpret_cast<sockaddr*>(&client), &client_len);
        if (len < 12) {
            continue;  // recv timeout (SO_RCVTIMEO) or a malformed/too-short query
        }

        // Walk the first question's QNAME so we know where to append the answer record.
        int question_end = 12;
        while (question_end < len && rx[question_end] != 0) {
            question_end += rx[question_end] + 1;
        }
        question_end += 1 + 4;  // terminating zero label + QTYPE + QCLASS
        if (question_end > len || question_end + 16 > static_cast<int>(sizeof(tx))) {
            continue;
        }

        memcpy(tx, rx, question_end);  // reuse the request header + question
        tx[2] = 0x81;                  // QR=1, opcode=0, RD=1
        tx[3] = 0x80;                  // RA=1, RCODE=0
        tx[6] = 0x00;
        tx[7] = 0x01;  // ANCOUNT = 1
        tx[8] = 0x00;
        tx[9] = 0x00;  // NSCOUNT = 0
        tx[10] = 0x00;
        tx[11] = 0x00;  // ARCOUNT = 0

        int p = question_end;
        tx[p++] = 0xC0;  // NAME = pointer...
        tx[p++] = 0x0C;  // ...to offset 12 (the question name)
        tx[p++] = 0x00;
        tx[p++] = 0x01;  // TYPE A
        tx[p++] = 0x00;
        tx[p++] = 0x01;  // CLASS IN
        tx[p++] = 0x00;
        tx[p++] = 0x00;
        tx[p++] = 0x00;
        tx[p++] = 0x3C;  // TTL = 60s
        tx[p++] = 0x00;
        tx[p++] = 0x04;  // RDLENGTH = 4
        tx[p++] = 192;
        tx[p++] = 168;
        tx[p++] = 4;
        tx[p++] = 1;  // RDATA = 192.168.4.1

        sendto(s_dns_socket, tx, p, 0, reinterpret_cast<sockaddr*>(&client), client_len);
    }

    if (s_dns_socket >= 0) {
        close(s_dns_socket);
        s_dns_socket = -1;
    }
    s_dns_task = nullptr;
    vTaskDelete(nullptr);
}

void StartCaptiveDns()
{
    if (s_dns_task != nullptr) {
        return;
    }
    s_dns_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s_dns_socket < 0) {
        ESP_LOGW(kTag, "Captive DNS socket create failed");
        return;
    }
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kCaptiveDnsPort);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(s_dns_socket, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ESP_LOGW(kTag, "Captive DNS bind failed");
        close(s_dns_socket);
        s_dns_socket = -1;
        return;
    }
    timeval tv = {};
    tv.tv_sec = 1;  // wake periodically so the stop flag is observed promptly
    setsockopt(s_dns_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    s_dns_stop.store(false, std::memory_order_relaxed);
    if (xTaskCreate(CaptiveDnsTask, "captive_dns", 3072, nullptr, 5, &s_dns_task) != pdPASS) {
        ESP_LOGW(kTag, "Captive DNS task create failed");
        close(s_dns_socket);
        s_dns_socket = -1;
        s_dns_task = nullptr;
    }
}

void StopCaptiveDns()
{
    // The task observes the flag on its next recv timeout (~1s), closes the socket, and self-deletes.
    s_dns_stop.store(true, std::memory_order_relaxed);
}

esp_err_t HandleCaptivePortalRedirect(httpd_req_t* request, httpd_err_code_t)
{
    httpd_resp_set_status(request, "302 Found");
    httpd_resp_set_hdr(request, "Location", kApRedirectUrl);
    return httpd_resp_send(request, "", 0);
}

esp_err_t SendEmbeddedAsset(httpd_req_t* request, const uint8_t* start, const uint8_t* end,
                            const char* content_type)
{
    httpd_resp_set_status(request, HTTPD_200);
    httpd_resp_set_type(request, content_type);
    // Every embedded portal asset is stored gzipped (see CMakeLists.txt).
    httpd_resp_set_hdr(request, "Content-Encoding", "gzip");
    const ssize_t length = end - start;
    return httpd_resp_send(request, reinterpret_cast<const char*>(start),
                           length > 0 ? length : 0);
}

esp_err_t HandlePortalRoot(httpd_req_t* request)
{
    return SendEmbeddedAsset(request, kIndexHtmlStart, kIndexHtmlEnd, "text/html; charset=utf-8");
}

esp_err_t HandlePortalIndexJs(httpd_req_t* request)
{
    return SendEmbeddedAsset(request, kIndexJsStart, kIndexJsEnd,
                             "application/javascript; charset=utf-8");
}

esp_err_t HandlePortalIndexCss(httpd_req_t* request)
{
    return SendEmbeddedAsset(request, kIndexCssStart, kIndexCssEnd, "text/css; charset=utf-8");
}

// --- Config commands (served over the portal's HTTP routes by config_api) ----------------------

config_api::Response CommandStatus(const cJSON*)
{
    const UiState ui_state = GetUiState();
    const ScanSnapshot snapshot = GetScanSnapshot();
    return config_api::Ok(ui_state.connected ? "Connected" : "Not connected",
                          BuildStatusJson(ui_state, &snapshot));
}

config_api::Response CommandScan(const cJSON*)
{
    const UiState ui_state = GetUiState();
    const ScanSnapshot snapshot = GetScanSnapshot();
    if (snapshot.state == ScanState::kRunning) {
        return config_api::Ok("Scanning for networks", BuildStatusJson(ui_state, &snapshot));
    }
    if (snapshot.state == ScanState::kComplete) {
        return config_api::Ok("Network scan complete", BuildStatusJson(ui_state, &snapshot));
    }
    if (!StartNetworkScan()) {
        return config_api::Error(500, "scan_failed", "Scan failed", {},
                                 BuildStatusJson(ui_state, nullptr));
    }
    const UiState running_ui_state = GetUiState();
    const ScanSnapshot running_snapshot = GetScanSnapshot();
    return config_api::Ok("Scanning for networks",
                          BuildStatusJson(running_ui_state, &running_snapshot));
}

config_api::Response CommandConnect(const cJSON* args)
{
    const std::string ssid = config_api::StringArg(args, "ssid");
    const std::string password = config_api::StringArg(args, "password");
    if (ssid.empty()) {
        return config_api::Error(400, "missing_ssid", "SSID required", "ssid");
    }
    if (!ConnectToNetwork(ssid, password, true)) {
        return config_api::Error(500, "connect_failed", "Failed to start Wi-Fi connection");
    }

    config_api::JsonPtr data(cJSON_CreateObject());
    cJSON_AddStringToObject(data.get(), "ssid", ssid.c_str());
    return config_api::Ok("Connecting to " + ssid, std::move(data));
}

config_api::Response CommandDisconnect(const cJSON*)
{
    const bool was_connected = IsConnected();
    const UiState previous = GetUiState();
    if (!DisconnectFromNetwork(true)) {
        return config_api::Error(500, "disconnect_failed", "Failed to disconnect Wi-Fi");
    }
    return config_api::Ok(was_connected
                              ? "Disconnected and cleared credentials for " + previous.ssid
                              : "Cleared saved Wi-Fi credentials");
}

void RegisterConfigCommands()
{
    config_api::RegisterCommand("wifi_status", CommandStatus);
    config_api::RegisterCommand("wifi_scan", CommandScan);
    config_api::RegisterCommand("wifi_connect", CommandConnect);
    config_api::RegisterCommand("wifi_disconnect", CommandDisconnect);
}

void StartConfigPortal()
{
    if (s_portal_server != nullptr) {
        return;
    }

    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    // Portal page + assets (/, index.js, index.css) and the config_api REST routes.
    config.max_uri_handlers = 3 + config_api::http::RouteCount() + 4;  // + spare
    config.lru_purge_enable = true;
    // Internal RAM is tight in AP mode, so lwIP can stall briefly waiting for TX buffers; the
    // 5s default aborted portal asset sends mid-transfer.
    config.send_wait_timeout = 15;

    esp_err_t err = httpd_start(&s_portal_server, &config);
    if (err != ESP_OK) {
        ESP_LOGE(kTag, "Failed to start Wi-Fi backend: %s", esp_err_to_name(err));
        s_portal_server = nullptr;
        return;
    }

    httpd_uri_t root = {
        .uri = "/",
        .method = HTTP_GET,
        .handler = HandlePortalRoot,
        .user_ctx = nullptr,
    };
    httpd_uri_t index_js = {
        .uri = kPortalIndexJsUri,
        .method = HTTP_GET,
        .handler = HandlePortalIndexJs,
        .user_ctx = nullptr,
    };
    httpd_uri_t index_css = {
        .uri = kPortalIndexCssUri,
        .method = HTTP_GET,
        .handler = HandlePortalIndexCss,
        .user_ctx = nullptr,
    };

    if (RegisterRoute(s_portal_server, &root) != ESP_OK ||
        RegisterRoute(s_portal_server, &index_js) != ESP_OK ||
        RegisterRoute(s_portal_server, &index_css) != ESP_OK ||
        config_api::http::RegisterRoutes(s_portal_server) != ESP_OK) {
        StopConfigPortal();
        return;
    }

    // Make it a real captive portal: redirect any unrecognised request (OS connectivity probes,
    // unknown hosts resolved to us by the DNS server below) to the portal root.
    httpd_register_err_handler(s_portal_server, HTTPD_404_NOT_FOUND, HandleCaptivePortalRedirect);
    StartCaptiveDns();

    ESP_LOGI(kTag, "Wi-Fi backend active at %s", GetUiState().ap_url.c_str());
}

void HandleWifiEvent(int32_t event_id, void* event_data);
void HandleIpEvent(int32_t event_id, void* event_data);
void StartStationAttempt(bool allow_ap_fallback);
void StopBackgroundRetry();
void ResumeAfterScan(const std::vector<ScannedNetwork>& networks);
void TransitionWorker(void*);

void OnWifiEvent(void* arg, esp_event_base_t base, int32_t event_id, void* event_data)
{
    (void)arg;
    (void)base;
    HandleWifiEvent(event_id, event_data);
}

void OnIpEvent(void* arg, esp_event_base_t base, int32_t event_id, void* event_data)
{
    (void)arg;
    (void)base;
    HandleIpEvent(event_id, event_data);
}

void OnWifiConnectTimeout(void* arg)
{
    (void)arg;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_connect_timer_active = false;
        s_reconnecting = false;
        s_connected = false;
        s_ip_address.clear();
        s_rssi = 0;
    }

    esp_err_t err = esp_wifi_disconnect();
    if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED &&
        err != ESP_ERR_WIFI_CONN) {
        ESP_LOGW(kTag, "esp_wifi_disconnect after timeout failed: %s", esp_err_to_name(err));
    }

    ESP_LOGW(kTag, "Wi-Fi connect timeout");
    Notify(State::kDisconnected, "CONNECT TIMEOUT");
}

void InitializeStack()
{
    if (s_stack_initialized) {
        return;
    }

    CheckOrAbort(esp_netif_init(), "esp_netif_init");
    CheckOrAbort(esp_event_loop_create_default(), "esp_event_loop_create_default");

    s_sta_netif = esp_netif_create_default_wifi_sta();
    s_ap_netif = esp_netif_create_default_wifi_ap();

    wifi_init_config_t wifi_init = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&wifi_init));
    ESP_ERROR_CHECK(esp_wifi_set_storage(WIFI_STORAGE_RAM));
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID,
                                                        OnWifiEvent, nullptr,
                                                        &s_wifi_event_handler));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP,
                                                        OnIpEvent, nullptr,
                                                        &s_ip_event_handler));
    UpdateAccessPointIdentity();
    s_stack_initialized = true;
}

bool QueueTransition(TransitionRequest request)
{
    if (s_transition_queue == nullptr) {
        return false;
    }
    if (xQueueSend(s_transition_queue, &request, 0) != pdPASS) {
        ESP_LOGW(kTag, "Wi-Fi transition queue full");
        return false;
    }
    return true;
}

// Ends a scan the driver will never report on, and returns whether there was one.
//
// esp_wifi_stop() and esp_wifi_connect() both abort a running scan silently -- no
// WIFI_EVENT_SCAN_DONE follows -- so every path that tears the station down has to
// resolve the scan itself. HandleScanDoneEvent used to be the only thing that could
// leave ScanState::kRunning, which is why an aborted scan wedged the service until
// reboot: StartNetworkScan's "already running" guard then dropped every later request.
bool ResolveInFlightScan(esp_err_t reason)
{
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (s_scan_snapshot.state != ScanState::kRunning) {
            return false;
        }
        s_scan_snapshot.state = ScanState::kFailed;
        s_scan_snapshot.last_error = reason;
        s_scan_snapshot.networks.clear();
    }

    if (s_scan_timeout_timer != nullptr) {
        CheckOrAbort(esp_timer_stop(s_scan_timeout_timer), "esp_timer_stop");
    }
    ESP_LOGW(kTag, "Network scan aborted: %s", esp_err_to_name(reason));
    Notify(State::kScanFailed, "SCAN_FAILED");
    if (reason == ESP_ERR_INVALID_STATE) {
        // A teardown (connect, disconnect, stop) aborted the scan and now owns the radio;
        // just lift the scan's pause instead of starting a competing reconnect.
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_resume_after_scan = false;
        s_reconnect_suspended = s_user_disconnected;
    } else {
        ResumeAfterScan({});
    }
    return true;
}

void OnScanTimeout(void* arg)
{
    (void)arg;
    if (ResolveInFlightScan(ESP_ERR_TIMEOUT)) {
        ESP_LOGW(kTag, "Network scan timed out without a SCAN_DONE event");
    }
}

void StopWifiNow()
{
    ResolveInFlightScan(ESP_ERR_INVALID_STATE);
    StopBackgroundRetry();
    CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");
    StopConfigPortal();
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_suppress_disconnect_event = true;
        s_connect_timer_active = false;
        s_reconnecting = false;
        s_connected = false;
        s_access_point_mode = false;
        s_current_ssid.clear();
        s_ip_address.clear();
        s_rssi = 0;
    }

    if (s_stack_initialized) {
        esp_err_t err = esp_wifi_stop();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED) {
            ESP_ERROR_CHECK(err);
        }
    }

    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_suppress_disconnect_event = false;
    }
    Notify(State::kIdle);
}

void EnterAccessPointModeNow()
{
    ResolveInFlightScan(ESP_ERR_INVALID_STATE);
    InitializeStack();
    UpdateAccessPointIdentity();
    CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");

    if (!GetUiState().wifi_enabled) {
        StopWifiNow();
        return;
    }

    ReloadSavedCredentials();

    Credentials credentials;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        credentials = ResolveStationCredentialsLocked();
    }
    if (credentials.valid()) {
        StartStationAttempt(false);
        return;
    }

    EnsureAccessPointPassword();
    std::string ap_ssid;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_suppress_disconnect_event = true;
        s_connect_timer_active = false;
        s_reconnecting = false;
        s_connected = false;
        s_access_point_mode = true;
        s_current_ssid.clear();
        s_ip_address.clear();
        s_rssi = 0;
        ap_ssid = s_ap_ssid;
    }

    wifi_config_t config = {};
    ConfigureAccessPointConfig(ap_ssid, &config);

    esp_err_t err = esp_wifi_stop();
    if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED) {
        ESP_ERROR_CHECK(err);
    }
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &config));
    ESP_ERROR_CHECK(esp_wifi_start());

    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_suppress_disconnect_event = false;
        s_ap_url = IpInfoToUrl(s_ap_netif);
    }

    StartConfigPortal();
    Notify(State::kAccessPointMode, GetUiState().ap_ssid);
}

// In setup mode with the setup network already up, points only the station interface at the
// new network and leaves the radio running. A stop/start instead restarts the setup network,
// which drops the phone on the portal (mid-DHCP, or before the page can see the result) on
// every connection attempt. Returns false when the radio is not in a state to do this; the
// caller then falls back to a full restart.
bool RetargetStationKeepingAccessPoint(wifi_config_t* station_config)
{
    wifi_mode_t mode = WIFI_MODE_NULL;
    // The portal runs only while the radio is started in AP or AP+STA mode (StopWifiNow stops
    // the portal before the radio).
    if (s_portal_server == nullptr || esp_wifi_get_mode(&mode) != ESP_OK ||
        (mode != WIFI_MODE_AP && mode != WIFI_MODE_APSTA)) {
        return false;
    }

    if (mode == WIFI_MODE_APSTA) {
        // The station may still be joining the previous network; set_config rejects that.
        const esp_err_t err = esp_wifi_disconnect();
        if (err != ESP_OK && err != ESP_ERR_WIFI_CONN) {
            ESP_LOGW(kTag, "esp_wifi_disconnect before retarget failed: %s",
                     esp_err_to_name(err));
            return false;
        }
    } else {
        const esp_err_t err = esp_wifi_set_mode(WIFI_MODE_APSTA);
        if (err != ESP_OK) {
            ESP_LOGW(kTag, "Switching setup mode to AP+STA failed: %s", esp_err_to_name(err));
            return false;
        }
    }

    const esp_err_t err = esp_wifi_set_config(WIFI_IF_STA, station_config);
    if (err != ESP_OK) {
        ESP_LOGW(kTag, "Station config for retarget failed: %s", esp_err_to_name(err));
        return false;
    }
    ESP_LOGI(kTag, "Setup network kept up; station retargeted to ssid=%.32s",
             reinterpret_cast<const char*>(station_config->sta.ssid));
    return true;
}

void StartStationAttempt(bool allow_ap_fallback)
{
    ResolveInFlightScan(ESP_ERR_INVALID_STATE);
    InitializeStack();
    ReloadSavedCredentials();

    if (!GetUiState().wifi_enabled) {
        StopWifiNow();
        return;
    }

    Credentials credentials;
    bool access_point_mode = false;
    std::string ap_ssid;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        credentials = ResolveStationCredentialsLocked();
        access_point_mode = s_access_point_mode;
        ap_ssid = s_ap_ssid;
    }
    if (access_point_mode) {
        EnsureAccessPointPassword();
    }
    if (!credentials.valid()) {
        if (allow_ap_fallback) {
            ESP_LOGI(kTag, "No station credentials; entering AP setup mode");
            EnterAccessPointModeNow();
            return;
        }

        if (!access_point_mode) {
            StopConfigPortal();
        }
        CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");
        {
            std::lock_guard<std::mutex> lock(s_state_mutex);
            s_suppress_disconnect_event = true;
            s_connect_timer_active = false;
            s_reconnecting = false;
            s_connected = false;
            s_current_ssid.clear();
            s_ip_address.clear();
            s_rssi = 0;
        }

        esp_err_t err = esp_wifi_stop();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED) {
            ESP_ERROR_CHECK(err);
        }
        ESP_ERROR_CHECK(esp_wifi_set_mode(access_point_mode ? WIFI_MODE_AP : WIFI_MODE_STA));
        if (access_point_mode) {
            wifi_config_t ap_config = {};
            ConfigureAccessPointConfig(ap_ssid, &ap_config);
            ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_config));
        }
        ESP_ERROR_CHECK(esp_wifi_start());

        {
            std::lock_guard<std::mutex> lock(s_state_mutex);
            s_suppress_disconnect_event = false;
            if (access_point_mode) {
                s_ap_url = IpInfoToUrl(s_ap_netif);
            }
        }

        if (access_point_mode) {
            StartConfigPortal();
            Notify(State::kAccessPointMode, ap_ssid);
        } else {
            Notify(State::kDisconnected, "NO_CREDENTIALS");
        }
        return;
    }

    if (!access_point_mode) {
        StopConfigPortal();
    }
    CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_suppress_disconnect_event = true;
        s_connect_timer_active = false;
        s_reconnecting = false;
        s_connected = false;
        s_active_credentials = credentials;
        s_current_ssid = credentials.ssid;
        s_ip_address.clear();
        s_rssi = 0;
    }

    wifi_config_t station_config = {};
    ConfigureStationConfig(credentials, &station_config);
    wifi_config_t ap_config = {};
    if (access_point_mode) {
        ConfigureAccessPointConfig(ap_ssid, &ap_config);
    }

    if (!access_point_mode || !RetargetStationKeepingAccessPoint(&station_config)) {
        esp_err_t err = esp_wifi_stop();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED) {
            ESP_ERROR_CHECK(err);
        }
        ESP_ERROR_CHECK(esp_wifi_set_mode(access_point_mode ? WIFI_MODE_APSTA : WIFI_MODE_STA));
        if (access_point_mode) {
            ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_config));
        }
        ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &station_config));
        ESP_ERROR_CHECK(esp_wifi_start());
    }

    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_suppress_disconnect_event = false;
        s_connect_timer_active = true;
        if (access_point_mode) {
            s_ap_url = IpInfoToUrl(s_ap_netif);
        }
    }

    if (access_point_mode) {
        StartConfigPortal();
    }
    Notify(State::kConnecting, credentials.ssid);
    ESP_ERROR_CHECK(esp_timer_start_once(s_connect_timer, kConnectTimeoutSec * 1000000ULL));
    ESP_ERROR_CHECK(esp_wifi_connect());
}

// --- background retry -------------------------------------------------------

void StopBackgroundRetry()
{
    if (s_retry_timer != nullptr) {
        CheckOrAbort(esp_timer_stop(s_retry_timer), "esp_timer_stop");
    }
}

// Caller holds s_state_mutex. Resets the retry loop after anything that counts as user intent
// or a successful association.
void ResetRetryStateLocked()
{
    s_reconnect_attempts = 0;
    s_backoff_step = 0;
}

// Arms the next background retry. Called once the fast attempts are used up.
void ScheduleBackgroundRetry()
{
    size_t step = 0;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        step = s_backoff_step;
        if (s_backoff_step + 1 < std::size(kBackoffDelaysSec)) {
            ++s_backoff_step;
        }
    }
    const uint32_t delay_sec = kBackoffDelaysSec[step];
    StopBackgroundRetry();
    const esp_err_t err = esp_timer_start_once(s_retry_timer, delay_sec * 1000000ULL);
    if (err != ESP_OK) {
        ESP_LOGW(kTag, "Wi-Fi retry timer start failed: %s", esp_err_to_name(err));
        return;
    }
    ESP_LOGI(kTag, "Wi-Fi not reachable; retrying in %u s", static_cast<unsigned>(delay_sec));
}

// Caller holds s_state_mutex. Makes the next saved network (round-robin) the one to try.
void SelectNextSavedNetworkLocked()
{
    if (s_saved_networks.empty()) {
        return;
    }
    s_rotation_index = (s_rotation_index + 1) % s_saved_networks.size();
    s_active_credentials = s_saved_networks[s_rotation_index];
    s_persist_active_credentials_on_success = false;
}

bool AutoReconnectAllowedLocked()
{
    return s_wifi_enabled && !s_user_disconnected && !s_access_point_mode && !s_connected &&
           !s_saved_networks.empty();
}

void OnRetryTimer(void* arg)
{
    (void)arg;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (!AutoReconnectAllowedLocked() || s_reconnecting ||
            s_scan_snapshot.state == ScanState::kRunning) {
            return;
        }
        SelectNextSavedNetworkLocked();
        // One attempt per retry: if it fails, the disconnect handler schedules the next.
        s_reconnect_attempts = kMaxReconnectAttempts;
        s_reconnect_suspended = false;
        s_reconnecting = true;
        ESP_LOGI(kTag, "Background Wi-Fi retry: ssid=%s", s_active_credentials.ssid.c_str());
    }
    if (!QueueTransition(TransitionRequest::kStartStation)) {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_reconnecting = false;
    }
}

// After a scan that paused the reconnect loop, join the strongest saved network the scan saw,
// or fall back to background retries if none is in range.
void ResumeAfterScan(const std::vector<ScannedNetwork>& networks)
{
    bool join = false;
    bool retry_later = false;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (!s_resume_after_scan) {
            return;
        }
        s_resume_after_scan = false;
        s_reconnect_suspended = s_user_disconnected;
        if (!AutoReconnectAllowedLocked() || s_reconnecting) {
            return;
        }

        const ScannedNetwork* best = nullptr;
        for (const ScannedNetwork& network : networks) {
            if (FindSavedNetworkLocked(network.ssid) != nullptr &&
                (best == nullptr || network.rssi > best->rssi)) {
                best = &network;
            }
        }
        if (best != nullptr) {
            s_active_credentials = *FindSavedNetworkLocked(best->ssid);
            s_persist_active_credentials_on_success = false;
            ResetRetryStateLocked();
            s_reconnecting = true;
            join = true;
            ESP_LOGI(kTag, "Joining saved network in range: ssid=%s rssi=%d",
                     best->ssid.c_str(), best->rssi);
        } else {
            retry_later = true;
        }
    }
    if (join && !QueueTransition(TransitionRequest::kStartStation)) {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_reconnecting = false;
    } else if (retry_later) {
        ScheduleBackgroundRetry();
    }
}

void DisconnectStationNow(bool clear_saved_credentials)
{
    {
        // Mark the disconnect as deliberate before anything else can restart the loop.
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_user_disconnected = true;
        s_resume_after_scan = false;
    }
    ResolveInFlightScan(ESP_ERR_INVALID_STATE);
    CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");
    StopBackgroundRetry();
    std::string disconnected_ssid;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        disconnected_ssid = s_current_ssid;
        s_suppress_disconnect_event = true;
        s_connect_timer_active = false;
        s_reconnecting = false;
        // A user-initiated disconnect must not be undone by the reconnect loop. Standing
        // the loop down here is deterministic; s_suppress_disconnect_event alone is not,
        // since STA_DISCONNECTED is delivered asynchronously on the event loop task.
        s_reconnect_suspended = true;
        s_reconnect_attempts = 0;
    }

    esp_err_t err = esp_wifi_disconnect();
    if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED &&
        err != ESP_ERR_WIFI_CONN) {
        ESP_LOGW(kTag, "esp_wifi_disconnect failed: %s", esp_err_to_name(err));
    }

    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_connected = false;
        s_current_ssid.clear();
        s_ip_address.clear();
        s_rssi = 0;
        s_active_credentials = {};
        s_persist_active_credentials_on_success = false;
        s_suppress_disconnect_event = false;
    }

    if (clear_saved_credentials) {
        // Forget only the network being disconnected; other saved networks stay.
        ForgetSavedNetworkNow(disconnected_ssid);
    }
    Notify(State::kDisconnected, clear_saved_credentials ? "DISCONNECTED" : "DISCONNECTED_TEMP");
}

// Runs on the transition worker so a scan can never interleave with the stop/start/connect
// of a reconnect cycle. StartNetworkScan has already suspended the reconnect loop and put
// the snapshot in kRunning; this only does the radio work.
void StartNetworkScanNow()
{
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (s_scan_snapshot.state != ScanState::kRunning) {
            // Resolved while queued (watchdog, or a teardown that ran first) -- nobody is
            // waiting on this scan any more.
            ESP_LOGI(kTag, "Queued network scan is stale; skipping");
            return;
        }
    }

    InitializeStack();

    const UiState state = GetUiState();

    // Break out of an in-flight association, but only when there is not an established
    // one. esp_wifi_scan_start() is rejected with ESP_ERR_WIFI_STATE while the station is
    // *connecting* -- the state an out-of-range device with saved credentials sits in
    // permanently -- but scanning while *connected* is fine. Disconnecting regardless
    // dropped a live link every time the Wi-Fi page was opened, since entry always starts
    // a scan.
    esp_err_t err = ESP_OK;
    if (!state.connected) {
        err = esp_wifi_disconnect();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_INIT && err != ESP_ERR_WIFI_NOT_STARTED &&
            err != ESP_ERR_WIFI_CONN) {
            ESP_LOGW(kTag, "esp_wifi_disconnect before scan failed: %s", esp_err_to_name(err));
        }
    }

    const wifi_mode_t desired_mode = state.access_point_mode ? WIFI_MODE_APSTA : WIFI_MODE_STA;
    wifi_mode_t current_mode = WIFI_MODE_NULL;
    err = esp_wifi_get_mode(&current_mode);
    if (err != ESP_OK) {
        ResolveInFlightScan(err);
        return;
    }

    if (current_mode != desired_mode) {
        err = esp_wifi_stop();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_STARTED) {
            ResolveInFlightScan(err);
            return;
        }
        err = esp_wifi_set_mode(desired_mode);
        if (err != ESP_OK) {
            ResolveInFlightScan(err);
            return;
        }
        if (desired_mode == WIFI_MODE_APSTA) {
            wifi_config_t ap_config = {};
            ConfigureAccessPointConfig(state.ap_ssid, &ap_config);
            err = esp_wifi_set_config(WIFI_IF_AP, &ap_config);
            if (err != ESP_OK) {
                ResolveInFlightScan(err);
                return;
            }
        }
    }

    err = esp_wifi_start();
    if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
        ResolveInFlightScan(err);
        return;
    }

    wifi_scan_config_t scan_config = {};
    scan_config.ssid = nullptr;
    scan_config.bssid = nullptr;
    scan_config.channel = 0;
    scan_config.show_hidden = false;
    scan_config.scan_type = WIFI_SCAN_TYPE_ACTIVE;
    scan_config.scan_time.active.min = 30;
    scan_config.scan_time.active.max = 80;
    scan_config.home_chan_dwell_time = 30;

    // Hold the scan off while the panel is mid-refresh; see ScanDeferProvider. Capped so a
    // stuck or very slow refresh delays the scan rather than losing it.
    {
        ScanDeferProvider defer_provider = nullptr;
        void* defer_context = nullptr;
        {
            std::lock_guard<std::mutex> lock(s_state_mutex);
            defer_provider = s_scan_defer_provider;
            defer_context = s_scan_defer_context;
        }
        if (defer_provider != nullptr) {
            int polls = 0;
            // Phase 1: let a just-requested refresh actually begin.
            const int start_polls = kScanDeferStartMs / kScanDeferPollMs;
            while (polls < start_polls && !defer_provider(defer_context)) {
                vTaskDelay(pdMS_TO_TICKS(kScanDeferPollMs));
                ++polls;
            }
            // Phase 2: let it finish painting before the radio goes on air.
            const int max_polls = kScanDeferMaxMs / kScanDeferPollMs;
            while (polls < max_polls && defer_provider(defer_context)) {
                vTaskDelay(pdMS_TO_TICKS(kScanDeferPollMs));
                ++polls;
            }
            if (polls > 0) {
                vTaskDelay(pdMS_TO_TICKS(kScanDeferSettleMs));
                ESP_LOGI(kTag, "Scan deferred %d ms for the display",
                         polls * kScanDeferPollMs + kScanDeferSettleMs);
            }
        }
    }

    for (int attempt = 0; attempt < kScanStartAttempts; ++attempt) {
        err = esp_wifi_scan_start(&scan_config, false);
        if (err != ESP_ERR_WIFI_STATE) {
            break;
        }
        ESP_LOGI(kTag, "Scan start deferred; station still busy (attempt %d/%d)",
                 attempt + 1, kScanStartAttempts);
        vTaskDelay(pdMS_TO_TICKS(kScanStartRetryDelayMs));
    }

    if (err != ESP_OK) {
        ResolveInFlightScan(err);
        return;
    }

    ESP_LOGI(kTag, "Network scan started");
}

void HandleTransitionRequest(TransitionRequest request)
{
    switch (request) {
        case TransitionRequest::kStartScan:
            StartNetworkScanNow();
            break;
        case TransitionRequest::kStart:
#if CONFIG_FOLLOWUP_WIFI_START_IN_AP_MODE
            EnterAccessPointModeNow();
#else
            StartStationAttempt(true);
#endif
            break;
        case TransitionRequest::kStartStation:
            StartStationAttempt(false);
            break;
        case TransitionRequest::kEnterAccessPoint:
            EnterAccessPointModeNow();
            break;
        case TransitionRequest::kDisableAccessPoint:
            StartStationAttempt(false);
            break;
        case TransitionRequest::kDisconnectStation:
            DisconnectStationNow(s_clear_saved_credentials_on_disconnect);
            break;
        case TransitionRequest::kStopWifi:
            StopWifiNow();
            break;
        case TransitionRequest::kForgetNetwork: {
            std::string ssid;
            {
                std::lock_guard<std::mutex> lock(s_state_mutex);
                ssid = std::move(s_forget_ssid);
                s_forget_ssid.clear();
            }
            if (!ssid.empty() && ForgetSavedNetworkNow(ssid)) {
                ESP_LOGI(kTag, "Forgot saved network ssid=%s", ssid.c_str());
                Notify(State::kIdle, "NETWORK_FORGOTTEN");
            }
            break;
        }
    }
}

void TransitionWorker(void*)
{
    TransitionRequest request = TransitionRequest::kStopWifi;
    while (true) {
        if (xQueueReceive(s_transition_queue, &request, portMAX_DELAY) == pdTRUE) {
            HandleTransitionRequest(request);
        }
    }
}

void HandleScanDoneEvent(void* event_data)
{
    if (s_scan_timeout_timer != nullptr) {
        CheckOrAbort(esp_timer_stop(s_scan_timeout_timer), "esp_timer_stop");
    }

    auto* event = static_cast<wifi_event_sta_scan_done_t*>(event_data);
    uint16_t ap_count = 0;
    esp_err_t status = esp_wifi_scan_get_ap_num(&ap_count);
    if (status != ESP_OK) {
        {
            std::lock_guard<std::mutex> lock(s_state_mutex);
            s_scan_snapshot.state = ScanState::kFailed;
            s_scan_snapshot.last_error = status;
            s_scan_snapshot.networks.clear();
        }
        Notify(State::kScanFailed, "SCAN_FAILED");
        ResumeAfterScan({});
        return;
    }

    std::vector<wifi_ap_record_t> raw_records(ap_count);
    if (ap_count > 0) {
        uint16_t count_to_copy = ap_count;
        status = esp_wifi_scan_get_ap_records(&count_to_copy, raw_records.data());
        if (status != ESP_OK) {
            {
                std::lock_guard<std::mutex> lock(s_state_mutex);
                s_scan_snapshot.state = ScanState::kFailed;
                s_scan_snapshot.last_error = status;
                s_scan_snapshot.networks.clear();
            }
            Notify(State::kScanFailed, "SCAN_FAILED");
            ResumeAfterScan({});
            return;
        }
        ap_count = count_to_copy;
    }

    std::vector<ScannedNetwork> networks;
    networks.reserve(ap_count);
    for (uint16_t index = 0; index < ap_count; ++index) {
        const wifi_ap_record_t& record = raw_records[index];
        if (record.ssid[0] == '\0') {
            continue;
        }
        networks.push_back({
            .ssid = std::string(reinterpret_cast<const char*>(record.ssid)),
            .rssi = record.rssi,
            .auth_mode = record.authmode,
        });
    }
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        for (ScannedNetwork& network : networks) {
            network.saved = FindSavedNetworkLocked(network.ssid) != nullptr;
        }
    }

    const bool success = event != nullptr && event->status == 0;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_scan_snapshot.state = success ? ScanState::kComplete : ScanState::kFailed;
        s_scan_snapshot.last_error = success ? ESP_OK : ESP_FAIL;
        s_scan_snapshot.networks = networks;
    }
    Notify(success ? State::kScanCompleted : State::kScanFailed,
           success ? "NETWORK_SCAN_COMPLETE" : "SCAN_FAILED");
    ResumeAfterScan(success ? networks : std::vector<ScannedNetwork>{});
}

void HandleWifiEvent(int32_t event_id, void* event_data)
{
    if (event_id == WIFI_EVENT_SCAN_DONE) {
        HandleScanDoneEvent(event_data);
        return;
    }

    if (event_id == WIFI_EVENT_STA_DISCONNECTED) {
        auto* event = static_cast<wifi_event_sta_disconnected_t*>(event_data);
        bool suppress = false;
        bool should_reconnect = false;
        bool attempts_exhausted = false;
        int attempt = 0;
        std::string reconnect_ssid;
        {
            std::lock_guard<std::mutex> lock(s_state_mutex);
            suppress = s_suppress_disconnect_event;
            if (!suppress) {
                s_connected = false;
                s_ip_address.clear();
                s_rssi = 0;
                const Credentials credentials = ResolveStationCredentialsLocked();
                const bool eligible = s_wifi_enabled && credentials.valid() &&
                                      !s_reconnecting && !s_reconnect_suspended;
                if (eligible && s_reconnect_attempts >= kMaxReconnectAttempts) {
                    // Fast attempts are used up: the network is probably out of range. Drop to
                    // slow background retries across all saved networks (ScheduleBackground
                    // Retry) rather than standing down for good.
                    attempts_exhausted = true;
                } else if (eligible) {
                    ++s_reconnect_attempts;
                    should_reconnect = true;
                    s_reconnecting = true;
                }
                attempt = s_reconnect_attempts;
                reconnect_ssid = credentials.ssid;
            }
        }

        if (!suppress) {
            Notify(State::kDisconnected,
                   attempts_exhausted
                       ? "RECONNECT_BACKGROUND"
                       : (event != nullptr ? DisconnectReasonToString(event->reason)
                                           : "DISCONNECTED"));
            if (attempts_exhausted) {
                ESP_LOGW(kTag,
                         "Wi-Fi fast reconnect attempts used up for ssid=%s; switching to "
                         "background retries",
                         reconnect_ssid.empty() ? "<unknown>" : reconnect_ssid.c_str());
                ScheduleBackgroundRetry();
            } else if (should_reconnect) {
                ESP_LOGI(kTag,
                         "Wi-Fi disconnected; scheduling reconnect %d/%d to ssid=%s",
                         attempt, kMaxReconnectAttempts,
                         reconnect_ssid.empty() ? "<unknown>" : reconnect_ssid.c_str());
                if (!QueueTransition(TransitionRequest::kStartStation)) {
                    std::lock_guard<std::mutex> lock(s_state_mutex);
                    s_reconnecting = false;
                    ESP_LOGW(kTag, "Wi-Fi reconnect queue request failed");
                }
            }
        }
    }
}

void HandleIpEvent(int32_t event_id, void* event_data)
{
    if (event_id != IP_EVENT_STA_GOT_IP || event_data == nullptr) {
        return;
    }

    auto* event = static_cast<ip_event_got_ip_t*>(event_data);
    char ip_address[32] = {};
    std::snprintf(ip_address, sizeof(ip_address), IPSTR, IP2STR(&event->ip_info.ip));

    int rssi = 0;
    wifi_ap_record_t ap_info = {};
    if (esp_wifi_sta_get_ap_info(&ap_info) == ESP_OK) {
        rssi = ap_info.rssi;
    }

    CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");
    StopBackgroundRetry();
    Credentials credentials_to_persist = {};
    bool persist_credentials = false;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_connect_timer_active = false;
        s_reconnecting = false;
        ResetRetryStateLocked();
        s_reconnect_suspended = false;
        s_user_disconnected = false;
        s_connected = true;
        s_ip_address = ip_address;
        s_rssi = rssi;
        if (s_persist_active_credentials_on_success && s_active_credentials.valid()) {
            credentials_to_persist = s_active_credentials;
            persist_credentials = true;
            s_persist_active_credentials_on_success = false;
        }
    }

    if (persist_credentials) {
        if (!SaveCredentials(credentials_to_persist.ssid, credentials_to_persist.password)) {
            ESP_LOGW(kTag, "Connected but failed to persist Wi-Fi credentials");
        }
    }

    Notify(State::kConnected, ip_address);
}

}  // namespace

esp_err_t Init()
{
    if (s_initialized) {
        return ESP_OK;
    }
    RegisterConfigCommands();

    esp_timer_create_args_t timer_args = {
        .callback = OnWifiConnectTimeout,
        .arg = nullptr,
        .dispatch_method = ESP_TIMER_TASK,
        .name = "wifi_connect_timer",
        .skip_unhandled_events = true,
    };
    ESP_ERROR_CHECK(esp_timer_create(&timer_args, &s_connect_timer));

    esp_timer_create_args_t retry_timer_args = {
        .callback = OnRetryTimer,
        .arg = nullptr,
        .dispatch_method = ESP_TIMER_TASK,
        .name = "wifi_retry_timer",
        .skip_unhandled_events = true,
    };
    ESP_ERROR_CHECK(esp_timer_create(&retry_timer_args, &s_retry_timer));

    esp_timer_create_args_t scan_timer_args = {
        .callback = OnScanTimeout,
        .arg = nullptr,
        .dispatch_method = ESP_TIMER_TASK,
        .name = "wifi_scan_timer",
        .skip_unhandled_events = true,
    };
    ESP_ERROR_CHECK(esp_timer_create(&scan_timer_args, &s_scan_timeout_timer));

    s_transition_queue = xQueueCreate(kTransitionQueueDepth, sizeof(TransitionRequest));
    if (s_transition_queue == nullptr) {
        return ESP_ERR_NO_MEM;
    }

    if (xTaskCreatePinnedToCore(TransitionWorker,
                                "wifi_transition",
                                kTransitionTaskStackWords,
                                nullptr,
                                followup_task_config::kPriorityWifiTransition,
                                &s_transition_task,
                                followup_task_config::kSystemCore) != pdPASS) {
        s_transition_task = nullptr;
        return ESP_ERR_NO_MEM;
    }

    if (xTaskCreatePinnedToCore(CallbackTask,
                                "wifi_callbacks",
                                kCallbackTaskStackWords,
                                nullptr,
                                followup_task_config::kPriorityWifiCallbacks,
                                &s_callback_task,
                                followup_task_config::kSystemCore) != pdPASS) {
        s_callback_task = nullptr;
        return ESP_ERR_NO_MEM;
    }

    ReloadSavedCredentials();
    LoadAccessPointPassword();
    s_initialized = true;
    ESP_LOGI(kTag, "Wi-Fi service initialized");
    return ESP_OK;
}

void Start()
{
    if (!s_initialized && Init() != ESP_OK) {
        return;
    }
    QueueTransition(TransitionRequest::kStart);
}

void SetEventHandler(EventHandler handler, void* context)
{
    std::lock_guard<std::mutex> lock(s_callback_mutex);
    s_event_handler = handler;
    s_event_context = context;
}

void SetScanDeferProvider(ScanDeferProvider provider, void* context)
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    s_scan_defer_provider = provider;
    s_scan_defer_context = context;
}

void SetWifiEnabled(bool enabled)
{
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_wifi_enabled = enabled;
        ResetRetryStateLocked();
        s_reconnect_suspended = false;
        s_user_disconnected = false;
    }
    StopBackgroundRetry();
    QueueTransition(enabled ? TransitionRequest::kStart : TransitionRequest::kStopWifi);
}

void SetAccessPointEnabled(bool enabled)
{
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_access_point_mode = enabled;
        if (enabled) {
            s_wifi_enabled = true;
        }
        ResetRetryStateLocked();
        s_reconnect_suspended = false;
        s_user_disconnected = false;
    }
    StopBackgroundRetry();
    QueueTransition(enabled ? TransitionRequest::kEnterAccessPoint
                            : TransitionRequest::kDisableAccessPoint);
}

void EnterAccessPointMode()
{
    SetAccessPointEnabled(true);
}

bool ConnectToNetwork(const std::string& ssid, const std::string& password, bool save_on_success)
{
    if (ssid.empty() || ssid.size() >= 65 || password.size() >= 65) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_wifi_enabled = true;
        s_active_credentials = {.ssid = ssid, .password = password};
        s_persist_active_credentials_on_success = save_on_success;
        s_current_ssid = ssid;
        ResetRetryStateLocked();
        s_reconnect_suspended = false;
        s_user_disconnected = false;
        s_resume_after_scan = false;
    }
    StopBackgroundRetry();

    return QueueTransition(TransitionRequest::kStartStation);
}

bool ConnectToSavedNetwork(const std::string& ssid)
{
    Credentials credentials;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        const Credentials* saved = FindSavedNetworkLocked(ssid);
        if (saved == nullptr) {
            return false;
        }
        credentials = *saved;
    }
    // Saving again on success moves it to the front of the most-recently-used list.
    return ConnectToNetwork(credentials.ssid, credentials.password, true);
}

bool ForgetNetwork(const std::string& ssid)
{
    if (ssid.empty()) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (FindSavedNetworkLocked(ssid) == nullptr) {
            return false;
        }
        s_forget_ssid = ssid;
    }
    // NVS writes must run on an internal-RAM stack; the transition worker has one.
    return QueueTransition(TransitionRequest::kForgetNetwork);
}

bool IsNetworkSaved(const std::string& ssid)
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return FindSavedNetworkLocked(ssid) != nullptr;
}

std::vector<std::string> GetSavedNetworkSsids()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    std::vector<std::string> ssids;
    ssids.reserve(s_saved_networks.size());
    for (const Credentials& network : s_saved_networks) {
        ssids.push_back(network.ssid);
    }
    return ssids;
}

bool DisconnectFromNetwork(bool clear_saved_credentials)
{
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_clear_saved_credentials_on_disconnect = clear_saved_credentials;
    }
    return QueueTransition(TransitionRequest::kDisconnectStation);
}

bool StartNetworkScan()
{
    InitializeStack();
    bool preempt_connection = false;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        if (!s_wifi_enabled) {
            return false;
        }
        if (s_scan_snapshot.state == ScanState::kRunning) {
            // Already scanning; that scan is the answer to this request.
            return true;
        }

        // Preempt the reconnect loop only when we are NOT associated. A scan runs fine
        // against a live connection -- esp_wifi_scan_start is rejected for "connecting",
        // not "connected" -- and the Wi-Fi page starts a scan on every entry, so standing
        // the loop down unconditionally tore down a perfectly good link just for opening
        // the page. When we are mid-connect the loop does have to stand down, because
        // esp_wifi_connect aborts a running scan without ever delivering SCAN_DONE. It
        // is paused only for the scan: when it finishes, ResumeAfterScan joins the strongest
        // saved network it found (or falls back to background retries).
        preempt_connection = !s_connected;
        if (preempt_connection) {
            s_reconnect_suspended = true;
            s_reconnecting = false;
            s_resume_after_scan = !s_access_point_mode;
        }
        s_scan_snapshot.state = ScanState::kRunning;
        s_scan_snapshot.last_error = ESP_OK;
        s_scan_snapshot.networks.clear();
    }

    // Only drop the pending connect deadline when the scan is going to tear the
    // association down; an established link keeps its timer state untouched.
    if (preempt_connection) {
        StopBackgroundRetry();
        CheckOrAbort(esp_timer_stop(s_connect_timer), "esp_timer_stop");
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_connect_timer_active = false;
    }

    if (s_scan_timeout_timer != nullptr) {
        CheckOrAbort(esp_timer_stop(s_scan_timeout_timer), "esp_timer_stop");
        const esp_err_t timer_err = esp_timer_start_once(s_scan_timeout_timer, kScanTimeoutUs);
        if (timer_err != ESP_OK) {
            ESP_LOGW(kTag, "Scan watchdog arm failed: %s", esp_err_to_name(timer_err));
        }
    }

    if (!QueueTransition(TransitionRequest::kStartScan)) {
        ResolveInFlightScan(ESP_ERR_NO_MEM);
        return false;
    }

    Notify(State::kScanning, "NETWORK_SCAN");
    return true;
}

bool ClearSavedCredentials()
{
    return ClearCredentialsFromNvs();
}

void RecoverAfterLightSleep()
{
    if (!s_initialized) {
        return;
    }

    bool wifi_enabled = false;
    bool connected = false;
    bool access_point_mode = false;
    bool reconnecting = false;
    bool has_credentials = false;
    bool reconnect_suspended = false;
    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        wifi_enabled = s_wifi_enabled;
        connected = s_connected;
        access_point_mode = s_access_point_mode;
        reconnecting = s_reconnecting;
        reconnect_suspended = s_reconnect_suspended;
        has_credentials = ResolveStationCredentialsLocked().valid();
    }

    if (!wifi_enabled) {
        ESP_LOGI(kTag, "Skipping Wi-Fi recovery after light sleep; Wi-Fi disabled");
        return;
    }

    if (reconnect_suspended && !access_point_mode) {
        // Either the user disconnected on purpose, or a scan has the loop paused and will
        // resume it when it finishes. Either way, recovery here would fight that.
        ESP_LOGI(kTag, "Skipping Wi-Fi recovery after light sleep; reconnect paused");
        return;
    }

    if (access_point_mode) {
        wifi_mode_t current_mode = WIFI_MODE_NULL;
        const esp_err_t mode_err = esp_wifi_get_mode(&current_mode);
        if (mode_err == ESP_OK &&
            (current_mode == WIFI_MODE_AP || current_mode == WIFI_MODE_APSTA)) {
            ESP_LOGI(kTag, "Wi-Fi AP mode still active after light sleep");
            return;
        }

        ESP_LOGI(kTag, "Recovering Wi-Fi AP mode after light sleep");
        (void)QueueTransition(TransitionRequest::kEnterAccessPoint);
        return;
    }

    bool link_healthy = connected;
    if (connected) {
        wifi_ap_record_t ap_info = {};
        const esp_err_t ap_err = esp_wifi_sta_get_ap_info(&ap_info);
        link_healthy = ap_err == ESP_OK;
        if (!link_healthy) {
            ESP_LOGW(kTag,
                     "Wi-Fi link check after light sleep failed: %s",
                     esp_err_to_name(ap_err));
        }
    }

    if ((connected && link_healthy) || reconnecting || !has_credentials) {
        ESP_LOGI(kTag,
                 "Wi-Fi recovery after light sleep not needed: connected=%d link_healthy=%d reconnecting=%d has_credentials=%d",
                 connected ? 1 : 0,
                 link_healthy ? 1 : 0,
                 reconnecting ? 1 : 0,
                 has_credentials ? 1 : 0);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_connected = false;
        s_reconnecting = true;
        s_ip_address.clear();
        s_rssi = 0;
    }

    ESP_LOGI(kTag, "Recovering Wi-Fi station after light sleep");
    if (!QueueTransition(TransitionRequest::kStartStation)) {
        std::lock_guard<std::mutex> lock(s_state_mutex);
        s_reconnecting = false;
        ESP_LOGW(kTag, "Wi-Fi recovery queue request failed after light sleep");
    }
}

UiState GetUiState()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return BuildUiStateLocked();
}

ScanSnapshot GetScanSnapshot()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return s_scan_snapshot;
}

bool IsConnected()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return s_connected;
}

bool IsAccessPointMode()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return s_access_point_mode;
}

bool IsBusy()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return s_connect_timer_active || s_reconnecting || s_scan_snapshot.state == ScanState::kRunning;
}

bool HasSavedCredentials()
{
    std::lock_guard<std::mutex> lock(s_state_mutex);
    return s_saved_credentials.valid();
}

const char* StateName(State state)
{
    switch (state) {
        case State::kIdle:
            return "idle";
        case State::kScanning:
            return "scanning";
        case State::kScanCompleted:
            return "scan_completed";
        case State::kScanFailed:
            return "scan_failed";
        case State::kConnecting:
            return "connecting";
        case State::kConnected:
            return "connected";
        case State::kDisconnected:
            return "disconnected";
        case State::kAccessPointMode:
            return "access_point";
        default:
            return "unknown";
    }
}

}  // namespace wifi_service
