#include "config_api_http.h"

#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <string>
#include <utility>

#include "config_api.h"
#include "esp_log.h"

namespace config_api::http {
namespace {

constexpr const char* kTag = "ConfigApiHttp";
constexpr size_t kMaxBodyLen = 512;

enum class Body : uint8_t {
    kNone,        // body ignored; the command gets empty args
    kJson,        // JSON object required
    kJsonOrForm,  // JSON object, or application/x-www-form-urlencoded
};

struct Route {
    const char* uri;
    httpd_method_t method;
    const char* command;
    Body body;
    const char* invalid_payload_message;  // reply when a required body is missing or too large
    const char* ok_message;               // overrides the command's success message, if set
};

// The setup portal's REST API. URIs, methods and reply shapes are what the portal page uses.
constexpr Route kRoutes[] = {
    {"/api/status", HTTP_GET, "wifi_status", Body::kNone, nullptr, nullptr},
    {"/api/scan", HTTP_GET, "wifi_scan", Body::kNone, nullptr, nullptr},
    {"/api/configure", HTTP_POST, "wifi_connect", Body::kJsonOrForm,
     "Invalid Wi-Fi configuration payload", nullptr},
    {"/api/disconnect", HTTP_POST, "wifi_disconnect", Body::kNone, nullptr, nullptr},
    {"/api/settings/gemini", HTTP_GET, "gemini_get", Body::kNone, nullptr,
     "Gemini settings loaded"},
    {"/api/settings/gemini", HTTP_PATCH, "gemini_set_key", Body::kJson,
     "Invalid Gemini settings payload", nullptr},
    {"/api/settings/gemini/reset", HTTP_POST, "gemini_clear_key", Body::kNone, nullptr, nullptr},
    {"/api/runtime/gemini", HTTP_GET, "gemini_get", Body::kNone, nullptr,
     "Gemini runtime loaded"},
    {"/api/settings/time", HTTP_GET, "time_get", Body::kNone, nullptr, nullptr},
    {"/api/settings/time", HTTP_PATCH, "time_set", Body::kJson,
     "Invalid time settings payload", nullptr},
    {"/api/runtime/time", HTTP_GET, "time_runtime", Body::kNone, nullptr, nullptr},
    {"/api/timezone/list", HTTP_GET, "timezone_list", Body::kNone, nullptr, nullptr},
};

std::string ReadRequestBody(httpd_req_t* request)
{
    std::string body(static_cast<size_t>(request->content_len), '\0');
    size_t offset = 0;
    while (offset < body.size()) {
        const int received = httpd_req_recv(request, body.data() + offset, body.size() - offset);
        if (received <= 0) {
            return {};
        }
        offset += static_cast<size_t>(received);
    }
    return body;
}

std::string UrlDecode(const std::string& value)
{
    std::string decoded;
    decoded.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '+') {
            decoded.push_back(' ');
            continue;
        }
        if (value[i] == '%' && i + 2 < value.size()) {
            char hex[3] = {value[i + 1], value[i + 2], '\0'};
            char* end = nullptr;
            const long parsed = std::strtol(hex, &end, 16);
            if (end != nullptr && *end == '\0') {
                decoded.push_back(static_cast<char>(parsed));
                i += 2;
                continue;
            }
        }
        decoded.push_back(value[i]);
    }
    return decoded;
}

// key=value&key=value into string args.
void ParseFormBody(const std::string& body, cJSON* args)
{
    size_t start = 0;
    while (start <= body.size()) {
        size_t end = body.find('&', start);
        if (end == std::string::npos) {
            end = body.size();
        }
        const std::string pair = body.substr(start, end - start);
        const size_t equals = pair.find('=');
        if (equals != std::string::npos && equals > 0) {
            const std::string key = UrlDecode(pair.substr(0, equals));
            if (!cJSON_HasObjectItem(args, key.c_str())) {
                cJSON_AddStringToObject(args, key.c_str(),
                                        UrlDecode(pair.substr(equals + 1)).c_str());
            }
        }
        start = end + 1;
    }
}

esp_err_t SendReply(httpd_req_t* request, Response response)
{
    cJSON* root = response.data != nullptr ? response.data.release() : cJSON_CreateObject();
    cJSON_AddBoolToObject(root, "success", response.ok);
    cJSON_AddStringToObject(root, "message", response.message.c_str());
    if (!response.ok) {
        if (!response.error_code.empty()) {
            cJSON_AddStringToObject(root, "error_code", response.error_code.c_str());
        }
        if (!response.field.empty()) {
            cJSON_AddStringToObject(root, "field", response.field.c_str());
        }
    }

    char* raw = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    const std::string payload = raw != nullptr ? raw : "{}";
    cJSON_free(raw);

    switch (response.status) {
        case 200:
            httpd_resp_set_status(request, HTTPD_200);
            break;
        case 400:
            httpd_resp_set_status(request, HTTPD_400);
            break;
        default:
            httpd_resp_set_status(request, HTTPD_500);
            break;
    }
    httpd_resp_set_type(request, "application/json; charset=utf-8");
    return httpd_resp_send(request, payload.c_str(), payload.size());
}

esp_err_t HandleRoute(httpd_req_t* request)
{
    const Route& route = *static_cast<const Route*>(request->user_ctx);
    JsonPtr args(cJSON_CreateObject());

    if (route.body != Body::kNone) {
        if (request->content_len <= 0 || request->content_len > static_cast<int>(kMaxBodyLen)) {
            return SendReply(request, Error(400, {}, route.invalid_payload_message));
        }
        const std::string body = ReadRequestBody(request);
        if (body.empty()) {
            return SendReply(request, Error(400, {}, route.invalid_payload_message));
        }
        if (route.body == Body::kJson || body.front() == '{') {
            JsonPtr parsed(cJSON_ParseWithLength(body.c_str(), body.size()));
            if (parsed == nullptr) {
                return SendReply(request, Error(400, {}, "Invalid JSON body"));
            }
            if (cJSON_IsObject(parsed.get())) {
                args = std::move(parsed);
            }
        } else {
            ParseFormBody(body, args.get());
        }
    }

    Response response = Dispatch(route.command, args.get());
    if (response.ok && route.ok_message != nullptr) {
        response.message = route.ok_message;
    }
    return SendReply(request, std::move(response));
}

}  // namespace

size_t RouteCount()
{
    return std::size(kRoutes);
}

esp_err_t RegisterRoutes(httpd_handle_t server)
{
    if (server == nullptr) {
        return ESP_ERR_INVALID_ARG;
    }

    esp_err_t first_error = ESP_OK;
    for (const Route& route : kRoutes) {
        const httpd_uri_t handler = {
            .uri = route.uri,
            .method = route.method,
            .handler = HandleRoute,
            .user_ctx = const_cast<Route*>(&route),
        };
        const esp_err_t err = httpd_register_uri_handler(server, &handler);
        if (err != ESP_OK) {
            ESP_LOGE(kTag, "Failed to register route %s [%d]: %s", route.uri,
                     static_cast<int>(route.method), esp_err_to_name(err));
            if (first_error == ESP_OK) {
                first_error = err;
            }
        }
    }
    return first_error;
}

}  // namespace config_api::http
