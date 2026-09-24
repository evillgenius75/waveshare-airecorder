#include "config_api.h"

#include <mutex>
#include <utility>
#include <vector>

#include "esp_app_desc.h"
#include "esp_log.h"

namespace config_api {
namespace {

constexpr const char* kTag = "ConfigApi";

struct Command {
    std::string name;
    Handler handler = nullptr;
};

std::mutex s_mutex;
std::vector<Command> s_commands;

Response HandleHello(const cJSON*)
{
    const esp_app_desc_t* app = esp_app_get_description();
    JsonPtr data(cJSON_CreateObject());
    cJSON_AddNumberToObject(data.get(), "protocol", kProtocolVersion);
    cJSON_AddStringToObject(data.get(), "project", app->project_name);
    cJSON_AddStringToObject(data.get(), "firmware", app->version);
    cJSON* commands = cJSON_AddArrayToObject(data.get(), "commands");
    {
        std::lock_guard<std::mutex> lock(s_mutex);
        for (const Command& command : s_commands) {
            cJSON_AddItemToArray(commands, cJSON_CreateString(command.name.c_str()));
        }
    }
    return Ok("Hello", std::move(data));
}

Handler FindHandler(const std::string& name)
{
    if (name == "hello") {
        return HandleHello;
    }
    std::lock_guard<std::mutex> lock(s_mutex);
    for (const Command& command : s_commands) {
        if (command.name == name) {
            return command.handler;
        }
    }
    return nullptr;
}

std::string Serialize(cJSON* root)
{
    char* raw = cJSON_PrintUnformatted(root);
    if (raw == nullptr) {
        return "{}";
    }
    std::string json(raw);
    cJSON_free(raw);
    return json;
}

const char* FallbackErrorCode(int status)
{
    return status == 400 ? "invalid_args" : "internal_error";
}

std::string ErrorReply(const cJSON* id, const std::string& code, const std::string& message,
                       const std::string& field = {})
{
    JsonPtr reply(cJSON_CreateObject());
    cJSON_AddNumberToObject(reply.get(), "v", kProtocolVersion);
    if (id != nullptr) {
        cJSON_AddItemToObject(reply.get(), "id", cJSON_Duplicate(id, true));
    }
    cJSON_AddBoolToObject(reply.get(), "ok", false);
    cJSON* error = cJSON_AddObjectToObject(reply.get(), "error");
    cJSON_AddStringToObject(error, "code", code.c_str());
    cJSON_AddStringToObject(error, "message", message.c_str());
    if (!field.empty()) {
        cJSON_AddStringToObject(error, "field", field.c_str());
    }
    return Serialize(reply.get());
}

}  // namespace

Response Ok(std::string message, JsonPtr data)
{
    Response response;
    response.message = std::move(message);
    response.data = std::move(data);
    return response;
}

Response Error(int status, std::string error_code, std::string message, std::string field,
               JsonPtr data)
{
    Response response;
    response.ok = false;
    response.status = status;
    response.error_code = std::move(error_code);
    response.message = std::move(message);
    response.field = std::move(field);
    response.data = std::move(data);
    return response;
}

void RegisterCommand(const char* name, Handler handler)
{
    if (name == nullptr || handler == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(s_mutex);
    for (Command& command : s_commands) {
        if (command.name == name) {
            command.handler = handler;
            return;
        }
    }
    s_commands.push_back({name, handler});
}

Response Dispatch(const std::string& command, const cJSON* args)
{
    const Handler handler = FindHandler(command);
    if (handler == nullptr) {
        ESP_LOGW(kTag, "Unknown command: %s", command.c_str());
        return Error(500, "unknown_command", "Unknown command: " + command);
    }

    JsonPtr empty_args;
    if (!cJSON_IsObject(args)) {
        empty_args.reset(cJSON_CreateObject());
        args = empty_args.get();
    }
    return handler(args);
}

std::string HandleMessage(const std::string& message)
{
    JsonPtr request(cJSON_ParseWithLength(message.c_str(), message.size()));
    if (!cJSON_IsObject(request.get())) {
        return ErrorReply(nullptr, "bad_request", "Invalid JSON message");
    }

    const cJSON* id = cJSON_GetObjectItemCaseSensitive(request.get(), "id");
    const cJSON* version = cJSON_GetObjectItemCaseSensitive(request.get(), "v");
    if (!cJSON_IsNumber(version) || version->valueint != kProtocolVersion) {
        return ErrorReply(id, "unsupported_version", "Protocol version 1 required");
    }
    const cJSON* command = cJSON_GetObjectItemCaseSensitive(request.get(), "cmd");
    if (!cJSON_IsString(command) || command->valuestring == nullptr) {
        return ErrorReply(id, "bad_request", "cmd required");
    }

    const Response response =
        Dispatch(command->valuestring, cJSON_GetObjectItemCaseSensitive(request.get(), "args"));
    if (!response.ok) {
        return ErrorReply(id,
                          response.error_code.empty() ? FallbackErrorCode(response.status)
                                                      : response.error_code,
                          response.message, response.field);
    }

    JsonPtr reply(cJSON_CreateObject());
    cJSON_AddNumberToObject(reply.get(), "v", kProtocolVersion);
    if (id != nullptr) {
        cJSON_AddItemToObject(reply.get(), "id", cJSON_Duplicate(id, true));
    }
    cJSON_AddBoolToObject(reply.get(), "ok", true);
    cJSON_AddItemToObject(reply.get(), "result",
                          response.data != nullptr ? cJSON_Duplicate(response.data.get(), true)
                                                   : cJSON_CreateObject());
    return Serialize(reply.get());
}

std::string StringArg(const cJSON* args, const char* key, const std::string& fallback)
{
    const cJSON* item = cJSON_GetObjectItemCaseSensitive(args, key);
    if (cJSON_IsString(item) && item->valuestring != nullptr) {
        return item->valuestring;
    }
    return fallback;
}

}  // namespace config_api
