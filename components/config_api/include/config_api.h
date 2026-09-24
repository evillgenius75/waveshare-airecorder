#ifndef CONFIG_API_H_
#define CONFIG_API_H_

#include <memory>
#include <string>

#include "cJSON.h"

// Transport-agnostic configuration commands. Services register named commands; transports
// (the setup portal's HTTP routes today, BLE later) turn their requests into a command name
// plus a JSON args object and dispatch here, so every transport shares one implementation.
//
// Wire protocol (HandleMessage), version 1:
//   request   {"v":1, "id":7, "cmd":"wifi_connect", "args":{"ssid":"Home","password":"..."}}
//   success   {"v":1, "id":7, "ok":true, "result":{...}}
//   failure   {"v":1, "id":7, "ok":false, "error":{"code":"...", "message":"...", "field":"..."}}
namespace config_api {

constexpr int kProtocolVersion = 1;

struct JsonDeleter {
    void operator()(cJSON* json) const { cJSON_Delete(json); }
};
using JsonPtr = std::unique_ptr<cJSON, JsonDeleter>;

// Outcome of one command. `status` uses HTTP meanings (200 ok, 400 bad args, 500 failure) so
// the HTTP adapter can pass it through unchanged.
struct Response {
    bool ok = true;
    int status = 200;
    std::string message;
    std::string error_code;  // empty on success; transports substitute a generic code if unset
    std::string field;       // the offending argument, for validation errors
    JsonPtr data;            // result object; on failure, optional extra state
};

Response Ok(std::string message, JsonPtr data = nullptr);
Response Error(int status, std::string error_code, std::string message,
               std::string field = {}, JsonPtr data = nullptr);

// `args` is always a JSON object, possibly empty. Handlers run on the calling transport's
// task, which must have an internal-RAM stack: several commands write NVS.
using Handler = Response (*)(const cJSON* args);

// Registers or replaces a command. Call from service Init, before any transport starts.
void RegisterCommand(const char* name, Handler handler);

// Runs one command. Unknown commands fail with error code "unknown_command".
Response Dispatch(const std::string& command, const cJSON* args);

// Handles one protocol message (see above) and returns the serialized reply.
std::string HandleMessage(const std::string& message);

// Argument helpers: the value when `key` holds the expected type, else the fallback.
std::string StringArg(const cJSON* args, const char* key, const std::string& fallback = {});

}  // namespace config_api

#endif  // CONFIG_API_H_
