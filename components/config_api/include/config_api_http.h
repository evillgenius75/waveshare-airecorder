#ifndef CONFIG_API_HTTP_H_
#define CONFIG_API_HTTP_H_

#include "esp_err.h"
#include "esp_http_server.h"

// HTTP transport for config_api: the setup portal's REST routes (/api/status, /api/configure,
// /api/settings/gemini, /api/settings/time, ...). Each route maps to one command, and replies
// keep the portal's original shape: {"success":bool, "message":"...", ...result fields}.
namespace config_api::http {

// Number of httpd URI handlers RegisterRoutes adds; size httpd max_uri_handlers with it.
size_t RouteCount();

// Registers every portal route on `server`. Returns the first registration error, if any.
esp_err_t RegisterRoutes(httpd_handle_t server);

}  // namespace config_api::http

#endif  // CONFIG_API_HTTP_H_
