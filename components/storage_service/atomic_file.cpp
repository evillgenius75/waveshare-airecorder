#include "atomic_file.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "esp_log.h"

namespace atomic_file {
namespace {

constexpr const char* kTag = "AtomicFile";

bool PathExists(const std::string& path)
{
    struct stat info = {};
    return stat(path.c_str(), &info) == 0;
}

bool EndsWith(const std::string& value, const char* suffix)
{
    const size_t suffix_length = std::strlen(suffix);
    return value.size() > suffix_length &&
           value.compare(value.size() - suffix_length, suffix_length, suffix) == 0;
}

}  // namespace

bool Write(const std::string& path, const void* data, size_t size)
{
    const std::string temp_path = path + kTempSuffix;

    errno = 0;
    FILE* file = std::fopen(temp_path.c_str(), "wb");
    if (file == nullptr) {
        ESP_LOGW(kTag, "Open %s failed: errno=%d (%s)", temp_path.c_str(), errno,
                 std::strerror(errno));
        return false;
    }

    bool ok = size == 0 || std::fwrite(data, 1, size, file) == size;
    ok = ok && std::fflush(file) == 0;
    ok = ok && fsync(fileno(file)) == 0;
    const int write_errno = errno;
    ok = (std::fclose(file) == 0) && ok;
    if (!ok) {
        ESP_LOGW(kTag, "Write %s failed: errno=%d (%s)", temp_path.c_str(), write_errno,
                 std::strerror(write_errno));
        std::remove(temp_path.c_str());
        return false;
    }

    // FAT rename() fails when the destination exists, so remove the old file first. A power
    // cut between these two calls leaves only the fully written temp file, which
    // RecoverDirectory() promotes.
    if (PathExists(path) && std::remove(path.c_str()) != 0) {
        ESP_LOGW(kTag, "Remove old %s failed: errno=%d (%s)", path.c_str(), errno,
                 std::strerror(errno));
        std::remove(temp_path.c_str());
        return false;
    }
    if (std::rename(temp_path.c_str(), path.c_str()) != 0) {
        // The complete data is still in the temp file; recovery will promote it.
        ESP_LOGW(kTag, "Rename %s failed: errno=%d (%s)", temp_path.c_str(), errno,
                 std::strerror(errno));
        return false;
    }
    return true;
}

size_t RecoverDirectory(const std::string& directory)
{
    DIR* dir = opendir(directory.c_str());
    if (dir == nullptr) {
        return 0;
    }

    // Collect first: renaming or removing entries while readdir() walks the same FAT
    // directory is not safe.
    std::vector<std::string> temp_names;
    while (struct dirent* entry = readdir(dir)) {
        const std::string name = entry->d_name;
        if (EndsWith(name, kTempSuffix)) {
            temp_names.push_back(name);
        }
    }
    closedir(dir);

    size_t promoted = 0;
    for (const std::string& temp_name : temp_names) {
        const std::string temp_path = directory + "/" + temp_name;
        const std::string final_path =
            temp_path.substr(0, temp_path.size() - std::strlen(kTempSuffix));
        if (PathExists(final_path)) {
            ESP_LOGW(kTag, "Discarding interrupted write %s", temp_path.c_str());
            std::remove(temp_path.c_str());
        } else if (std::rename(temp_path.c_str(), final_path.c_str()) == 0) {
            ESP_LOGW(kTag, "Recovered %s from interrupted write", final_path.c_str());
            promoted++;
        } else {
            ESP_LOGW(kTag, "Recover %s failed: errno=%d (%s)", temp_path.c_str(), errno,
                     std::strerror(errno));
        }
    }
    return promoted;
}

}  // namespace atomic_file
