#pragma once

#include <cstddef>
#include <string>

// Crash-safe small-file writes for SD card sidecars (recording metadata, transcripts,
// summaries).
//
// A plain fopen("wb") truncates the real file before the new bytes land, so a power cut
// mid-write leaves an empty or partial file. Write() instead writes "<path>.tmp", flushes and
// fsyncs it, then swaps it into place. FAT's rename() cannot replace an existing file, so the
// old file is unlinked first; if power is lost in that short window, only "<path>.tmp"
// survives, and RecoverDirectory() promotes it on the next scan.
namespace atomic_file {

inline constexpr const char* kTempSuffix = ".tmp";

// Writes data to path atomically (from the reader's point of view). Returns false and leaves
// any existing file untouched if the temp file cannot be fully written.
bool Write(const std::string& path, const void* data, size_t size);

inline bool Write(const std::string& path, const std::string& text)
{
    return Write(path, text.data(), text.size());
}

// Finishes or discards interrupted writes in one directory: a "<name>.tmp" with no "<name>"
// is promoted; a "<name>.tmp" next to an intact "<name>" is a torn write and is removed.
// Returns the number of files promoted.
size_t RecoverDirectory(const std::string& directory);

}  // namespace atomic_file
