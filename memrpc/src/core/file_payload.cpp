#include "memrpc/core/file_payload.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "memrpc/core/byte_reader.h"
#include "memrpc/core/byte_writer.h"
#include "memrpc/core/codec.h"
#include "virus_protection_executor_log.h"

namespace MemRpc {
namespace {

std::string ParentDir(const std::string& path)
{
    const std::size_t slash = path.rfind('/');
    if (slash == std::string::npos || slash == 0) {
        return {};
    }
    return path.substr(0, slash);
}

bool EnsureFilePayloadDir(const std::string& dir)
{
    if (dir.empty()) {
        return false;
    }
    if (mkdir(dir.c_str(), 0700) == 0) {
        return true;
    }
    if (errno == EEXIST) {
        struct stat st {};
        return stat(dir.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
    }
    if (errno == ENOENT) {
        const std::string parent = ParentDir(dir);
        if (!parent.empty() && EnsureFilePayloadDir(parent) && mkdir(dir.c_str(), 0700) == 0) {
            return true;
        }
        if (errno == EEXIST) {
            struct stat st {};
            return stat(dir.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
        }
    }
    HILOGE("file payload mkdir failed path=%{public}s errno=%{public}d", dir.c_str(), errno);
    return false;
}

std::string MakeFilePayloadPath(const std::string& dir)
{
    static std::atomic<int> counter{0};
    const int seq = counter.fetch_add(1);
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return dir + "/" + std::to_string(getpid()) + "_" + std::to_string(ts) + "_" + std::to_string(seq);
}

bool WriteAll(int fd, const uint8_t* data, std::size_t size)
{
    std::size_t written = 0;
    while (written < size) {
        const ssize_t n = write(fd, data + written, size - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            HILOGE("file payload write failed errno=%{public}d", errno);
            return false;
        }
        if (n == 0) {
            return false;
        }
        written += static_cast<std::size_t>(n);
    }
    return true;
}

bool ReadAll(int fd, std::vector<uint8_t>* payload)
{
    if (payload == nullptr) {
        return false;
    }
    struct stat st {};
    if (fstat(fd, &st) != 0 || st.st_size < 0 ||
        static_cast<unsigned long long>(st.st_size) > static_cast<unsigned long long>(MAX_FILE_PAYLOAD_BYTES)) {
        HILOGE("file payload invalid file size errno=%{public}d", errno);
        return false;
    }
    payload->resize(static_cast<std::size_t>(st.st_size));
    std::size_t offset = 0;
    while (offset < payload->size()) {
        const ssize_t n = read(fd, payload->data() + offset, payload->size() - offset);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            HILOGE("file payload read failed errno=%{public}d", errno);
            return false;
        }
        if (n == 0) {
            return false;
        }
        offset += static_cast<std::size_t>(n);
    }
    return true;
}

bool IsPathInsideFilePayloadRoot(const std::string& dir, const std::string& path)
{
    const std::string prefix = dir + "/";
    return path.rfind(prefix, 0) == 0 && path.find("..") == std::string::npos;
}

bool UnlinkFilePayload(const std::string& dir, const FilePayloadRef& ref, bool logWarning)
{
    if (ref.path.empty() || ref.size > MAX_FILE_PAYLOAD_BYTES || !IsPathInsideFilePayloadRoot(dir, ref.path)) {
        return false;
    }
    if (unlink(ref.path.c_str()) != 0 && errno != ENOENT) {
        if (logWarning) {
            HILOGW("file payload unlink failed path=%{public}s errno=%{public}d", ref.path.c_str(), errno);
        } else {
            HILOGE("file payload unlink failed path=%{public}s errno=%{public}d", ref.path.c_str(), errno);
        }
        return false;
    }
    return true;
}

}  // namespace

bool ClearFilePayloads(const std::string& dir)
{
    if (!EnsureFilePayloadDir(dir)) {
        return false;
    }

    DIR* stream = opendir(dir.c_str());
    if (stream == nullptr) {
        if (errno == ENOENT) {
            return true;
        }
        HILOGE("file payload opendir failed path=%{public}s errno=%{public}d", dir.c_str(), errno);
        return false;
    }

    while (dirent* entry = readdir(stream)) {
        const char* name = entry->d_name;
        if (std::strcmp(name, ".") == 0 || std::strcmp(name, "..") == 0) {
            continue;
        }
        const std::string path = dir + "/" + name;
        struct stat st {};
        if (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
            continue;
        }
        if (unlink(path.c_str()) != 0 && errno != ENOENT) {
            HILOGW("file payload unlink failed path=%{public}s errno=%{public}d", path.c_str(), errno);
        }
    }
    closedir(stream);
    return true;
}

bool RemoveFilePayload(const std::string& dir, const FilePayloadRef& ref)
{
    return UnlinkFilePayload(dir, ref, false);
}

bool WriteFilePayload(const std::string& dir, const std::vector<uint8_t>& payload, FilePayloadRef* ref)
{
    if (ref == nullptr || payload.size() > std::numeric_limits<uint32_t>::max() ||
        payload.size() > MAX_FILE_PAYLOAD_BYTES) {
        return false;
    }
    if (!EnsureFilePayloadDir(dir)) {
        return false;
    }

    const std::string path = MakeFilePayloadPath(dir);
    const int fd = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) {
        HILOGE("file payload open failed path=%{public}s errno=%{public}d", path.c_str(), errno);
        return false;
    }

    bool ok = WriteAll(fd, payload.data(), payload.size());
    if (close(fd) != 0) {
        ok = false;
    }
    if (!ok) {
        unlink(path.c_str());
        return false;
    }

    ref->path = path;
    ref->size = static_cast<uint32_t>(payload.size());
    return true;
}

bool ReadAndRemoveFilePayload(const std::string& dir, const FilePayloadRef& ref, std::vector<uint8_t>* payload)
{
    if (payload == nullptr || ref.path.empty() || ref.size > MAX_FILE_PAYLOAD_BYTES ||
        !IsPathInsideFilePayloadRoot(dir, ref.path)) {
        return false;
    }

    const int fd = open(ref.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        HILOGE("file payload open for read failed path=%{public}s errno=%{public}d", ref.path.c_str(), errno);
        (void)RemoveFilePayload(dir, ref);
        return false;
    }

    const bool ok = ReadAll(fd, payload);
    close(fd);
    if (!RemoveFilePayload(dir, ref)) {
        HILOGW("file payload consume unlink failed path=%{public}s", ref.path.c_str());
    }
    if (!ok || payload->size() != ref.size) {
        HILOGE("file payload size mismatch expected=%{public}u actual=%{public}zu", ref.size, payload->size());
        payload->clear();
        return false;
    }
    return true;
}

bool EncodeFilePayloadEnvelope(const FilePayloadRef& ref, std::vector<uint8_t>* payload)
{
    if (payload == nullptr) {
        return false;
    }
    ByteWriter writer;
    return writer.WriteUint32(FILE_PAYLOAD_ENVELOPE_MAGIC) && writer.WriteUint32(FILE_PAYLOAD_ENVELOPE_VERSION) &&
           writer.WriteUint32(ref.size) && writer.WriteString(ref.path) && detail::AssignBytes(writer, payload);
}

bool DecodeFilePayloadEnvelope(const uint8_t* bytes, std::size_t size, FilePayloadRef* ref)
{
    if (ref == nullptr) {
        return false;
    }
    ByteReader reader(bytes, size);
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t payloadSize = 0;
    std::string path;
    if (!reader.ReadUint32(&magic) || magic != FILE_PAYLOAD_ENVELOPE_MAGIC) {
        return false;
    }
    if (!reader.ReadUint32(&version) || version != FILE_PAYLOAD_ENVELOPE_VERSION || !reader.ReadUint32(&payloadSize) ||
        !reader.ReadString(&path) || path.empty()) {
        return false;
    }
    ref->path = std::move(path);
    ref->size = payloadSize;
    return true;
}

bool PrepareFilePayloadForTransport(const std::string& dir,
                                    const std::vector<uint8_t>& payload,
                                    std::size_t maxTransportBytes,
                                    std::vector<uint8_t>* transportPayload,
                                    uint8_t* payloadKind)
{
    if (transportPayload == nullptr || payloadKind == nullptr) {
        return false;
    }

    transportPayload->clear();
    *payloadKind = PAYLOAD_KIND_INLINE;

    if (payload.size() <= maxTransportBytes) {
        *transportPayload = payload;
        return true;
    }

    FilePayloadRef fileRef;
    std::vector<uint8_t> envelope;
    if (!WriteFilePayload(dir, payload, &fileRef) || !EncodeFilePayloadEnvelope(fileRef, &envelope)) {
        (void)RemoveFilePayload(dir, fileRef);
        return false;
    }
    if (envelope.size() > maxTransportBytes) {
        (void)RemoveFilePayload(dir, fileRef);
        return false;
    }

    *transportPayload = std::move(envelope);
    *payloadKind = PAYLOAD_KIND_FILE_REF;
    return true;
}

bool RemoveFilePayloadFromTransport(const std::string& dir,
                                    const uint8_t* bytes,
                                    std::size_t size,
                                    uint8_t payloadKind)
{
    if (payloadKind == PAYLOAD_KIND_INLINE) {
        return true;
    }
    if (payloadKind != PAYLOAD_KIND_FILE_REF) {
        return false;
    }

    FilePayloadRef fileRef;
    return DecodeFilePayloadEnvelope(bytes, size, &fileRef) && RemoveFilePayload(dir, fileRef);
}

bool ResolveFilePayloadFromTransport(const std::string& dir,
                                     const uint8_t* bytes,
                                     std::size_t size,
                                     uint8_t payloadKind,
                                     std::vector<uint8_t>* payload)
{
    if (payload == nullptr) {
        return false;
    }
    payload->clear();
    if (payloadKind == PAYLOAD_KIND_INLINE) {
        if (size == 0U) {
            return true;
        }
        if (bytes == nullptr && size != 0U) {
            return false;
        }
        payload->assign(bytes, bytes + size);
        return true;
    }
    if (payloadKind != PAYLOAD_KIND_FILE_REF) {
        return false;
    }

    FilePayloadRef fileRef;
    return DecodeFilePayloadEnvelope(bytes, size, &fileRef) && ReadAndRemoveFilePayload(dir, fileRef, payload);
}

}  // namespace MemRpc
