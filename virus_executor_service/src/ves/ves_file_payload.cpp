#include "ves/ves_file_payload.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "memrpc/core/byte_reader.h"
#include "memrpc/core/byte_writer.h"
#include "memrpc/core/codec.h"
#include "memrpc/core/protocol.h"
#include "virus_protection_executor_log.h"

namespace VirusExecutorService {
namespace {

constexpr const char* DEFAULT_FILE_PAYLOAD_DIR = "/tmp/virus_executor_service_file_payload";
constexpr const char* FILE_PAYLOAD_DIR_ENV = "VES_FILE_PAYLOAD_DIR";
constexpr const char* FILE_PAYLOAD_PREFIX = "ves_file_payload_";
constexpr const char* LEGACY_FILE_PAYLOAD_PREFIX = "ves_payload_";
constexpr const char* FILE_PAYLOAD_PROCESS_DIR_PREFIX = "pid_";
constexpr const char* TMP_FILE_SUFFIX = ".tmp";
constexpr uint32_t FILE_PAYLOAD_ENVELOPE_MAGIC = 0x56465031U;  // VFP1
constexpr uint32_t FILE_PAYLOAD_ENVELOPE_VERSION = 1U;
constexpr std::size_t MAX_FILE_PAYLOAD_BYTES = 64ULL * 1024ULL * 1024ULL;

struct FilePayloadRef {
    std::string path;
    uint32_t size = 0;
};

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
    HILOGE("file payload mkdir failed path=%{public}s errno=%{public}d", dir.c_str(), errno);
    return false;
}

bool HasPrefix(const char* name, const char* prefix)
{
    return std::strncmp(name, prefix, std::strlen(prefix)) == 0;
}

bool IsDirectory(const std::string& path)
{
    struct stat st {};
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

std::string MakeFilePayloadPath(const std::string& dir, bool temporary)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    std::random_device rd;
    std::mt19937_64 rng((static_cast<uint64_t>(now) << 1U) ^ rd());
    std::string path;
    for (int attempt = 0; attempt < 16; ++attempt) {
        path = dir + "/" + FILE_PAYLOAD_PREFIX + std::to_string(getpid()) + "_" + std::to_string(now) + "_" +
               std::to_string(rng());
        if (temporary) {
            path += TMP_FILE_SUFFIX;
        }
        if (access(path.c_str(), F_OK) != 0) {
            return path;
        }
    }
    return {};
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

std::string ResolveFilePayloadRoot()
{
    const char* overrideDir = std::getenv(FILE_PAYLOAD_DIR_ENV);
    if (overrideDir != nullptr && overrideDir[0] != '\0') {
        return overrideDir;
    }
    return DEFAULT_FILE_PAYLOAD_DIR;
}

std::string ResolveProcessFilePayloadDir()
{
    return ResolveFilePayloadRoot() + "/" + FILE_PAYLOAD_PROCESS_DIR_PREFIX + std::to_string(getpid());
}

bool IsFilePayloadName(const char* name)
{
    return HasPrefix(name, FILE_PAYLOAD_PREFIX) || HasPrefix(name, LEGACY_FILE_PAYLOAD_PREFIX);
}

bool ClearFilePayloadsInDir(const std::string& dir)
{
    DIR* stream = opendir(dir.c_str());
    if (stream == nullptr) {
        if (errno == ENOENT) {
            return true;
        }
        HILOGE("file payload opendir failed path=%{public}s errno=%{public}d", dir.c_str(), errno);
        return false;
    }
    while (dirent* entry = readdir(stream)) {
        if (!IsFilePayloadName(entry->d_name)) {
            continue;
        }
        const std::string path = dir + "/" + entry->d_name;
        if (unlink(path.c_str()) != 0 && errno != ENOENT) {
            HILOGW("file payload unlink failed path=%{public}s errno=%{public}d", path.c_str(), errno);
        }
    }
    closedir(stream);
    return true;
}

std::string ParentDir(const std::string& path)
{
    const std::size_t slash = path.rfind('/');
    if (slash == std::string::npos) {
        return {};
    }
    return path.substr(0, slash);
}

void RemoveProcessDirIfEmpty(const std::string& root, const std::string& path)
{
    const std::string parent = ParentDir(path);
    if (parent.empty() || parent == root || !IsPathInsideFilePayloadRoot(root, parent)) {
        return;
    }
    if (rmdir(parent.c_str()) != 0 && errno != ENOENT && errno != ENOTEMPTY) {
        HILOGW("file payload rmdir failed path=%{public}s errno=%{public}d", parent.c_str(), errno);
    }
}

bool WriteFilePayload(const std::vector<uint8_t>& payload, FilePayloadRef* ref)
{
    if (ref == nullptr || payload.size() > std::numeric_limits<uint32_t>::max() ||
        payload.size() > MAX_FILE_PAYLOAD_BYTES) {
        return false;
    }
    const std::string root = ResolveFilePayloadRoot();
    if (!EnsureFilePayloadDir(root)) {
        return false;
    }
    const std::string dir = ResolveProcessFilePayloadDir();
    if (!EnsureFilePayloadDir(dir)) {
        return false;
    }
    const std::string tmpPath = MakeFilePayloadPath(dir, true);
    if (tmpPath.empty()) {
        return false;
    }
    const std::string finalPath = tmpPath.substr(0, tmpPath.size() - std::strlen(TMP_FILE_SUFFIX));
    const int fd = open(tmpPath.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) {
        HILOGE("file payload open failed path=%{public}s errno=%{public}d", tmpPath.c_str(), errno);
        return false;
    }
    bool ok = WriteAll(fd, payload.data(), payload.size());
    if (ok && fsync(fd) != 0) {
        HILOGE("file payload fsync failed path=%{public}s errno=%{public}d", tmpPath.c_str(), errno);
        ok = false;
    }
    if (close(fd) != 0) {
        ok = false;
    }
    if (ok && rename(tmpPath.c_str(), finalPath.c_str()) != 0) {
        HILOGE("file payload rename failed errno=%{public}d", errno);
        ok = false;
    }
    if (!ok) {
        unlink(tmpPath.c_str());
        unlink(finalPath.c_str());
        return false;
    }
    ref->path = finalPath;
    ref->size = static_cast<uint32_t>(payload.size());
    return true;
}

bool ReadAndRemoveFilePayload(const FilePayloadRef& ref, std::vector<uint8_t>* payload)
{
    const std::string root = ResolveFilePayloadRoot();
    if (payload == nullptr || ref.path.empty() || ref.size > MAX_FILE_PAYLOAD_BYTES ||
        !IsPathInsideFilePayloadRoot(root, ref.path)) {
        return false;
    }
    const int fd = open(ref.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        HILOGE("file payload open for read failed path=%{public}s errno=%{public}d", ref.path.c_str(), errno);
        unlink(ref.path.c_str());
        return false;
    }
    const bool ok = ReadAll(fd, payload);
    close(fd);
    if (unlink(ref.path.c_str()) != 0 && errno != ENOENT) {
        HILOGW("file payload consume unlink failed path=%{public}s errno=%{public}d", ref.path.c_str(), errno);
    }
    RemoveProcessDirIfEmpty(root, ref.path);
    if (!ok || payload->size() != ref.size) {
        HILOGE("file payload size mismatch expected=%{public}u actual=%{public}zu", ref.size, payload->size());
        payload->clear();
        return false;
    }
    return true;
}

bool EncodeFilePayloadEnvelope(const FilePayloadRef& ref, std::vector<uint8_t>* payload)
{
    MemRpc::ByteWriter writer;
    return writer.WriteUint32(FILE_PAYLOAD_ENVELOPE_MAGIC) && writer.WriteUint32(FILE_PAYLOAD_ENVELOPE_VERSION) &&
           writer.WriteUint32(ref.size) && writer.WriteString(ref.path) && MemRpc::detail::AssignBytes(writer, payload);
}

bool DecodeFilePayloadEnvelope(MemRpc::PayloadView payload, FilePayloadRef* ref)
{
    if (ref == nullptr) {
        return false;
    }
    MemRpc::ByteReader reader(payload.data(), payload.size());
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t size = 0;
    std::string path;
    if (!reader.ReadUint32(&magic) || magic != FILE_PAYLOAD_ENVELOPE_MAGIC) {
        return false;
    }
    if (!reader.ReadUint32(&version) || version != FILE_PAYLOAD_ENVELOPE_VERSION || !reader.ReadUint32(&size) ||
        !reader.ReadString(&path) || path.empty()) {
        return false;
    }
    ref->path = std::move(path);
    ref->size = size;
    return true;
}

}  // namespace

bool ClearVesFilePayloads()
{
    const std::string root = ResolveFilePayloadRoot();
    if (!EnsureFilePayloadDir(root)) {
        return false;
    }

    bool ok = ClearFilePayloadsInDir(root);
    const std::string processDir = ResolveProcessFilePayloadDir();
    if (IsDirectory(processDir)) {
        ok = ClearFilePayloadsInDir(processDir) && ok;
        if (rmdir(processDir.c_str()) != 0 && errno != ENOENT && errno != ENOTEMPTY) {
            HILOGW("file payload rmdir failed path=%{public}s errno=%{public}d", processDir.c_str(), errno);
        }
    }
    return ok;
}

namespace detail {

bool ReadVesFilePayloadForDecode(MemRpc::PayloadView payload, std::vector<uint8_t>* resolvedPayload)
{
    if (resolvedPayload == nullptr) {
        return false;
    }
    FilePayloadRef ref;
    return DecodeFilePayloadEnvelope(payload, &ref) && ReadAndRemoveFilePayload(ref, resolvedPayload);
}

}  // namespace detail

MemRpc::StatusCode PrepareVesFilePayloadForMemRpc(std::vector<uint8_t>* payload, MemRpc::RequestFlags* flags)
{
    if (payload == nullptr || flags == nullptr) {
        return MemRpc::StatusCode::InvalidArgument;
    }
    *flags &= static_cast<MemRpc::RequestFlags>(~VES_REQUEST_FLAG_FILE_PAYLOAD_REF);
    if (payload->size() <= MemRpc::DEFAULT_MAX_REQUEST_BYTES) {
        return MemRpc::StatusCode::Ok;
    }

    HILOGW("oversized VES request uses file payload: size=%{public}zu/%{public}zu",
           payload->size(),
           MemRpc::DEFAULT_MAX_REQUEST_BYTES);
    FilePayloadRef fileRef;
    std::vector<uint8_t> fileEnvelope;
    if (!WriteFilePayload(*payload, &fileRef) || !EncodeFilePayloadEnvelope(fileRef, &fileEnvelope) ||
        fileEnvelope.size() > MemRpc::DEFAULT_MAX_REQUEST_BYTES) {
        HILOGE("failed to prepare VES file payload size=%{public}zu", payload->size());
        return MemRpc::StatusCode::PayloadTooLarge;
    }
    *payload = std::move(fileEnvelope);
    *flags |= VES_REQUEST_FLAG_FILE_PAYLOAD_REF;
    return MemRpc::StatusCode::Ok;
}

}  // namespace VirusExecutorService
