#include "io/cache/file_segment.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <thread>

#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "vec/common/hex.h"

namespace doris {
namespace io {

FileSegment::FileSegment(size_t offset_, size_t size_, const Key& key_, IFileCache* cache_,
                         State download_state_)
        : _segment_range(offset_, offset_ + size_ - 1),
          _download_state(download_state_),
          _file_key(key_),
          _cache(cache_) {
    /// On creation, file segment state can be EMPTY, DOWNLOADED, DOWNLOADING.
    switch (_download_state) {
    /// EMPTY is used when file segment is not in cache and
    /// someone will _potentially_ want to download it (after calling getOrSetDownloader()).
    case (State::EMPTY): {
        break;
    }
    /// DOWNLOADED is used either on initial cache metadata load into memory on server startup
    /// or on reduceSizeToDownloaded() -- when file segment object is updated.
    case (State::DOWNLOADED): {
        _reserved_size = _downloaded_size = size_;
        break;
    }
    /// DOWNLOADING is used only for write-through caching (e.g. getOrSetDownloader() is not
    /// needed, downloader is set on file segment creation).
    case (State::DOWNLOADING): {
        _downloader_id = get_caller_id();
        break;
    }
    default: {
        DCHECK(false) << "Can create cell with either EMPTY, DOWNLOADED, DOWNLOADING ";
    }
    }
}

FileSegment::State FileSegment::state() const {
    std::lock_guard segment_lock(_mutex);
    return _download_state;
}

size_t FileSegment::get_download_offset() const {
    std::lock_guard segment_lock(_mutex);
    return range().left + get_downloaded_size(segment_lock);
}

size_t FileSegment::get_downloaded_size() const {
    std::lock_guard segment_lock(_mutex);
    return get_downloaded_size(segment_lock);
}

size_t FileSegment::get_downloaded_size(std::lock_guard<std::mutex>& /* segment_lock */) const {
    if (_download_state == State::DOWNLOADED) {
        return _downloaded_size;
    }

    std::lock_guard download_lock(_download_mutex);
    return _downloaded_size;
}

std::string FileSegment::get_caller_id() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    return ss.str();
}

std::string FileSegment::get_or_set_downloader() {
    std::lock_guard segment_lock(_mutex);

    if (_downloader_id.empty()) {
        DCHECK(_download_state != State::DOWNLOADING);

        _downloader_id = get_caller_id();
        _download_state = State::DOWNLOADING;
    } else if (_downloader_id == get_caller_id()) {
        LOG(INFO) << "Attempt to set the same downloader for segment " << range().to_string()
                  << " for the second time";
    }

    return _downloader_id;
}

void FileSegment::reset_downloader() {
    std::lock_guard segment_lock(_mutex);

    DCHECK(!_downloader_id.empty()) << "There is no downloader";

    DCHECK(get_caller_id() == _downloader_id) << "Downloader can be reset only by downloader";

    reset_downloader_impl(segment_lock);
}

void FileSegment::reset_downloader_impl(std::lock_guard<std::mutex>& segment_lock) {
    if (_downloaded_size == range().size()) {
        set_downloaded(segment_lock);
    }

    _downloader_id.clear();
}

std::string FileSegment::get_downloader() const {
    std::lock_guard segment_lock(_mutex);
    return _downloader_id;
}

bool FileSegment::is_downloader() const {
    std::lock_guard segment_lock(_mutex);
    return get_caller_id() == _downloader_id;
}

bool FileSegment::is_downloader_impl(std::lock_guard<std::mutex>& /* segment_lock */) const {
    return get_caller_id() == _downloader_id;
}

void FileSegment::append(Slice data) {
    DCHECK(data.size != 0) << "Writing zero size is not allowed";

    DCHECK(available_size() >= data.size)
            << "Not enough space is reserved. Available: " << available_size()
            << ", expected: " << data.size;

    if (!_cache_writer) {
        auto download_path = get_path_in_local_cache();
        global_local_filesystem()->create_file(download_path, &_cache_writer);
    }

    _cache_writer->append(data);

    std::lock_guard download_lock(_download_mutex);

    _downloaded_size += data.size;
}

std::string FileSegment::get_path_in_local_cache() const {
    return _cache->get_path_in_local_cache(key(), offset());
}

void FileSegment::read_at(Slice buffer, size_t offset) {
    std::lock_guard segment_lock(_mutex);

    if (!_cache_reader) {
        auto download_path = get_path_in_local_cache();
        global_local_filesystem()->open_file(download_path, &_cache_reader);
    }
    size_t bytes_reads = buffer.size;
    _cache_reader->read_at(offset, buffer, &bytes_reads);
    DCHECK(bytes_reads == buffer.size);
}

size_t FileSegment::finalize_write() {
    std::lock_guard segment_lock(_mutex);

    size_t size = _cache_writer->bytes_appended();

    _downloaded_size += size;

    set_downloaded(segment_lock);

    return size;
}

FileSegment::State FileSegment::wait() {
    std::unique_lock segment_lock(_mutex);

    if (_downloader_id.empty()) {
        return _download_state;
    }

    if (_download_state == State::DOWNLOADING) {
        assert(!_downloader_id.empty());
        assert(_downloader_id != get_caller_id());

        _cv.wait_for(segment_lock, std::chrono::seconds(60));
    }

    return _download_state;
}

bool FileSegment::reserve(size_t size) {
    /**
     * It is possible to have downloaded_size < reserved_size when reserve is called
     * in case previous downloader did not fully download current file_segment
     * and the caller is going to continue;
     */
    size_t free_space = _reserved_size - _downloaded_size;
    size_t size_to_reserve = size - free_space;

    std::lock_guard cache_lock(_cache->_mutex);

    bool reserved = _cache->try_reserve(key(), offset(), size_to_reserve, cache_lock);

    if (reserved) {
        std::lock_guard segment_lock(_mutex);
        _reserved_size += size;
    }

    return reserved;
}

void FileSegment::set_downloaded(std::lock_guard<std::mutex>& /* segment_lock */) {
    if (_is_downloaded) {
        return;
    }

    _download_state = State::DOWNLOADED;
    _is_downloaded = true;
    _downloader_id.clear();

    if (_cache_writer) {
        _cache_writer->finalize();
        _cache_writer->close();
        _cache_writer.reset();
    }
}

void FileSegment::complete(State state) {
    std::lock_guard cache_lock(_cache->_mutex);
    std::lock_guard segment_lock(_mutex);

    if (state == State::DOWNLOADED) {
        set_downloaded(segment_lock);
    }

    _download_state = state;

    complete_impl(cache_lock, segment_lock);

    _cv.notify_all();
}

void FileSegment::complete(std::lock_guard<std::mutex>& cache_lock) {
    std::lock_guard segment_lock(_mutex);

    complete_unlocked(cache_lock, segment_lock);
}

void FileSegment::complete_unlocked(std::lock_guard<std::mutex>& cache_lock,
                                    std::lock_guard<std::mutex>& segment_lock) {
    if (is_downloader_impl(segment_lock) && _download_state != State::DOWNLOADED &&
        get_downloaded_size(segment_lock) == range().size()) {
        set_downloaded(segment_lock);
    }

    complete_impl(cache_lock, segment_lock);

    _cv.notify_all();
}

void FileSegment::complete_impl(std::lock_guard<std::mutex>& cache_lock,
                                std::lock_guard<std::mutex>& segment_lock) {
    if (!_downloader_id.empty() && is_downloader_impl(segment_lock)) {
        _downloader_id.clear();
    }
}

std::string FileSegment::get_info_for_log() const {
    std::lock_guard segment_lock(_mutex);
    return get_info_for_log_impl(segment_lock);
}

std::string FileSegment::get_info_for_log_impl(std::lock_guard<std::mutex>& segment_lock) const {
    std::stringstream info;
    info << "File segment: " << range().to_string() << ", ";
    info << "state: " << state_to_string(_download_state) << ", ";
    info << "downloaded size: " << get_downloaded_size(segment_lock) << ", ";
    info << "reserved size: " << _reserved_size << ", ";
    info << "downloader id: " << _downloader_id << ", ";
    info << "caller id: " << get_caller_id();

    return info.str();
}

std::string FileSegment::state_to_string(FileSegment::State state) {
    switch (state) {
    case FileSegment::State::DOWNLOADED:
        return "DOWNLOADED";
    case FileSegment::State::EMPTY:
        return "EMPTY";
    case FileSegment::State::DOWNLOADING:
        return "DOWNLOADING";
    default:
        DCHECK(false);
        return "";
    }
}

bool FileSegment::has_finalized_state() const {
    return _download_state == State::DOWNLOADED;
}

FileSegment::~FileSegment() {
    std::lock_guard segment_lock(_mutex);
}

FileSegmentsHolder::~FileSegmentsHolder() {
    /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
    /// FileSegmentsHolder right after calling file_segment->complete(), so on destruction here
    /// remain only uncompleted file segments.

    IFileCache* cache = nullptr;

    for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end();) {
        auto current_file_segment_it = file_segment_it;
        auto& file_segment = *current_file_segment_it;

        if (!cache) {
            cache = file_segment->_cache;
        }

        /// File segment pointer must be reset right after calling complete() and
        /// under the same mutex, because complete() checks for segment pointers.
        std::lock_guard cache_lock(cache->_mutex);

        file_segment->complete(cache_lock);

        file_segment_it = file_segments.erase(current_file_segment_it);
    }
}

std::string FileSegmentsHolder::to_string() {
    std::string ranges;
    for (const auto& file_segment : file_segments) {
        if (!ranges.empty()) {
            ranges += ", ";
        }
        ranges += file_segment->range().to_string();
    }
    return ranges;
}

} // namespace io
} // namespace doris
