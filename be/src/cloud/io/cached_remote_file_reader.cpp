// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "cloud/io/cached_remote_file_reader.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <memory>
#include <utility>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_factory.h"
#include "cloud/io/cloud_file_cache_fwd.h"
#include "cloud/io/s3_common.h"
#include "olap/olap_common.h"
#include "util/async_io.h"
#include "util/doris_metrics.h"
#include "vec/common/sip_hash.h"

namespace doris {
namespace io {

CachedRemoteFileReader::CachedRemoteFileReader(FileReaderSPtr remote_file_reader,
                                               metrics_hook metrics)
        : _remote_file_reader(std::move(remote_file_reader)), _metrics(metrics) {
    _cache_key = IFileCache::hash(path().filename().native());
    _cache = FileCacheFactory::instance().getByPath(_cache_key);
    _disposable_cache = FileCacheFactory::instance().getDisposableCache(_cache_key);
}

CachedRemoteFileReader::~CachedRemoteFileReader() {
    close();
}

Status CachedRemoteFileReader::close() {
    return _remote_file_reader->close();
}

std::pair<size_t, size_t> CachedRemoteFileReader::_align_size(size_t offset,
                                                              size_t read_size) const {
    size_t left = offset;
    size_t right = offset + read_size - 1;
    size_t align_left = (left / config::file_cache_max_file_segment_size) *
                        config::file_cache_max_file_segment_size;
    size_t align_right = (right / config::file_cache_max_file_segment_size + 1) *
                         config::file_cache_max_file_segment_size;
    align_right = align_right < size() ? align_right : size();
    size_t align_size = align_right - align_left;
    return std::make_pair(align_left, align_size);
}

Status CachedRemoteFileReader::read_at(size_t offset, Slice result, size_t* bytes_read,
                                       IOState* state) {
    if (bthread_self() == 0) {
        return read_at_impl(offset, result, bytes_read, state);
    }
    Status s;
    auto task = [&] { s = read_at_impl(offset, result, bytes_read, state); };
    AsyncIO::run_task(task, io::FileSystemType::S3);
    return s;
}

Status CachedRemoteFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                            IOState* state) {
    DCHECK(!closed());
    if (offset > size()) {
        return Status::IOError(
                fmt::format("offset exceeds file size(offset: {), file size: {}, path: {})", offset,
                            size(), path().native()));
    }
    size_t bytes_req = result.size;
    bytes_req = std::min(bytes_req, size() - offset);
    if (UNLIKELY(bytes_req == 0)) {
        *bytes_read = 0;
        return Status::OK();
    }
    CloudFileCachePtr cache = state == nullptr              ? _cache
                              : state->use_disposable_cache ? _disposable_cache
                                                            : _cache;
    // cache == nullptr since use_disposable_cache = true and don't set  disposable cache in conf
    if (cache == nullptr) {
        return _remote_file_reader->read_at(offset, result, bytes_read, state);
    }
    ReadStatistics stats;
    stats.bytes_read = bytes_req;
    auto [align_left, align_size] = _align_size(offset, bytes_req);
    DCHECK((align_left % config::file_cache_max_file_segment_size) == 0);
    // if state == nullptr, the method is called for read footer/index
    bool is_persistent = state ? state->is_persistent : true;
    TUniqueId query_id = state && state->query_id ? *state->query_id : TUniqueId();
    FileSegmentsHolder holder =
            cache->get_or_set(_cache_key, align_left, align_size, is_persistent, query_id);
    std::vector<FileSegmentSPtr> empty_segments;
    for (auto& segment : holder.file_segments) {
        if (segment->state() == FileSegment::State::EMPTY) {
            segment->get_or_set_downloader();
            if (segment->is_downloader()) {
                empty_segments.push_back(segment);
            }
        } else if (segment->state() == FileSegment::State::SKIP_CACHE) {
            empty_segments.push_back(segment);
            stats.bytes_skip_cache += segment->range().size();
        }
    }

    size_t empty_start = 0;
    size_t empty_end = 0;
    if (!empty_segments.empty()) {
        empty_start = empty_segments.front()->range().left;
        empty_end = empty_segments.back()->range().right;
        size_t size = empty_end - empty_start + 1;
        std::unique_ptr<char[]> buffer(new char[size]);
        RETURN_IF_ERROR(
                _remote_file_reader->read_at(empty_start, Slice(buffer.get(), size), &size, state));
        for (auto& segment : empty_segments) {
            if (segment->state() == FileSegment::State::SKIP_CACHE) {
                continue;
            }
            char* cur_ptr = buffer.get() + segment->range().left - empty_start;
            size_t segment_size = segment->range().size();
            RETURN_IF_ERROR(segment->append(Slice(cur_ptr, segment_size)));
            RETURN_IF_ERROR(segment->finalize_write());
            stats.write_in_file_cache++;
            stats.bytes_write_in_file_cache += segment_size;
        }
        // copy from memory directly
        size_t right_offset = offset + result.size - 1;
        if (empty_start <= right_offset && empty_end >= offset) {
            size_t copy_left_offset = offset < empty_start ? empty_start : offset;
            size_t copy_right_offset = right_offset < empty_end ? right_offset : empty_end;
            char* dst = result.data + (copy_left_offset - offset);
            char* src = buffer.get() + (copy_left_offset - empty_start);
            size_t copy_size = copy_right_offset - copy_left_offset + 1;
            memcpy(dst, src, copy_size);
        }
    } else {
        stats.hit_cache = true;
    }

    size_t current_offset = offset;
    size_t end_offset = offset + bytes_req - 1;
    *bytes_read = 0;
    for (auto& segment : holder.file_segments) {
        size_t left = segment->range().left;
        size_t right = segment->range().right;
        if (right < offset) {
            continue;
        }
        size_t read_size =
                end_offset > right ? right - current_offset + 1 : end_offset - current_offset + 1;
        if (empty_start <= left && right <= empty_end) {
            *bytes_read += read_size;
            current_offset = right + 1;
            continue;
        }
        FileSegment::State state;
        int64_t wait_time = 0;
        static int64_t MAX_WAIT_TIME = 10;
        do {
            state = segment->wait();
            if (state == FileSegment::State::DOWNLOADED) {
                break;
            }
            if (state != FileSegment::State::DOWNLOADING) {
                return Status::IOError(
                        "File Cache State is {}, the cache downloader encounters an error, please "
                        "retry it",
                        state);
            }
        } while (++wait_time < MAX_WAIT_TIME);
        if (UNLIKELY(wait_time) == MAX_WAIT_TIME) {
            return Status::IOError("Waiting too long for the download to complete");
        }
        size_t file_offset = current_offset - left;
        RETURN_IF_ERROR(segment->read_at(Slice(result.data + (current_offset - offset), read_size),
                                         file_offset));
        stats.bytes_read_from_file_cache = read_size;
        *bytes_read += read_size;
        current_offset = right + 1;
        if (current_offset > end_offset) {
            break;
        }
    }
    DCHECK(*bytes_read == bytes_req);
    _update_state(stats, state);
    DorisMetrics::instance()->s3_bytes_read_total->increment(*bytes_read);
    if (state != nullptr && _metrics != nullptr) {
        _metrics(state->stats);
    }
    return Status::OK();
}

void CachedRemoteFileReader::_update_state(const ReadStatistics& read_stats, IOState* state) const {
    if (state == nullptr) {
        return;
    }
    auto stats = state->stats;
    stats->file_cache_stats.num_io_total++;
    stats->file_cache_stats.num_io_bytes_read_total += read_stats.bytes_read;
    stats->file_cache_stats.num_io_bytes_written_in_file_cache +=
            read_stats.bytes_write_in_file_cache;
    if (read_stats.hit_cache) {
        stats->file_cache_stats.num_io_hit_cache++;
    }
    stats->file_cache_stats.num_io_bytes_read_from_file_cache +=
            read_stats.bytes_read_from_file_cache;
    stats->file_cache_stats.num_io_written_in_file_cache += read_stats.write_in_file_cache;
    stats->file_cache_stats.num_io_bytes_skip_cache += read_stats.bytes_skip_cache;
}

} // namespace io
} // namespace doris
