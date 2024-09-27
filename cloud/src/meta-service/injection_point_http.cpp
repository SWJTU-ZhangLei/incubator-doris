
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

#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <brpc/uri.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "meta_service.h"
#include "meta_service_http.h"

namespace doris::cloud {

HttpResponse set_sleep(const std::string& point, const brpc::URI& uri) {
    std::string duration_str(http_query(uri, "duration"));
    int64_t duration = 0;
    try {
        duration = std::stol(duration_str);
    } catch (const std::exception& e) {
        auto msg = fmt::format("invalid duration:{}", duration_str);
        LOG(WARNING) << msg;
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point, duration](auto&& args) {
        LOG(INFO) << "injection point hit, point=" << point << " sleep milliseconds=" << duration;
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    });
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse set_return(const std::string& point, const brpc::URI& uri) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point](auto&& args) {
        try {
            LOG(INFO) << "injection point hit, point=" << point << " return void";
            auto pred = try_any_cast<bool*>(args.back());
            *pred = true;
        } catch (const std::bad_any_cast& e) {
            LOG_EVERY_N(ERROR, 10) << "failed to process `return` e:" << e.what();
        }
    });

    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse set_return_ok(const std::string& point, const brpc::URI& uri) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [point](auto&& args) {
        try {
            LOG(INFO) << "injection point hit, point=" << point << " return ok";
            auto* pair = try_any_cast_ret<MetaServiceCode>(args);
            pair->first = MetaServiceCode::OK;
            pair->second = true;
        } catch (const std::bad_any_cast& e) {
            LOG_EVERY_N(ERROR, 10) << "failed to process `return_ok` e:" << e.what();
        }
    });
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse set_return_error(const std::string& point, const brpc::URI& uri) {
    //todo:
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_set(const brpc::URI& uri) {
    const std::string point(http_query(uri, "name"));
    if (point.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "empty point name");
    }

    const std::string behavior(http_query(uri, "behavior"));
    if (behavior.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "empty behavior");
    }
    if (behavior == "sleep") {
        return set_sleep(point, uri);
    } else if (behavior == "return") {
        return set_return(point, uri);
    } else if (behavior == "return_ok") {
        return set_return_ok(point, uri);
    } else if (behavior == "return_error") {
        return set_return_error(point, uri);
    }

    return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "unknown behavior: " + behavior);
}

HttpResponse handle_clear(const brpc::URI& uri) {
    const std::string point(http_query(uri, "name"));
    auto* sp = SyncPoint::get_instance();
    LOG(INFO) << "clear injection point : " << (point.empty() ? "(all points)" : point);
    if (point.empty()) {
        // If point name is emtpy, clear all
        sp->clear_all_call_backs();
        return http_json_reply(MetaServiceCode::OK, "OK");
    }
    sp->clear_call_back(point);
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_apply_suite(const brpc::URI& uri) {
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_enable(const brpc::URI& uri) {
    SyncPoint::get_instance()->enable_processing();
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse handle_disable(const brpc::URI& uri) {
    SyncPoint::get_instance()->disable_processing();
    return http_json_reply(MetaServiceCode::OK, "OK");
}

HttpResponse process_injection_point(MetaServiceImpl* service, brpc::Controller* ctrl) {
    auto& uri = ctrl->http_request().uri();
    LOG(INFO) << "handle InjectionPointAction uri:" << uri;
    const std::string op(http_query(uri, "op"));

    if (op == "set") {
        return handle_set(uri);
    } else if (op == "clear") {
        return handle_clear(uri);
    } else if (op == "apply_suite") {
        return handle_apply_suite(uri);
    } else if (op == "enable") {
        return handle_enable(uri);
    } else if (op == "disable") {
        return handle_disable(uri);
    }

    return http_json_reply(MetaServiceCode::INVALID_ARGUMENT, "unknown op:" + op);
}
} // namespace doris::cloud