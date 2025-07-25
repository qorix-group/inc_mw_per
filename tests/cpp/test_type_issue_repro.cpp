/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

/* Change Private Members and final to public to allow access to member variables and derive from
 * kvsvalue in unittests*/
#define private public
#define final
#include "kvs.hpp"
#undef private
#undef final
#include "internal/kvs_helper.hpp"

////////////////////////////////////////////////////////////////////////////////

/* Test Environment Setup - Standard Variables for tests*/
const std::uint32_t instance = 123;
const InstanceId instance_id{instance};
const std::string process_name = "my_process";
const std::string base_dir = "./data_folder";
const std::string data_dir = base_dir + "/" + process_name;
const std::string default_prefix = data_dir + "/kvs_" + std::to_string(instance) + "_default";
const std::string kvs_prefix = data_dir + "/kvs_" + std::to_string(instance) + "_0";
const std::string filename_prefix = data_dir + "/kvs_" + std::to_string(instance);
const std::string default_json = R"({ "default": 5 })";
const std::string kvs_json = R"({ "kvs": 3 })";

////////////////////////////////////////////////////////////////////////////////

/* adler32 control instance */
uint32_t adler32(const std::string& data) {
    const uint32_t mod = 65521;
    uint32_t a = 1, b = 0;
    for (unsigned char c : data) {
        a = (a + c) % mod;
        b = (b + a) % mod;
    }
    return (b << 16) | a;
}

void cleanup_environment() {
    /* Cleanup the test environment */
    if (std::filesystem::exists(base_dir)) {
        for (auto& p : std::filesystem::recursive_directory_iterator(base_dir)) {
            std::filesystem::permissions(p,
                                         std::filesystem::perms::owner_all |
                                             std::filesystem::perms::group_all |
                                             std::filesystem::perms::others_all,
                                         std::filesystem::perm_options::replace);
        }
        std::filesystem::remove_all(base_dir);
    }
}

void prepare_environment_param(const std::string& json_string) {
    /* Prepare the test environment */
    mkdir(base_dir.c_str(), 0777);
    mkdir(data_dir.c_str(), 0777);

    std::ofstream default_json_file(default_prefix + ".json");
    default_json_file << default_json;
    default_json_file.close();

    std::ofstream kvs_json_file(kvs_prefix + ".json");
    kvs_json_file << json_string;
    kvs_json_file.close();

    uint32_t default_hash = adler32(default_json);
    uint32_t kvs_hash = adler32(json_string);

    std::ofstream default_hash_file(default_prefix + ".hash", std::ios::binary);
    default_hash_file.put((default_hash >> 24) & 0xFF);
    default_hash_file.put((default_hash >> 16) & 0xFF);
    default_hash_file.put((default_hash >> 8) & 0xFF);
    default_hash_file.put(default_hash & 0xFF);
    default_hash_file.close();

    std::ofstream kvs_hash_file(kvs_prefix + ".hash", std::ios::binary);
    kvs_hash_file.put((kvs_hash >> 24) & 0xFF);
    kvs_hash_file.put((kvs_hash >> 16) & 0xFF);
    kvs_hash_file.put((kvs_hash >> 8) & 0xFF);
    kvs_hash_file.put(kvs_hash & 0xFF);
    kvs_hash_file.close();
}

////////////////////////////////////////////////////////////////////////////////

TEST(kvs_type_issue_repro, number_zero) {
    auto json_string{R"({"kvs":0})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Number);
    EXPECT_DOUBLE_EQ(std::get<double>(get_value_result.value().getValue()), 0);

    cleanup_environment();
}

TEST(kvs_type_issue_repro, number_one) {
    auto json_string{R"({"kvs":1})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Number);
    EXPECT_DOUBLE_EQ(std::get<double>(get_value_result.value().getValue()), 1);

    cleanup_environment();
}

TEST(kvs_type_issue_repro, bool_false) {
    auto json_string{R"({"kvs":false})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Boolean);
    EXPECT_EQ(std::get<bool>(get_value_result.value().getValue()), false);

    cleanup_environment();
}

TEST(kvs_type_issue_repro, bool_true) {
    auto json_string{R"({"kvs":true})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Boolean);
    EXPECT_EQ(std::get<bool>(get_value_result.value().getValue()), true);

    cleanup_environment();
}

TEST(kvs_type_issue_repro, string_empty) {
    auto json_string{R"({"kvs":""})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::String);
    EXPECT_EQ(std::get<std::string>(get_value_result.value().getValue()), "");

    cleanup_environment();
}

TEST(kvs_type_issue_repro, string_null) {
    auto json_string{R"({"kvs":"null"})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::String);
    EXPECT_EQ(std::get<std::string>(get_value_result.value().getValue()), "null");

    cleanup_environment();
}

TEST(kvs_type_issue_repro, null) {
    auto json_string{R"({"kvs":null})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Null);
    EXPECT_EQ(std::get<std::nullptr_t>(get_value_result.value().getValue()), nullptr);

    cleanup_environment();
}

TEST(kvs_type_issue_repro, array_empty) {
    auto json_string{R"({"kvs":[]})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Array);
    EXPECT_EQ(std::get<KvsValue::Array>(get_value_result.value().getValue()).size(), 0);

    cleanup_environment();
}

TEST(kvs_type_issue_repro, object_empty) {
    auto json_string{R"({"kvs":{}})"};
    prepare_environment_param(json_string);

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required,
                            OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().flush_on_exit = false;

    /* Check Data existing */
    EXPECT_FALSE(result.value().kvs.empty());

    /* Check if value is returned */
    auto get_value_result = result.value().get_value("kvs");
    ASSERT_TRUE(get_value_result);
    EXPECT_EQ(get_value_result.value().getType(), KvsValue::Type::Object);
    EXPECT_EQ(std::get<KvsValue::Object>(get_value_result.value().getValue()).size(), 0);

    cleanup_environment();
}
