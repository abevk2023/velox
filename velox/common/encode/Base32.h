/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <exception>
#include <map>
#include <string>

#include <glog/logging.h>
#include <folly/Range.h>

namespace facebook::velox::encoding {

class Base32Exception : public std::exception {
 public:
  explicit Base32Exception(const char* msg) : msg_(msg) {}
  const char* what() const noexcept override {
    return msg_;
  }

 protected:
  const char* msg_;
};

class Base32 {
 public:
  using Charset = std::array<char, 32>;
  using ReverseIndex = std::array<uint8_t, 256>;

  static std::string encode(const char* data, size_t len);
  static std::string encode(folly::StringPiece text);

  /// Returns encoded size for the input of the specified size.
  static size_t calculateEncodedSize(size_t size, bool withPadding = true);

  /// Encodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateEncodedSize().
  static void encode(const char* data, size_t size, char* output);

  static std::string decode(folly::StringPiece encoded);

  /// Returns decoded size for the specified input. Adjusts the 'size' to
  /// subtract the length of the padding, if exists.
  static size_t
  calculateDecodedSize(const char* data, size_t& size, bool withPadding = true);

  /// Decodes the specified number of characters from the 'data' and writes the
  /// result to the 'output'. The output must have enough space, e.g. as
  /// returned by the calculateDecodedSize().
  static void decode(const char* data, size_t size, char* output);

  static void decode(
      const std::pair<const char*, int32_t>& payload,
      std::string& outp);

  static size_t
  decode(const char* src, size_t src_len, char* dst, size_t dst_len);

  constexpr static char kBase32Pad = '=';

 private:
  static inline size_t countPadding(const char* src, size_t len) {
    DCHECK_GE(len, 2);
    return src[len - 1] != kBase32Pad ? 0 : src[len - 2] != kBase32Pad ? 1 : 2;
  }

  static uint8_t Base32ReverseLookup(char p, const ReverseIndex& table);

  template <class T>
  static std::string
  encodeImpl(const T& data, const Charset& charset, bool include_pad);

  template <class T>
  static void encodeImpl(
      const T& data,
      const Charset& charset,
      bool include_pad,
      char* out);

  static size_t decodeImpl(
      const char* src,
      size_t src_len,
      char* dst,
      size_t dst_len,
      const ReverseIndex& table,
      bool include_pad);
};

} // namespace facebook::velox::encoding
