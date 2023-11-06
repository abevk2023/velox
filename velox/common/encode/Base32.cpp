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
#include "velox/common/encode/Base32.h"

#include <folly/Portability.h>
#include <folly/container/Foreach.h>
#include <folly/io/Cursor.h>
#include <stdint.h>

namespace facebook::velox::encoding {

constexpr const Base32::Charset kBase32Charset = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    '2', '3', '4', '5', '6', '7'};

constexpr const Base32::ReverseIndex kBase32ReverseIndexTable = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 26,  27,  28,  29,  30,  31,  255, 255, 255, 255,
    255, 255, 255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255};

constexpr bool checkForwardIndex(
    uint8_t idx,
    const Base32::Charset& charset,
    const Base32::ReverseIndex& table) {
  return (table[static_cast<uint8_t>(charset[idx])] == idx) &&
      (idx > 0 ? checkForwardIndex(idx - 1, charset, table) : true);
}
// Verify that for every entry in kBase32Charset, the corresponding entry
// in kBase32ReverseIndexTable is correct.
static_assert(
    checkForwardIndex(
        sizeof(kBase32Charset) - 1,
        kBase32Charset,
        kBase32ReverseIndexTable),
    "kBase32Charset has incorrect entries");

// Similar to strchr(), but for null-terminated const strings.
// Another difference is that we do not consider "\0" to be present in the
// string.
// Returns true if "str" contains the character c.
constexpr bool constCharsetContains(
    const Base32::Charset& charset,
    uint8_t idx,
    const char c) {
  return idx < charset.size() &&
      ((charset[idx] == c) || constCharsetContains(charset, idx + 1, c));
}

constexpr bool checkReverseIndex(
    uint8_t idx,
    const Base32::Charset& charset,
    const Base32::ReverseIndex& table) {
  return (table[idx] == 255
              ? !constCharsetContains(charset, 0, static_cast<char>(idx))
              : (charset[table[idx]] == idx)) &&
      (idx > 0 ? checkReverseIndex(idx - 1, charset, table) : true);
}

// Verify that for every entry in kBase32ReverseIndexTable, the corresponding
// entry in kBase32Charset is correct.
static_assert(
    checkReverseIndex(
        sizeof(kBase32ReverseIndexTable) - 1,
        kBase32Charset,
        kBase32ReverseIndexTable),
    "kBase32ReverseIndexTable has incorrect entries.");


template <class T>
/*  static */ std::string
Base32::encodeImpl(const T& data, const Charset& charset, bool include_pad) {
  size_t outlen = calculateEncodedSize(data.size(), include_pad);

  std::string out;
  out.resize(outlen);

  encodeImpl(data, charset, include_pad, out.data());
  return out;
}

// static
size_t Base32::calculateEncodedSize(size_t size, bool withPadding) {
  if (size == 0) {
    return 0;
  }

  // Calculate the output size without padding.
  size_t encodedSize = std::ceil(((size * 8) + 4) /5.0);
  if (withPadding) {
    // If the padding was requested, add the padding bytes.
    encodedSize = ((encodedSize + 7)/8) * 8;
  }
  return encodedSize;
}

// static
void Base32::encode(const char* data, size_t len, char* output) {
  encodeImpl(folly::StringPiece(data, len), kBase32Charset, true, output);
}

template <class T>
/* static */ void Base32::encodeImpl(
    const T& data,
    const Charset& charset,
    bool include_pad,
    char* out) {
  auto len = data.size();
  if (len == 0) {
    return;
  }

  auto wp = out;
  auto it = data.begin();

  // For each group of 5 bytes (40 bits) in the input, split that into
  // 8 groups of 5 bits and encode that using the supplied charset lookup
  for (; len > 4; len -= 5) {
    uint64_t curr = uint64_t(*it++) << 32;
    curr |= uint8_t(*it++) << 24;
    curr |= uint8_t(*it++) << 16;
    curr |= uint8_t(*it++) << 8;
    curr |= uint8_t(*it++);

    *wp++ = charset[(curr >> 35) & 0x1f];
    *wp++ = charset[(curr >> 30) & 0x1f];
    *wp++ = charset[(curr >> 25) & 0x1f];
    *wp++ = charset[(curr >> 20) & 0x1f];
    *wp++ = charset[(curr >> 15) & 0x1f];
    *wp++ = charset[(curr >> 10) & 0x1f];
    *wp++ = charset[(curr >> 5) & 0x1f];
    *wp++ = charset[curr & 0x1f];
  }

  if (len > 0) {
    // We have either 1 to 4 input bytes left.  Encode this similar to the
    // above (assuming 0 for all other bytes).  Optionally append the '='
    // character if it is requested.
    uint64_t curr = uint64_t(*it++) << 32;
    *wp++ = charset[(curr >> 35) & 0x1f];
    if (len > 3) {
      curr |= uint8_t(*it++) << 24;
      curr |= uint8_t(*it++) << 16;
      curr |= uint8_t(*it) << 8;

      *wp++ = charset[(curr >> 30) & 0x1f];
      *wp++ = charset[(curr >> 25) & 0x1f];
      *wp++ = charset[(curr >> 20) & 0x1f];
      *wp++ = charset[(curr >> 15) & 0x1f];
      *wp++ = charset[(curr >> 10) & 0x1f];
      *wp++ = charset[(curr >> 5) & 0x1f];

      if (include_pad) {
        *wp = kBase32Pad;
      }
    } else if (len > 2) {
      curr |= uint8_t(*it++) << 24;
      curr |= uint8_t(*it++) << 16;

      *wp++ = charset[(curr >> 30) & 0x1f];
      *wp++ = charset[(curr >> 25) & 0x1f];
      *wp++ = charset[(curr >> 20) & 0x1f];
      *wp++ = charset[(curr >> 15) & 0x1f];

      if (include_pad) {
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp = kBase32Pad;
      }
    } else if (len > 1) {
      curr |= uint8_t(*it) << 24;

      *wp++ = charset[(curr >> 30) & 0x1f];
      *wp++ = charset[(curr >> 25) & 0x1f];
      *wp++ = charset[(curr >> 20) & 0x1f];

      if (include_pad) {
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp = kBase32Pad;
      }
    } else {
      *wp++ = charset[(curr >> 30) & 0x1f];

      if (include_pad) {
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp++ = kBase32Pad;
        *wp = kBase32Pad;
      }
    }
  }
}

std::string Base32::encode(folly::StringPiece text) {
  return encodeImpl(text, kBase32Charset, true);
}

std::string Base32::encode(const char* data, size_t len) {
  return encode(folly::StringPiece(data, len));
}

std::string Base32::decode(folly::StringPiece encoded) {
  std::string output;
  Base32::decode(std::make_pair(encoded.data(), encoded.size()), output);
  return output;
}

void Base32::decode(
    const std::pair<const char*, int32_t>& payload,
    std::string& output) {
  // Every 8 encoded characters together represent 40 bits (5 bytes) of of the original data.
  size_t out_len = (payload.second * 5) / 8;
  output.resize(out_len, '\0');
  out_len = Base32::decode(payload.first, payload.second, &output[0], out_len);
  output.resize(out_len);
}

// static
void Base32::decode(const char* data, size_t size, char* output) {
  size_t out_len = (size * 5) / 8;
  Base32::decode(data, size, output, out_len);
}

uint8_t Base32::Base32ReverseLookup(
    char p,
    const Base32::ReverseIndex& reverse_lookup) {
  auto curr = reverse_lookup[(uint8_t)p];
  if (curr >= 0x20) {
    throw Base32Exception(
        "Base32::decode() - invalid input string: invalid characters");
  }

  return curr;
}

size_t
Base32::decode(const char* src, size_t src_len, char* dst, size_t dst_len) {
  return decodeImpl(src, src_len, dst, dst_len, kBase32ReverseIndexTable, true);
}

// static
size_t
Base32::calculateDecodedSize(const char* data, size_t& size, bool withPadding) {
  if (size == 0) {
    return 0;
  }

  auto needed = (size * 5) / 8;
  if (withPadding) {
    // If the pad characters are included then the source string must be a
    // multiple of 8 and we can query the end of the string to see how much
    // padding exists.
    if (size % 8 != 0) {
      throw Base32Exception(
          "Base32::decode() - invalid input string: "
          "string length is not multiple of 8.");
    }

    auto padding = countPadding(data, size);
    size -= padding;
    return needed - padding;
  }

  // If padding doesn't exist we need to calculate it from the size - if the
  // size % 8 is 0 then we have an even multiple 5 byte chunks in the result
  // if it is 7 then we need 1 more byte in the output.  If it is 5 then we
  // need 3 more bytes in the output. Likewise 4 and 2. But, it should never
  // be 6 or 3 or 1.
  auto extra = size % 8;
  if (extra) {
    if ((extra == 6)||(extra == 3)||(extra == 1)) {
      throw Base32Exception(
          "Base32::decode() - invalid input string: "
          "string length cannot be 6, 3 or 1 more than a multiple of 8.");
    }
    return needed + extra - 1;
  }

  // Just because we don't need the pad, doesn't mean it is not there.  The
  // URL decoder should be able to handle the original encoding.
  auto padding = countPadding(data, size);
  size -= padding;
  return needed - padding;
}

size_t Base32::decodeImpl(
    const char* src,
    size_t src_len,
    char* dst,
    size_t dst_len,
    const Base32::ReverseIndex& reverse_lookup,
    bool include_pad) {
  if (!src_len) {
    return 0;
  }

  auto needed = calculateDecodedSize(src, src_len, include_pad);
  if (dst_len < needed) {
    throw Base32Exception(
        "Base32::decode() - invalid output string: "
        "output string is too small.");
  }

  // Handle full groups of 8 characters
  for (; src_len > 8; src_len -= 8, src += 8, dst += 5) {
    // Each character of the 8 bytes encode 5 bits of the original, grab each with
    // the appropriate shifts to rebuild the original and then split that back
    // into the original 8 bit bytes.
    uint64_t last = (uint64_t(Base32ReverseLookup(src[0], reverse_lookup)) << 35) |
        (Base32ReverseLookup(src[1], reverse_lookup) << 30) |
        (Base32ReverseLookup(src[2], reverse_lookup) << 25) |
        (Base32ReverseLookup(src[3], reverse_lookup) << 20) |
        (Base32ReverseLookup(src[4], reverse_lookup) << 15) |
        (Base32ReverseLookup(src[5], reverse_lookup) << 10) |
        (Base32ReverseLookup(src[6], reverse_lookup) << 5) |
        Base32ReverseLookup(src[7], reverse_lookup);
    dst[0] = (last >> 32) & 0xff;
    dst[1] = (last >> 24) & 0xff;
    dst[2] = (last >> 16) & 0xff;
    dst[3] = (last >> 8) & 0xff;
    dst[4] = last & 0xff;
  }

  // Handle the last 2, 4, 5 or 7 characters.  This is similar to the above, but the
  // last characters may or may not exist.
  DCHECK(src_len >= 2);
  uint64_t last = (uint64_t(Base32ReverseLookup(src[0], reverse_lookup)) << 35) |
      (Base32ReverseLookup(src[1], reverse_lookup) << 30);
  dst[0] = (last >> 32) & 0xff;
  if (src_len > 2) {
    last |= Base32ReverseLookup(src[2], reverse_lookup) << 25;
    last |= Base32ReverseLookup(src[3], reverse_lookup) << 20;
    dst[1] = (last >> 24) & 0xff;
    if (src_len > 4) {
      last |= Base32ReverseLookup(src[4], reverse_lookup)<<15;
      dst[3] = (last >> 16) & 0xff;
      if (src_len > 5) {
        last |= Base32ReverseLookup(src[5], reverse_lookup)<<10;
        last |= Base32ReverseLookup(src[6], reverse_lookup)<<5;
        dst[4] = (last >> 8) & 0xff;
      }
    }
  }

  return needed;
}

} // namespace facebook::velox::encoding
