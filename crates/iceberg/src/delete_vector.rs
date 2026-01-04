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

use std::io::Cursor;
use std::ops::BitOrAssign;

use crc32fast::Hasher;
use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

const DELETION_VECTOR_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
const DELETION_VECTOR_LENGTH_BYTES: usize = 4;
const DELETION_VECTOR_CRC_BYTES: usize = 4;

use crate::{Error, ErrorKind, Result};

#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    #[allow(unused)]
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    pub fn insert(&mut self, pos: u64) -> bool {
        self.inner.insert(pos)
    }

    /// Marks the given `positions` as deleted and returns the number of elements appended.
    ///
    /// The input slice must be strictly ordered in ascending order, and every value must be greater than all existing values already in the set.
    ///
    /// # Errors
    ///
    /// Returns an error if the precondition is not met.
    #[allow(dead_code)]
    pub fn insert_positions(&mut self, positions: &[u64]) -> Result<usize> {
        if let Err(err) = self.inner.append(positions.iter().copied()) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "failed to marks rows as deleted".to_string(),
            )
            .with_source(err));
        }
        Ok(positions.len())
    }

    #[allow(unused)]
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Serializes this deletion vector to the Iceberg spec wire format.
    ///
    /// Reserved for future use in deletion vector file I/O operations.
    #[allow(dead_code)]
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        serialize_deletion_vector(&self.inner)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        deserialize_deletion_vector(bytes).map(DeleteVector::new)
    }
}

#[allow(dead_code)]
fn serialize_deletion_vector(bitmap: &RoaringTreemap) -> Result<Vec<u8>> {
    let mut bitmap_bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut bitmap_bytes)
        .map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                "Failed to serialize deletion vector bitmap".to_string(),
            )
            .with_source(err)
        })?;

    let mut payload = Vec::with_capacity(DELETION_VECTOR_MAGIC.len() + bitmap_bytes.len());
    payload.extend_from_slice(&DELETION_VECTOR_MAGIC);
    payload.extend_from_slice(&bitmap_bytes);

    let payload_len = payload.len();
    if payload_len > u32::MAX as usize {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Deletion vector payload exceeds u32 length".to_string(),
        ));
    }

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    let mut out = Vec::with_capacity(
        DELETION_VECTOR_LENGTH_BYTES + payload.len() + DELETION_VECTOR_CRC_BYTES,
    );
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}

fn deserialize_deletion_vector(bytes: &[u8]) -> Result<RoaringTreemap> {
    let min_len = DELETION_VECTOR_LENGTH_BYTES + DELETION_VECTOR_MAGIC.len() + DELETION_VECTOR_CRC_BYTES;
    if bytes.len() < min_len {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Deletion vector blob is too short".to_string(),
        ));
    }

    let length = u32::from_be_bytes(bytes[..DELETION_VECTOR_LENGTH_BYTES].try_into().map_err(
        |err| {
            Error::new(
                ErrorKind::DataInvalid,
                "Deletion vector length prefix is invalid".to_string(),
            )
            .with_source(err)
        },
    )?) as usize;

    let total_len = DELETION_VECTOR_LENGTH_BYTES
        .checked_add(length)
        .and_then(|value| value.checked_add(DELETION_VECTOR_CRC_BYTES))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Deletion vector length overflows".to_string(),
            )
        })?;

    if bytes.len() != total_len {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Deletion vector length mismatch: expected {total_len} bytes, got {}",
                bytes.len()
            ),
        ));
    }

    if length < DELETION_VECTOR_MAGIC.len() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Deletion vector payload is too short for magic".to_string(),
        ));
    }

    let payload_start = DELETION_VECTOR_LENGTH_BYTES;
    let payload_end = payload_start + length;
    let payload = &bytes[payload_start..payload_end];

    if payload[..DELETION_VECTOR_MAGIC.len()] != DELETION_VECTOR_MAGIC {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Deletion vector magic mismatch".to_string(),
        ));
    }

    let mut hasher = Hasher::new();
    hasher.update(payload);
    let expected_crc = hasher.finalize();

    let crc_start = payload_end;
    let crc_end = crc_start + DELETION_VECTOR_CRC_BYTES;
    let actual_crc = u32::from_be_bytes(bytes[crc_start..crc_end].try_into().map_err(|err| {
        Error::new(ErrorKind::DataInvalid, "Deletion vector CRC is invalid".to_string())
            .with_source(err)
    })?);

    if actual_crc != expected_crc {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Deletion vector CRC mismatch".to_string(),
        ));
    }

    let bitmap_bytes = &payload[DELETION_VECTOR_MAGIC.len()..];
    RoaringTreemap::deserialize_from(Cursor::new(bitmap_bytes)).map_err(|err| {
        Error::new(
            ErrorKind::DataInvalid,
            "Failed to deserialize deletion vector bitmap".to_string(),
        )
        .with_source(err)
    })
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which is very handy in ArrowReader::build_deletes_row_selection.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
pub struct DeleteVectorIterator<'a> {
    // NB: `BitMapIter` was only exposed publicly in https://github.com/RoaringBitmap/roaring-rs/pull/316
    // which is not yet released. As a consequence our Cargo.toml temporarily uses a git reference for
    // the roaring dependency.
    outer: BitmapIter<'a>,
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    high_bits: u32,
    bitmap_iter: Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner
            && let Some(inner_next) = inner.bitmap_iter.next()
        {
            return Some(u64::from(inner.high_bits) << 32 | u64::from(inner_next));
        }

        if let Some((high_bits, next_bitmap)) = self.outer.next() {
            self.inner = Some(DeleteVectorIteratorInner {
                high_bits,
                bitmap_iter: next_bitmap.iter(),
            })
        } else {
            return None;
        }

        self.next()
    }
}

impl DeleteVectorIterator<'_> {
    pub fn advance_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        let Some(ref mut inner) = self.inner else {
            return;
        };

        while inner.high_bits < hi {
            let Some((next_hi, next_bitmap)) = self.outer.next() else {
                return;
            };

            *inner = DeleteVectorIteratorInner {
                high_bits: next_hi,
                bitmap_iter: next_bitmap.iter(),
            }
        }

        inner.bitmap_iter.advance_to(lo);
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_iteration() {
        let mut dv = DeleteVector::default();
        assert!(dv.insert(42));
        assert!(dv.insert(100));
        assert!(!dv.insert(42));

        let mut items: Vec<u64> = dv.iter().collect();
        items.sort();
        assert_eq!(items, vec![42, 100]);
        assert_eq!(dv.len(), 2);
    }

    #[test]
    fn test_successful_insert_positions() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 2, 3, 1000, 1 << 33];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 5);

        let mut collected: Vec<u64> = dv.iter().collect();
        collected.sort();
        assert_eq!(collected, positions);
    }

    /// Testing scenario: bulk insertion fails because input positions are not strictly increasing.
    #[test]
    fn test_failed_insertion_unsorted_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 4];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have intersection with existing ones.
    #[test]
    fn test_failed_insertion_with_intersection() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 3);

        let res = dv.insert_positions(&[2, 4]);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have duplicates.
    #[test]
    fn test_failed_insertion_duplicate_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 5];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    #[test]
    fn test_delete_vector_roundtrip_bytes() {
        let mut dv = DeleteVector::default();
        dv.insert(1);
        dv.insert(5);
        dv.insert(1 << 33);

        let bytes = dv.to_bytes().unwrap();
        let decoded = DeleteVector::from_bytes(&bytes).unwrap();

        let mut actual: Vec<u64> = decoded.iter().collect();
        actual.sort();
        assert_eq!(actual, vec![1, 5, 1 << 33]);
    }

    #[test]
    fn test_delete_vector_bad_magic() {
        let mut dv = DeleteVector::default();
        dv.insert(10);
        let mut bytes = dv.to_bytes().unwrap();
        bytes[DELETION_VECTOR_LENGTH_BYTES] ^= 0xFF;

        let err = DeleteVector::from_bytes(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_delete_vector_bad_crc() {
        let mut dv = DeleteVector::default();
        dv.insert(10);
        let mut bytes = dv.to_bytes().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;

        let err = DeleteVector::from_bytes(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_delete_vector_length_mismatch() {
        let mut dv = DeleteVector::default();
        dv.insert(10);
        let mut bytes = dv.to_bytes().unwrap();

        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;
        bytes[3] = 0;

        let err = DeleteVector::from_bytes(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
