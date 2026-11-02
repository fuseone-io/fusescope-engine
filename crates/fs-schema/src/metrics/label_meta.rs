//! Where each label of a series came from: its level and the OTLP types
//! its value was received as (`MAPPING.md`, "Metrics").
//!
//! One `u16` a label, in `label_metadata` of the series schema v2:
//!
//! | Bits | Field |
//! |---|---|
//! | 15–12 | level: 1 resource, 2 scope, 3 point |
//! | 11–8 | reserved, zero |
//! | 7 | legacy: converted from a row of series v1, level and types unknown |
//! | 6–0 | datatypes seen: bit 0 Str, 1 Int, 2 Double, 3 Bool, 4 Slice, 5 Map, 6 Bytes |
//!
//! A native entry has a level and at least one type, and no legacy bit; a
//! legacy entry is the legacy bit alone. Anything else is damage. The
//! level is part of a series' identity, the types are not: one key received
//! as Int `1` and as Str `"1"` renders and hashes alike and is one series,
//! its types accumulated. A legacy entry is no evidence of a level: merged
//! with a native one it takes that one's.

use opentelemetry_proto::tonic::common::v1::any_value::Value;

use crate::SchemaError;

const LEVEL_SHIFT: u16 = 12;
const RESERVED: u16 = 0x0f00;
const LEGACY: u16 = 0x0080;
const TYPES: u16 = 0x007f;

/// The level a label came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LabelLevel {
    /// A resource attribute.
    Resource = 1,
    /// A scope attribute, or one of the scope's generated labels.
    Scope = 2,
    /// A point attribute, or a generated label of the point.
    Point = 3,
}

impl LabelLevel {
    /// As the metadata table's `attr_type` names it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Scope => "scope",
            Self::Point => "point",
        }
    }
}

/// An OTLP type a value was received as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Datatype {
    /// A string.
    Str = 0,
    /// An integer.
    Int = 1,
    /// A double.
    Double = 2,
    /// A bool.
    Bool = 3,
    /// An array.
    Slice = 4,
    /// A key-value list.
    Map = 5,
    /// Bytes.
    Bytes = 6,
}

impl Datatype {
    /// Every type, by its bit.
    pub const ALL: [Self; 7] = [
        Self::Str,
        Self::Int,
        Self::Double,
        Self::Bool,
        Self::Slice,
        Self::Map,
        Self::Bytes,
    ];

    /// The type of `value`, `None` for one that renders no text.
    pub fn of(value: &Value) -> Option<Self> {
        match value {
            Value::StringValue(_) => Some(Self::Str),
            Value::IntValue(_) => Some(Self::Int),
            Value::DoubleValue(_) => Some(Self::Double),
            Value::BoolValue(_) => Some(Self::Bool),
            Value::ArrayValue(_) => Some(Self::Slice),
            Value::KvlistValue(_) => Some(Self::Map),
            Value::BytesValue(_) => Some(Self::Bytes),
            Value::StringValueStrindex(_) => None,
        }
    }

    /// As the metadata table's `attr_datatype` names it, the pdata name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Str => "Str",
            Self::Int => "Int",
            Self::Double => "Double",
            Self::Bool => "Bool",
            Self::Slice => "Slice",
            Self::Map => "Map",
            Self::Bytes => "Bytes",
        }
    }
}

/// The origin of one label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LabelMeta(u16);

/// Two native origins of different levels: two identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Conflict;

impl LabelMeta {
    /// The origin of a label of a row of series v1.
    pub const LEGACY: Self = Self(LEGACY);

    /// A label of `level` received as `datatype`.
    pub fn native(level: LabelLevel, datatype: Datatype) -> Self {
        Self(((level as u16) << LEVEL_SHIFT) | (1 << datatype as u16))
    }

    /// Its encoding.
    pub fn bits(self) -> u16 {
        self.0
    }

    /// The origin `bits` encode.
    ///
    /// # Errors
    /// Reserved bits, a level outside 1 to 3, no type, or the legacy bit
    /// beside a level or a type: a damaged file or one of a newer engine.
    pub fn decode(bits: u16) -> Result<Self, SchemaError> {
        if bits == LEGACY {
            return Ok(Self::LEGACY);
        }
        let level = bits >> LEVEL_SHIFT;
        if bits & (RESERVED | LEGACY) != 0 || !(1..=3).contains(&level) || bits & TYPES == 0 {
            return Err(SchemaError::Layout(
                "metrics: a label origin this engine cannot read",
            ));
        }
        Ok(Self(bits))
    }

    /// Its level, `None` for a legacy one.
    pub fn level(self) -> Option<LabelLevel> {
        match self.0 >> LEVEL_SHIFT {
            1 => Some(LabelLevel::Resource),
            2 => Some(LabelLevel::Scope),
            3 => Some(LabelLevel::Point),
            _ => None,
        }
    }

    /// The types seen, in bit order.
    pub fn datatypes(self) -> impl Iterator<Item = Datatype> {
        Datatype::ALL
            .into_iter()
            .filter(move |d| self.0 & (1 << *d as u16) != 0)
    }

    /// Whether `self` and `other` may be one label of one series: either
    /// is legacy, or both are of one level.
    pub fn agrees(self, other: Self) -> bool {
        match (self.level(), other.level()) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }

    /// One origin of `self` and `other`: a legacy one takes the other's,
    /// two of a level accumulate their types.
    ///
    /// # Errors
    /// Two levels: two identities, never merged.
    pub fn merge(self, other: Self) -> Result<Self, Conflict> {
        match (self.level(), other.level()) {
            (None, _) => Ok(other),
            (_, None) => Ok(self),
            (Some(a), Some(b)) if a == b => Ok(Self(self.0 | other.0)),
            _ => Err(Conflict),
        }
    }

    /// The order series rows of equal labels keep: by level, legacy last.
    pub fn rank(self) -> u8 {
        self.level().map_or(u8::MAX, |l| l as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_round_trip_and_damage_is_refused() {
        let native = LabelMeta::native(LabelLevel::Scope, Datatype::Map);
        assert_eq!(native.bits(), 0x2020);
        assert_eq!(LabelMeta::decode(native.bits()).unwrap(), native);
        assert_eq!(LabelMeta::decode(0x0080).unwrap(), LabelMeta::LEGACY);
        for damaged in [
            0x0000, 0x1000, 0x0001, 0x4001, 0x1081, 0x0081, 0x1101, 0x00ff,
        ] {
            assert!(LabelMeta::decode(damaged).is_err(), "{damaged:#06x}");
        }
        assert_eq!(native.level(), Some(LabelLevel::Scope));
        assert_eq!(LabelMeta::LEGACY.level(), None);
    }

    #[test]
    fn a_legacy_origin_takes_a_known_one_and_two_levels_conflict() {
        let int = LabelMeta::native(LabelLevel::Point, Datatype::Int);
        let str = LabelMeta::native(LabelLevel::Point, Datatype::Str);
        let both = int.merge(str).unwrap();
        assert_eq!(
            both.datatypes().collect::<Vec<_>>(),
            [Datatype::Str, Datatype::Int]
        );
        assert_eq!(LabelMeta::LEGACY.merge(int), Ok(int));
        assert_eq!(int.merge(LabelMeta::LEGACY), Ok(int));
        assert_eq!(
            LabelMeta::LEGACY.merge(LabelMeta::LEGACY),
            Ok(LabelMeta::LEGACY)
        );
        let resource = LabelMeta::native(LabelLevel::Resource, Datatype::Str);
        assert_eq!(int.merge(resource), Err(Conflict));
        assert!(!int.agrees(resource));
        assert!(LabelMeta::LEGACY.agrees(resource));
        assert!(resource.rank() < int.rank() && int.rank() < LabelMeta::LEGACY.rank());
    }
}
