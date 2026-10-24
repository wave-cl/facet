//! `Value` → `Deserialize`.

use serde::de::value::{MapDeserializer, SeqDeserializer};
use serde::de::{self, IntoDeserializer, Visitor};
use serde::forward_to_deserialize_any;

use super::Value;
use crate::error::Error;

impl<'de> IntoDeserializer<'de, Error> for Value {
    type Deserializer = Value;
    fn into_deserializer(self) -> Value {
        self
    }
}

impl<'de> de::Deserializer<'de> for Value {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self {
            Value::Null => visitor.visit_unit(),
            Value::Bool(b) => visitor.visit_bool(b),
            Value::Int(i) => visitor.visit_i64(i),
            Value::Uint(u) => visitor.visit_u64(u),
            Value::Float(f) => visitor.visit_f64(f),
            Value::Decimal(d) => visitor.visit_string(d.to_string()),
            Value::Str(s) => visitor.visit_string(s),
            Value::Bytes(b) => visitor.visit_byte_buf(b.to_vec()),
            Value::Uuid(u) => visitor.visit_string(u.hyphenated().to_string()),
            Value::Timestamp(t) => visitor.visit_i64(t.0),
            Value::List(l) => visitor.visit_seq(SeqDeserializer::new(l.into_iter())),
            Value::Map(m) => visitor.visit_map(MapDeserializer::new(m.into_iter())),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self {
            Value::Null => visitor.visit_none(),
            other => visitor.visit_some(other),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        match self {
            Value::Str(s) => {
                let de: serde::de::value::StringDeserializer<Error> = s.into_deserializer();
                visitor.visit_enum(de)
            }
            Value::Map(m) if m.len() == 1 => {
                let (variant, value) = m.into_iter().next().unwrap();
                visitor.visit_enum(EnumDe { variant, value })
            }
            other => Err(de::Error::invalid_type(
                de::Unexpected::Other(other.type_name()),
                &"a variant name or a single-entry map",
            )),
        }
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self {
            Value::Bytes(b) => visitor.visit_seq(SeqDeserializer::new(
                b.iter().map(|&x| Value::Uint(u64::from(x))),
            )),
            other => other.deserialize_any(visitor),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct tuple tuple_struct map struct
        identifier ignored_any
    }
}

struct EnumDe {
    variant: String,
    value: Value,
}

impl<'de> de::EnumAccess<'de> for EnumDe {
    type Error = Error;
    type Variant = Value;
    fn variant_seed<V: de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Value), Error> {
        let de: serde::de::value::StringDeserializer<Error> = self.variant.into_deserializer();
        let v = seed.deserialize(de)?;
        Ok((v, self.value))
    }
}

impl<'de> de::VariantAccess<'de> for Value {
    type Error = Error;
    fn unit_variant(self) -> Result<(), Error> {
        match self {
            Value::Null => Ok(()),
            other => Err(de::Error::invalid_type(
                de::Unexpected::Other(other.type_name()),
                &"a unit variant",
            )),
        }
    }
    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(self)
    }
    fn tuple_variant<V: Visitor<'de>>(self, _: usize, visitor: V) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_any(self, visitor)
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_any(self, visitor)
    }
}
